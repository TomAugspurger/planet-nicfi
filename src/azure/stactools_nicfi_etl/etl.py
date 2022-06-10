"""
ETL for planet-nicfi data from Planet -> Azure Blob Storage.

The high-level pipeline is:


AOI File -> List[Quad Info] -> Copy assets -> Create item
"""
from __future__ import annotations
import collections
from typing import Union, Any

import argparse
import datetime
import dataclasses
import dateutil
import functools
import json
import logging
import os
import pathlib
import warnings

import azure.core.credentials
import azure.storage.blob
import azure.data.tables
import distributed
import geopandas
import pandas as pd
import pystac
import requests
import shapely
import dask_gateway

from stactools_nicfi_etl import quads
from stactools_nicfi_etl.core import ETLRecord, etl_as_completed, bundle

API = "https://api.planet.com/basemaps/v1/mosaics"
logger = logging.getLogger(__name__)


def build_session(planet_api_key: str) -> requests.Session:
    auth = requests.auth.HTTPBasicAuth(planet_api_key, "")
    session = requests.Session()
    retries = requests.adapters.Retry(
        total=5, backoff_factor=1, status_forcelist=[502, 503, 504]
    )
    session.mount("http://", requests.adapters.HTTPAdapter(max_retries=retries))
    session.auth = auth
    return session


@functools.lru_cache
def lookup_mosaic_info_by_name(mosaic_name: str, planet_api_key: str) -> dict[str, Any]:
    session = build_session(planet_api_key)
    r = session.get(API, params={"name__is": mosaic_name})
    r.raise_for_status()
    assert len(r.json()["mosaics"]) == 1, len(r.json()["mosaics"])
    return r.json()["mosaics"][0]


@functools.lru_cache
def lookup_mosaic_info_by_id(mosaic_id: str, planet_api_key: str) -> dict[str, Any]:
    session = build_session(planet_api_key)
    r = session.get(f"{API}/{mosaic_id}")
    r.raise_for_status()
    return r.json()


@dataclasses.dataclass
class PlanetNICFIRecord(ETLRecord):
    planet_api_key: str | None = None

    def __post_init__(self):
        self.planet_api_key = self.planet_api_key or os.environ.get(
            "ETL_PLANET_API_KEY"
        )

    @property
    def mosaic_id(self) -> str:
        return self.row_key.split("_")[0]

    @property
    def quad_id(self) -> str:
        return self.row_key.split("_")[1]

    def get_mosaic(self) -> dict:
        return lookup_mosaic_info_by_id(self.mosaic_id, self.planet_api_key)


def mosaics_for_datetime(timestamp: datetime.datetime) -> list[str]:
    """
    Generate a list of mosaics overlapping a datetime.

    Planet mosaics each cover some timespan, either a 6 months or a month. Given
    a datetime, we find the mosaic with the right bounds. We generate mosaics
    for both visual and analytic products.

    See Also
    --------
    mosaics_for_date_range
    """
    b1 = datetime.datetime(2020, 9, 1)
    b2 = datetime.datetime(2020, 6, 1)
    out = []

    if timestamp < b1:
        # biannual mosaic
        if timestamp.month < 6:
            t1 = timestamp.replace(year=timestamp.year - 1, month=12, day=1)
        else:
            t1 = timestamp.replace(month=6)

        if timestamp < b2:
            t2 = (t1 + pd.DateOffset(months=5)).to_pydatetime()
        else:
            t2 = datetime.datetime(2020, 8, 1)
        out.append(f"planet_medres_normalized_analytic_{t1:%Y-%m}_{t2:%Y-%m}_mosaic")
        out.append(f"planet_medres_visual_{t1:%Y-%m}_{t2:%Y-%m}_mosaic")
    else:
        out.append(f"planet_medres_normalized_analytic_{timestamp:%Y-%m}_mosaic")
        out.append(f"planet_medres_visual_{timestamp:%Y-%m}_mosaic")
    return sorted(set(out))


def mosaics_for_date_range(
    start_datetime: datetime.datetime, end_datetime: datetime.datetime
) -> list[str]:
    """
    Generate a list of mosaics overlapping a datetime.

    See Also
    --------
    mosaics_for_datetime
    """
    out = []
    timestamps = pd.date_range(start_datetime, end_datetime, freq="MS")
    for timestamp in timestamps:
        out.extend(mosaics_for_datetime(timestamp))
    return sorted(set(out))


def name_blob(mosaic_info, item_info):
    """
    Generate the name for a data file in Azure Blob Storage.

    This follows the pattern {kind}/{mosaic-id}/{item-id}/data.tif where
    kind is either analytic or visual.
    """
    prefix = "analytic" if "analytic" in mosaic_info["name"] else "visual"
    return f"{prefix}/{mosaic_info['id']}/{item_info['id']}/data.tif"


def name_thumbnail(mosaic_info, item_info):
    """
    Generate the name for a thumbnail file in Azure Blob Storage.

    This follows the pattern {kind}/{mosaic-id}/{item-id}/thumbnail.png where
    kind is either analytic or visual.
    """
    return str(
        pathlib.Path(name_blob(mosaic_info, item_info)).with_name("thumbnail.png")
    )


def name_mosaic_info(mosaic_info):
    """
    Generate the name for a mosaic metadata file in Azure Blob Storage.

    This follows the pattern `metadata/mosaic/{mosaic-id}.json`.
    """
    return f"metadata/mosaic/{mosaic_info['id']}.json"


def name_item_info(mosaic_info, item_info):
    """
    Generate the name for a mosaic metadata file in Azure Blob Storage.

    This follows the pattern `metadata/quad/{mosaic-id}/{item-id}.json`.
    """
    return f"metadata/quad/{mosaic_info['id']}/{item_info['id']}.json"


def copy_item(
    mosaic: dict,
    quad_info: dict,
    redownload=False,
    overwrite=True,
    asset_credential: str | None = None,
) -> dict[str, str]:
    # pass a SAS token for credential
    container_client = azure.storage.blob.ContainerClient(
        "https://planet.blob.core.windows.net", "nicfi", credential=asset_credential
    )
    blob_name = name_blob(mosaic, quad_info)
    thumbnail_name = name_thumbnail(mosaic, quad_info)
    mosaic_name = name_mosaic_info(mosaic)
    quad_name = name_item_info(mosaic, quad_info)
    logger.debug("Copying item_info %s -> %s", quad_info["id"], blob_name)
    with container_client.get_blob_client(blob_name) as bc:
        if redownload or not bc.exists():
            r_image = requests.get(quad_info["_links"]["download"])
            r_image.raise_for_status()
            image = r_image.content
            bc.upload_blob(
                image,
                content_settings=azure.storage.blob.ContentSettings(
                    content_type=str(pystac.MediaType.COG)
                ),
                max_concurrency=8,
                overwrite=overwrite,
            )

    with container_client.get_blob_client(thumbnail_name) as bc:
        if redownload or not bc.exists():
            r_thumbnail = requests.get(quad_info["_links"]["thumbnail"])
            r_thumbnail.raise_for_status()
            thumbnail = r_thumbnail.content
            bc.upload_blob(
                thumbnail,
                content_settings=azure.storage.blob.ContentSettings(
                    content_type=str(pystac.MediaType.PNG)
                ),
                overwrite=overwrite,
            )

    with container_client.get_blob_client(mosaic_name) as bc:
        if redownload or not bc.exists():
            bc.upload_blob(
                json.dumps(mosaic).encode(),
                content_settings=azure.storage.blob.ContentSettings(
                    str(pystac.MediaType.JSON)
                ),
                overwrite=overwrite,
            )

    with container_client.get_blob_client(quad_name) as bc:
        if redownload or not bc.exists():
            bc.upload_blob(
                json.dumps(quad_info).encode(),
                content_settings=azure.storage.blob.ContentSettings(
                    str(pystac.MediaType.JSON)
                ),
                overwrite=overwrite,
            )
    return {
        "blob_name": blob_name,
        "thumbnail_name": thumbnail_name,
        "mosaic_name": mosaic_name,
        "quad_name": quad_name,
    }


def do_one(record: PlanetNICFIRecord, quad_info, asset_credential) -> PlanetNICFIRecord:
    mosaic_info = record.get_mosaic()
    context = copy_item(mosaic_info, quad_info, asset_credential=asset_credential)
    result = dataclasses.replace(record, state="finished", context=context)
    return result


def process_mosaic_item_info_pairs(
    records: list[tuple[PlanetNICFIRecord, dict]],
    asset_credential: str | None = None,
    etl_table_credential: azure.core.credentials.AzureSasCredential | str | None = None,
) -> dict[str, list[PlanetNICFIRecord]]:
    if etl_table_credential is None:
        etl_table_credential = os.environ.get("ETL_ETL_TABLE_CREDENTIAL")
    if isinstance(etl_table_credential, str):
        etl_table_credential = azure.core.credentials.AzureSasCredential(
            etl_table_credential
        )
    table_client = azure.data.tables.TableClient(
        "https://planet.table.core.windows.net",
        "etl",
        credential=etl_table_credential,
    )

    gateway = dask_gateway.Gateway()

    cluster = gateway.new_cluster()
    import pathlib

    if "__file__" in globals():
        p = pathlib.Path(__file__).parent
    else:
        p = pathlib.Path("stactools_nicfi_etl")

    plugin = bundle(p)
    results = collections.defaultdict(list)

    with cluster:
        cluster.adapt(minimum=1, maximum=80)
        client: distributed.Client = cluster.get_client()
        with client:
            print("Dashboard:", client.dashboard_link)

            client.register_worker_plugin(plugin)

            futures_to_records: dict[distributed.Future, PlanetNICFIRecord] = {
                client.submit(
                    do_one,
                    record,
                    quad_info=quad_info,
                    asset_credential=asset_credential,
                    retries=4,
                ): record
                for record, quad_info in records
            }

            distributed.fire_and_forget(list(futures_to_records))
            for record in etl_as_completed(
                futures_to_records, table_client=table_client, total=len(records)
            ):
                results[record.state].append(record)

    gateway.stop_cluster(cluster.name)

    return dict(results)


def load_quads(
    table_credential: str | azure.core.credentials.AzureSasCredential | None = None,
) -> geopandas.GeoDataFrame:
    if isinstance(table_credential, str):
        table_credential = azure.core.credentials.AzureSasCredential(table_credential)
    quad_table = azure.data.tables.TableClient(
        "https://planet.table.core.windows.net", "quads", credential=table_credential
    )
    quads = list(quad_table.list_entities())
    quads = pd.DataFrame(quads)
    quads["geometry"] = [  #  type: ignore
        shapely.geometry.box(*json.loads(x)) for x in quads["bbox"].tolist()  # type: ignore
    ]
    quads = geopandas.GeoDataFrame(quads, crs="WGS84")
    return quads


def process_geometry(
    geometry: Union[shapely.geometry.Polygon, shapely.geometry.MultiPolygon],
    start_datetime: datetime.datetime,
    end_datetime: datetime.datetime,
    planet_api_key: str,
    asset_credential: str | None,
    quads_table_credential: str
    | azure.core.credentials.AzureSasCredential
    | None = None,
    etl_table_credential: str | azure.core.credentials.AzureSasCredential | None = None,
) -> dict[str, list[PlanetNICFIRecord]]:
    """
    Copy data from Planet to Azure Blob Storage for a specific space / time.

    Parameters
    ----------
    geometry
    start_datetime, end_datetime : datetime.datetime
        The start and end date ranges to pull data for.
    planet_api_key : str
        Your API key for Planet
    asset_credential : str
        A credential to upload to the planet/nicfi storage container. Probably
        a SAS token with write permissions.
    """
    logger.info("Loading quads")
    df = load_quads(quads_table_credential)
    # These don't match 100%. The Planet API returns some items that
    # don't actually intersect with the bounding box. So we add a tiny buffer
    # TODO: filter warning
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", "Geometry is in a geographic")
        subquads = df[df.buffer(0.000001).intersects(geometry)]

    # TODO: Filter to state ne 'copied'
    item_quads = [
        quads.Quad.from_entity(x._asdict())
        for x in subquads.drop(columns=["geometry", "context"]).itertuples(index=False)
    ]

    # essentially geometry.covered_by, but might be slight gaps "on" the boundary.
    assert (geometry - subquads.unary_union).area < 0.0001
    # TODO: just fall back to querying.

    mosaic_names = mosaics_for_date_range(start_datetime, end_datetime)
    records: list[tuple[PlanetNICFIRecord, dict]] = []

    for name in mosaic_names:
        mosaic_info = lookup_mosaic_info_by_name(name, planet_api_key)
        for q in item_quads:
            quad_info = q.to_api(mosaic_info, planet_api_key)
            records.append(
                (
                    PlanetNICFIRecord(
                        partition_key="planet-nicfi",
                        row_key="_".join([mosaic_info["id"], quad_info["id"]]),
                        state=None,
                        context={},
                    ),
                    quad_info,
                )
            )

    results = process_mosaic_item_info_pairs(
        records, asset_credential, etl_table_credential
    )
    errors = []
    errors.extend(results.get("cancelled", []))
    errors.extend(results.get("error", []))
    if errors:
        raise RuntimeError(errors)

    return results


def parse_args(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bbox",
        help="List of floats as left, bottom, right, top.",
        type=json.loads,
    )
    parser.add_argument(
        "--geometry",
        help="List of floats as left, bottom, right, top.",
    )
    parser.add_argument(
        "--file",
        help="List of floats as left, bottom, right, top.",
    )

    parser.add_argument("--start-datetime", type=dateutil.parser.parse)
    parser.add_argument("--end-datetime", type=dateutil.parser.parse)
    parser.add_argument(
        "--planet-api-key", default=os.environ.get("ETL_PLANET_API_KEY")
    )
    parser.add_argument(
        "--asset-credential",
        default=os.environ.get("ETL_ASSET_CREDENTIAL"),
    )
    parser.add_argument(
        "--quads-table-credential",
        default=os.environ.get("ETL_QUADS_TABLE_CREDENTIAL"),
    )
    parser.add_argument(
        "--etl-table-credential",
        default=os.environ.get("ETL_ETL_TABLE_CREDENTIAL"),
    )

    return parser.parse_args(args)


def main(args=None):
    args = parse_args(args)

    if sum(bool(x) for x in [args.bbox, args.geometry, args.file]) != 1:
        raise ValueError("Specify one of bbox, geometry, file")

    if args.bbox:
        shape = shapely.geometry.box(*args.bbox)
    elif args.geometry:
        shape = shapely.geometry.shape(args.geometry)
    else:
        df = geopandas.read_file(args.file)
        shape = df.geometry.unary_union

    process_geometry(
        shape,
        args.start_datetime,
        args.end_datetime,
        planet_api_key=args.planet_api_key,
        asset_credential=args.asset_credential,
        quads_table_credential=args.quads_table_credential,
        etl_table_credential=args.etl_table_credential,
    )


if __name__ == "__main__":
    main()
