"""
ETL for planet-nicfi data from Planet -> Azure Blob Storage.

The high-level pipeline is:


AOI File -> List[Quad Info] -> Copy assets -> Create item
"""
from __future__ import annotations
import traceback
from typing import Union, Any, Optional

import argparse
import datetime
import dataclasses
import dask
import dateutil
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
import tlz

from stactools_nicfi_etl import quads
from stactools_nicfi_etl.core import ETLRecord, bundle, compute_requester_id
import tqdm

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


_MOSAIC_INFO_NAME_CACHE = {}
_MOSAIC_INFO_ID_CACHE = {}


def lookup_mosaic_info_by_name(mosaic_name: str, planet_api_key: str) -> dict[str, Any]:
    result = _MOSAIC_INFO_NAME_CACHE.get(mosaic_name, {})
    if result:
        return result

    session = build_session(planet_api_key)
    r = session.get(API, params={"name__is": mosaic_name}, timeout=30)
    r.raise_for_status()
    assert len(r.json()["mosaics"]) == 1, len(r.json()["mosaics"])
    result = r.json()["mosaics"][0]
    _MOSAIC_INFO_NAME_CACHE.setdefault(mosaic_name, result)
    return result


def lookup_mosaic_info_by_id(mosaic_id: str, planet_api_key: str) -> dict[str, Any]:
    result = _MOSAIC_INFO_ID_CACHE.get(mosaic_id, {})
    if result:
        return result

    session = build_session(planet_api_key)
    r = session.get(f"{API}/{mosaic_id}", timeout=30)
    r.raise_for_status()
    result = r.json()
    _MOSAIC_INFO_ID_CACHE.setdefault(mosaic_id, result)
    return result


# XXX: I don't understand the behavior of dataclasses here.
# asdict is returning just the subclasses stuff. Or nothing at all?
# only when run under __main__.
@dataclasses.dataclass
class PlanetNICFIRecord(ETLRecord):
    partition_key: str
    row_key: str
    state: Optional[str]
    context: dict
    run_id: str
    planet_api_key: Optional[str] = None

    @property
    def entity(self) -> dict[str, Any]:
        # d = dataclasses.asdict(self)
        d = {
            "PartitionKey": self.partition_key,
            "RowKey": self.row_key,
            "state": self.state,
            "context": json.dumps(self.context),
            "run_id": self.run_id,
        }
        return d

    def __post_init__(self):
        self.planet_api_key = self.planet_api_key or os.environ.get(
            "ETL_PLANET_API_KEY"
        )

    @classmethod
    def from_entity(cls, entity):
        d = dict(entity)
        d.setdefault("context", "{}")
        d["partition_key"] = d.pop("PartitionKey")
        d["row_key"] = d.pop("RowKey")
        d["context"] = json.loads(d["context"])
        return cls(**d)

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
            r_image = requests.get(quad_info["_links"]["download"], timeout=60)
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
            r_thumbnail = requests.get(quad_info["_links"]["thumbnail"], timeout=60)
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


def do_one(
    record: PlanetNICFIRecord,
    requester_id: str,
    asset_credential: str | None,
    planet_api_key: str | None = None,
    quads_table_credential: azure.core.credentials.AzureSasCredential
    | str
    | None = None,
    etl_table_credential: azure.core.credentials.AzureSasCredential | str | None = None,
) -> PlanetNICFIRecord:
    mosaic_info = record.get_mosaic()
    quad_table_client = get_table_client("quads", quads_table_credential)
    etl_table_client = get_table_client("etl", etl_table_credential)
    try:
        entity = quad_table_client.get_entity(requester_id, record.quad_id)
        quad_info = quads.Quad.from_entity(entity).to_api(mosaic_info, planet_api_key)
        context = copy_item(mosaic_info, quad_info, asset_credential=asset_credential)
        result = dataclasses.replace(record, state="finished", context=context)
    except Exception as e:
        context = {"traceback": traceback.format_tb(e)}
        result = dataclasses.replace(record, state="error", context=context)

    etl_table_client.upsert_entity(result.entity)
    return result


def get_table_client(
    table_name, credential: str | azure.core.credentials.AzureSasCredential
):
    if isinstance(credential, str):
        credential = azure.core.credentials.AzureSasCredential(credential)
    return azure.data.tables.TableClient(
        "https://planet.table.core.windows.net",
        table_name,
        credential=credential,
    )


def process_mosaic_item_info_pairs(
    records: list[PlanetNICFIRecord],
    requester_id: str,
    asset_credential: str | None = None,
    planet_api_key: str | None = None,
    etl_table_credential: azure.core.credentials.AzureSasCredential | str | None = None,
    quads_table_credential: azure.core.credentials.AzureSasCredential
    | str
    | None = None,
) -> dict[str, list[PlanetNICFIRecord]]:
    gateway = dask_gateway.Gateway()

    cluster = gateway.new_cluster()
    import pathlib

    if "__file__" in globals():
        p = pathlib.Path(__file__).parent
    else:
        p = pathlib.Path("stactools_nicfi_etl")

    plugin = bundle(p)

    errors = []
    success = []

    with cluster:
        cluster.adapt(minimum=1, maximum=80)
        client: distributed.Client = cluster.get_client()
        with client:
            print("Dashboard:", client.dashboard_link)

            # client.wait_for_workers(1)
            client.register_worker_plugin(plugin)
            batches = tlz.partition_all(100, records)
            futures_to_records: dict[distributed.Future, PlanetNICFIRecord] = {}

            for batch in batches:
                for record in batch:
                    future = client.submit(
                        do_one,
                        record,
                        requester_id,
                        asset_credential=asset_credential,
                        planet_api_key=planet_api_key,
                        quads_table_credential=quads_table_credential,
                        etl_table_credential=etl_table_credential,
                        retries=4,
                    )
                    futures_to_records[future] = record

            distributed.fire_and_forget(list(futures_to_records))
            for future in distributed.as_completed(futures_to_records):
                assert future.status in {"cancelled", "error", "finished"}
                record = futures_to_records[future]
                if future.status == "finished":
                    success.append(record)
                else:
                    logger.warning("Error %s", record)
                    errors.append(record)
    gateway.stop_cluster(cluster.name)

    return success, errors


def load_quads(
    geometry: Union[shapely.geometry.Polygon, shapely.geometry.MultiPolygon],
    table_credential: str | azure.core.credentials.AzureSasCredential | None = None,
) -> list[quads.Quad]:
    if isinstance(table_credential, str):
        table_credential = azure.core.credentials.AzureSasCredential(table_credential)
    quad_table = azure.data.tables.TableClient(
        "https://planet.table.core.windows.net", "quads", credential=table_credential
    )
    df_quads = list(quad_table.list_entities())
    df_quads = pd.DataFrame(df_quads)
    df_quads["geometry"] = [  #  type: ignore
        shapely.geometry.box(*json.loads(x)) for x in df_quads["bbox"].tolist()  # type: ignore
    ]
    df = geopandas.GeoDataFrame(df_quads, crs="WGS84")

    # These don't match 100%. The Planet API returns some items that
    # don't actually intersect with the bounding box. So we add a tiny buffer
    # TODO: filter warning
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", "Geometry is in a geographic")
        subquads = df[df.buffer(0.000001).intersects(geometry)]

    assert (geometry - subquads.unary_union).area < 0.0001
    # TODO: Filter to state ne 'copied', but also have to intersect...
    # essentially geometry.covered_by, but might be slight gaps "on" the boundary.
    item_quads = [
        quads.Quad.from_entity(x._asdict())
        for x in subquads.drop(columns=["geometry", "context"]).itertuples(index=False)
    ]

    return item_quads


def process_initialized(
    run_id: str,
    requester_id: str,
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
    table_client = get_table_client("etl", etl_table_credential)
    records = (
        PlanetNICFIRecord.from_entity(x)
        for x in table_client.query_entities(f"state eq 'initialized' and run_id eq '{run_id}'")
        # TODO: filter on run ID too
    )
    success, errors = process_mosaic_item_info_pairs(
        records,
        requester_id,
        asset_credential=asset_credential,
        planet_api_key=planet_api_key,
        etl_table_credential=etl_table_credential,
        quads_table_credential=quads_table_credential,
    )
    if errors:
        raise RuntimeError(errors)

    return success


def initialize(
    run_id: str,
    geometry: Union[shapely.geometry.Polygon, shapely.geometry.MultiPolygon],
    start_datetime: datetime.datetime,
    end_datetime: datetime.datetime,
    planet_api_key: str,
    quads_table_credential: str
    | azure.core.credentials.AzureSasCredential
    | None = None,
    etl_table_credential: str | azure.core.credentials.AzureSasCredential | None = None,
):
    """Initialize the per-item records"""
    # TODO: this can use requester_id now.
    table_client = get_table_client("etl", etl_table_credential)
    item_quads = load_quads(geometry, quads_table_credential)
    mosaic_names = mosaics_for_date_range(start_datetime, end_datetime)
    records = []

    for name in mosaic_names:
        mosaic_info = lookup_mosaic_info_by_name(name, planet_api_key)
        for q in item_quads:
            quad_info = q.to_api(mosaic_info, planet_api_key)
            records.append(
                PlanetNICFIRecord(
                    partition_key="planet-nicfi",
                    row_key="_".join([mosaic_info["id"], quad_info["id"]]),
                    state="initialized",
                    run_id=run_id,
                    context={},
                )
            )

    batches = tlz.partition_all(100, records)
    total = len(records) // 100
    logger.info("Initializing %d records with run_id=%s", len(records), run_id)
    for batch in tqdm.tqdm(batches, total=total):
        operations = [("upsert", record.entity) for record in batch]
        table_client.submit_transaction(operations)


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
    parser.add_argument("--run-id", default=None)

    return parser.parse_args(args)


def get_run_id(
    geometry: Union[shapely.geometry.Polygon, shapely.geometry.MultiPolygon],
    start_datetime: datetime.datetime,
    end_datetime: datetime.datetime,
):
    geometry_token = dask.base.tokenize(shapely.geometry.mapping(geometry))
    run_id = f"{geometry_token}_{start_datetime.isoformat()}_{end_datetime.isoformat()}"
    return run_id


def main(args=None):
    args = parse_args(args)

    if sum(bool(x) for x in [args.bbox, args.geometry, args.file]) != 1:
        raise ValueError("Specify one of bbox, geometry, file")

    if args.bbox:
        geometry = shapely.geometry.box(*args.bbox)
    elif args.geometry:
        geometry = shapely.geometry.shape(args.geometry)
    else:
        file = args.file
        if file.startswith("https://planet.blob.core.windows.net/nicfi-etl-data"):
            file = f"{file}?{os.environ['ETL_PLANET_ETL_DATA_CREDENTIAL'].lstrip('?')}"

        df = geopandas.read_file(file)
        geometry = df.geometry.unary_union

    requester_id = compute_requester_id(geometry)

    run_id = args.run_id
    if run_id is None:
        run_id = get_run_id(geometry, args.start_datetime, args.end_datetime)

    initialize(
        run_id,
        geometry,
        start_datetime=args.start_datetime,
        end_datetime=args.end_datetime,
        planet_api_key=args.planet_api_key,
        quads_table_credential=args.quads_table_credential,
        etl_table_credential=args.etl_table_credential,
    )

    results = process_initialized(
        run_id=run_id,
        requester_id=requester_id,
        planet_api_key=args.planet_api_key,
        asset_credential=args.asset_credential,
        quads_table_credential=args.quads_table_credential,
        etl_table_credential=args.etl_table_credential,
    )
    logger.info("Processed %d items", len(results))


if __name__ == "__main__":
    main()
