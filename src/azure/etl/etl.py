"""
ETL for planet-nicfi data from Planet -> Azure Blob Storage.
"""
from __future__ import annotations
from typing import Union, IO, AnyStr, TypedDict

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
import tqdm.auto


API = "https://api.planet.com/basemaps/v1/mosaics"
logger = logging.getLogger(__name__)


class ETLProcessResult(TypedDict):
    """
    Represents the result of calling :meth:`ETLRecord.process`.

    Parameters
    ----------
    name: str
        The eventual name in the Azure Blob Storage container.
    data: str, os.PathLike, typing.BinaryIO
        The data provided to :meth:`azure.storage.blob.ContainerClient.upload_blob`.
        This can be a path, file-like object, or the raw bytes to write.
    content_settings: azure.storage.blob.ContentSettings
        The content settings to set for the blob.
    """
    name: str
    data: str | IO[AnyStr]
    content_settings: azure.storage.blob.ContentSettings | None


@dataclasses.dataclass
class ETLRecord:
    """
    Represents an item to be processed by pc-tasks.

    An ETLRecord will correspond to one or more files from the source provider,
    one or more blobs in Azure Blob Storage, and (ideally) *exactly one STAC item*.

    Parameters
    ----------
    dataset_id: str
        The identifier for the dataset. This might also be the collection ID.
        This is used as the partition Key in Azure Storage Table. This and
        `ETLRecord.item_id` uniquely identify a record
    item_id: str
    are_assets_copied: bool, default False
    context: bool, default False
    """

    dataset_id: str
    item_id: str
    # TODO: harmonize are_assets_copied / state
    # are_assets_copied: bool = False
    state: str  # TODO: enum
    run_id: str | None = None
    context: dict = dataclasses.field(default_factory=dict)

    @property
    def collection_id(self):
        """Assume 1:1 relationship between source and item"""
        return self.dataset_id

    @property
    def entity(self) -> dict:
        """
        Returns a dictionary safe to write to an Azure Storage Table.
        """
        # TODO: extra fields
        return {
            "PartitionKey": self.dataset_id,
            "RowKey": self.item_id,
            "are_assets_copied": self.are_assets_copied,
            "context": json.dumps(self.context),
        }

    @classmethod
    def from_entity(cls, entity: dict) -> "ETLRecord":
        """
        Create an ETLRecord from an Azure Blob Storage query.
        """
        entity = dict(entity)
        row_key = entity.pop("RowKey")
        dataset_id = entity.pop("PartitionKey")
        entity["context"] = json.loads(entity["context"])
        return cls(**{"item_id": row_key, "dataset_id": dataset_id}, **entity)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def process(self) -> list[ETLProcessResult]:
        raise NotImplementedError


@dataclasses.dataclass
class PlanetNICFIRecord(ETLRecord):
    planet_api_key : str | None = None

    def __post_init__(self):
        self._session_ = None
        self.planet_api_key = self.planet_api_key or os.environ.get("ETL_PLANET_API_KEY")

    @property
    def mosaic_id(self) -> str: 
        return self.item_id.split("_")[0]

    @property
    def quad_id(self) -> str:
        return self.item_id.split("_")[1]

    @property
    def _session(self) -> requests.Session:
        if self._session_ is None:
            auth = requests.auth.HTTPBasicAuth(self.planet_api_key, "")
            session = requests.Session()
            retries = requests.adapters.Retry(
                total=5, backoff_factor=1, status_forcelist=[502, 503, 504]
            )
            session.mount("http://", requests.adapters.HTTPAdapter(max_retries=retries))
            session.auth = auth
            self._session_ = session
        return self._session_

    def get_mosaic(self) -> dict:
        r = self._session.get(f"{API}/{self.mosaic_id}")
        r.raise_for_status()
        return r.json()

    def get_quad(self) -> dict:
        r = self._session.get(f"{API}/{self.mosaic_id}/quads/{self.quad_id}")
        r.raise_for_status()
        return r.json()

    def process(self) -> list[ETLProcessResult]:
        ...


def etl_as_completed(futures_to_keys: dict[distributed.Future], credential=None):
    """
    Like dask.distributed.as_completed, but logs the result.

    This could be made more generic and moved to an etl framework.
    """
    if credential is None:
        credential = os.environ.get("ETL_ETL_TABLE_CREDENTIAL")
    if isinstance(credential, str):
        credential = azure.core.credentials.AzureSasCredential(credential)
    table = azure.data.tables.TableClient(
        "https://planet.table.core.windows.net",
        "etl",
        credential=credential,
    )

    # TODO: This might be a bottleneck. Batch into size 100
    for future in distributed.as_completed(futures_to_keys):
        keys = futures_to_keys[future]
        entity = dict(keys)
        if future.status == "finished":
            result = future.result()
            entity["state"] = "copied"
            entity["context"] = json.dumps({"context": result})
        elif future.status == "error":
            entity["state"] = "error"
            entity["context"] = json.dumps({"error": str(future.exception())})
        else:
            entity["state"] = "unknown"

        try:
            table.upsert_entity(entity)
        except Exception:
            logger.exception("Failed to update state")
        yield future


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


@functools.lru_cache
def mosaic_info(mosaic_name, planet_api_key):
    """
    Get the metadata for a Planet mosaic.

    This fetches the STAC-like metadata for a given `mosaic_name` from the
    Planet STAC API.
    """
    auth = requests.auth.HTTPBasicAuth(planet_api_key, "")
    r = requests.get(API, auth=auth, params={"name__is": mosaic_name})
    r.raise_for_status()
    assert len(r.json()["mosaics"]) == 1, len(r.json()["mosaics"])
    return r.json()["mosaics"][0]


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
    item_info: dict,
    redownload=False,
    overwrite=True,
    asset_credential: str | None = None,
) -> dict[str, str]:
    # pass a SAS token for credential
    container_client = azure.storage.blob.ContainerClient(
        "https://planet.blob.core.windows.net", "nicfi", credential=asset_credential
    )
    blob_name = name_blob(mosaic, item_info)
    thumbnail_name = name_thumbnail(mosaic, item_info)
    mosaic_name = name_mosaic_info(mosaic)
    quad_name = name_item_info(mosaic, item_info)
    logger.debug("Copying item_info %s -> %s", item_info["id"], blob_name)
    with container_client.get_blob_client(blob_name) as bc:
        if redownload or not bc.exists():
            r_image = requests.get(item_info["_links"]["download"])
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
            r_thumbnail = requests.get(item_info["_links"]["thumbnail"])
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
                json.dumps(item_info).encode(),
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


def consume(session, request, key="items"):
    items = request.json()[key]
    while "_next" in request.json()["_links"]:
        request = session.get(request.json()["_links"]["_next"])
        request.raise_for_status()
        items.extend(request.json()[key])
    return items


def process_mosaic_item_info_pairs(
    mosaic_item_info_pairs: list[tuple],
    asset_credential: str | None = None,
    etl_table_credential: str | None = None,
) -> tuple[dict[str, str], list[str]]:
    gateway = dask_gateway.Gateway()

    cluster = gateway.new_cluster(shutdown_on_close=False)
    with cluster:
        cluster.adapt(minimum=1, maximum=80)
        client: distributed.Client = cluster.get_client()
        with client:
            client.register_worker_plugin(
                distributed.UploadFile(os.path.abspath(__file__))
            )

            print("Dashboard:", client.dashboard_link)

            futures_to_keys = {
                client.submit(
                    copy_item, mosaic, item_info, asset_credential=asset_credential, retries=4
                ): {
                    "PartitionKey": "planet-nicfi",
                    "RowKey": f"{mosaic['id']}_{item_info['id']}",
                }
                for mosaic, item_info in mosaic_item_info_pairs
            }

            items = []
            errors = []
            distributed.fire_and_forget(list(futures_to_keys))

            # this is kinda slow. Only 2.5 items/s. Most likely
            # time is spent updating the table.
            for item in tqdm.auto.tqdm(
                etl_as_completed(futures_to_keys, credential=etl_table_credential),
                total=len(futures_to_keys),
            ):
                try:
                    items.append(item.result())
                except Exception as e:
                    logger.exception("failure")
                    errors.append(str(e))

    gateway.stop_cluster(cluster.name)

    return items, errors


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
    quads["geometry"] = [
        shapely.geometry.box(*json.loads(x)) for x in quads["bbox"].tolist()
    ]
    quads = geopandas.GeoDataFrame(quads, crs="WGS84")
    return quads


def process_geometry(
    geometry: Union[shapely.geometry.Polygon, shapely.geometry.MultiPolygon],
    start_datetime: datetime.datetime,
    end_datetime: datetime.datetime,
    planet_api_key: str,
    asset_credential: str | None,
    quads_table_credential: str | azure.core.credentials.AzureSasCredential | None = None,
    etl_table_credential: str | azure.core.credentials.AzureSasCredential | None = None,
) -> dict[str, str]:
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
    import quads

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
        quads.Quad.deserialize(x._asdict())
        for x in subquads.drop(columns="geometry").itertuples(index=False)
    ]

    # essentially geometry.covered_by, but might be slight gaps "on" the boundary.
    assert (geometry - subquads.unary_union).area < 0.0001
    # TODO: just fall back to querying.

    auth = requests.auth.HTTPBasicAuth(planet_api_key, "")
    session = requests.Session()
    retries = requests.adapters.Retry(
        total=5, backoff_factor=1, status_forcelist=[502, 503, 504]
    )
    session.mount("http://", requests.adapters.HTTPAdapter(max_retries=retries))
    session.auth = auth

    mosaic_names = mosaics_for_date_range(start_datetime, end_datetime)
    mosaic_item_info_pairs = []

    for name in mosaic_names:
        mosaic = mosaic_info(name, planet_api_key)
        for q in item_quads:
            mosaic_item_info_pairs.append((mosaic, q.to_api(mosaic, planet_api_key)))

    mosaic_item_info_pairs = sorted(mosaic_item_info_pairs, key=lambda x: x[1]["id"])
    items, errors = process_mosaic_item_info_pairs(
        mosaic_item_info_pairs, asset_credential=asset_credential, etl_table_credential=etl_table_credential
    )

    if errors:
        raise RuntimeError(errors)

    return items


def redo(etl_table_credential, asset_credential):
    table = azure.data.tables.TableClient(
        "https://planet.table.core.windows.net",
        "etl",
        credential=etl_table_credential,
    )
    errors = table.query_entities("state ne 'copied'")
    records = (PlanetNICFIRecord.from_entity(e) for e in errors)
    mosaic_item_info_pairs = ((record.get_mosaic(), record.get_quad()) for record in records)
    items, errors = process_mosaic_item_info_pairs(
        mosaic_item_info_pairs, asset_credential=asset_credential, etl_table_credential=etl_table_credential
    )
    if errors:
        raise RuntimeError(errors)
    return items



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
    parser.add_argument("--planet-api-key", default=os.environ.get("ETL_PLANET_API_KEY"))
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
