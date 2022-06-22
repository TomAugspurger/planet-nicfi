"""
Core ETL functionality to refactor elsewhere.
"""
from __future__ import annotations
import json

import os
import dataclasses
import logging
import traceback
from typing import TypeVar, Type, Any, Optional
from collections.abc import Mapping
import io
import zipfile
import distributed
import requests

import tlz
import azure.data.tables
import dask.distributed
import shapely.geometry
import geopandas
from dask.distributed import WorkerPlugin
import tqdm


ETLRecordT = TypeVar("ETLRecordT", bound="ETLRecord")

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ETLRecord:
    partition_key: str
    row_key: str
    state: Optional[str]
    context: dict

    @property
    def entity(self) -> dict[str, Any]:
        d = dataclasses.asdict(self)
        d["PartitionKey"] = d.pop("partition_key")
        d["RowKey"] = d.pop("row_key")
        d["context"] = json.dumps(d["context"])
        return d

    @classmethod
    def from_entity(cls: Type[ETLRecordT], entity: Mapping[str, Any]) -> ETLRecordT:
        d = dict(entity)
        d.setdefault("context", "{}")
        d["partition_key"] = d.pop("PartitionKey")
        d["row_key"] = d.pop("RowKey")
        d["context"] = json.loads(d["context"])
        return cls(**d)

    @property
    def id(self):
        return "_".join([self.partition_key, self.row_key])


def etl_as_completed(
    futures: dict[dask.distributed.Future, ETLRecordT],
    table_client: azure.data.tables.TableClient,
    total: int | None = None,
):
    """
    Iterate over futures returning ETLRecords, saving the results to the provided table.
    """
    completed = dask.distributed.as_completed(futures=futures)
    batches: list[tuple[dask.distributed.Future]] = tlz.partition_all(100, completed)

    if total:
        total = int(total // 100)

    for batch in tqdm.tqdm(batches, total=total):
        operations = []
        output = []
        for future in batch:
            record = futures[future]
            assert future.status in {"cancelled", "error", "finished"}
            if future.status == "cancelled":
                result = dataclasses.replace(record, state="cancelled")
            elif future.status == "error":
                result = dataclasses.replace(
                    record,
                    state="error",
                    context={"traceback": traceback.format_tb(future.traceback())},
                )
            else:
                result = future.result()
            operations.append(("upsert", result.entity))
            output.append(result)

        logger.debug("Updating %d rows", len(operations))
        table_client.submit_transaction(operations)

        yield from output


def consume(session: requests.Session, response: requests.Response, key="items"):
    """
    Consume all pages of an API request.
    """
    items = response.json()[key]
    while "_next" in response.json()["_links"]:
        response = session.get(response.json()["_links"]["_next"])
        response.raise_for_status()
        items.extend(response.json()[key])
    return items


def parse_aoi(
    bbox: list[float], geometry: str, file: os.PathLike
) -> list[shapely.geometry.Polygon]:
    if sum(bool(x) for x in [bbox, geometry, file]) != 1:
        raise ValueError("Specify one of bbox, geometry, file")

    if bbox:
        shapes = [shapely.geometry.box(*bbox)]

    elif geometry:
        shape = shapely.geometry.shape(geometry)
        if isinstance(shape, shapely.geometry.MultiPolygon):
            shapes = shape
        else:
            shapes = [shape]
    else:
        df = geopandas.read_file(file)
        shapes = df.geometry.tolist()
    return shapes


def bundle(package_directory):
    with zipfile.PyZipFile("package.zip", mode="w") as zip_module:
        zip_module.writepy(package_directory)
    plugin = distributed.UploadFile("package.zip")
    return plugin


class InitPlugin(WorkerPlugin):
    def __init__(
        self, package_directory: str, zip_module_path="/tmp/mymodule.zip"
    ) -> None:
        self.zip_module_path = zip_module_path

        sink = io.BytesIO()
        with zipfile.PyZipFile(sink, mode="w") as zip_module:
            zip_module.writepy(package_directory)

        sink.seek(0)
        self._data = sink.getvalue()
        super().__init__()

    async def setup(self, worker: distributed.Worker):
        import sys
        import logging

        logger = logging.getLogger("distributed.worker")
        logger.warning("Running setup")
        response = await worker.upload_file(
            comm=None, filename=self.zip_module_path, data=self._data, load=False
        )
        logger.warning("Uploaded file")
        sys.path.append(self.zip_module_path)
        # return super().setup(worker)
        assert len(self._data) == response["nbytes"]
        return response
