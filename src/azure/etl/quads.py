"""
Caching the quads to speed up ETL.

To support arbitrary geojson AOIs, we need to avoid Planets `bboxes`
API queries. We'll instead cache the bounding box of each quad (plus
a little extra metadata).
"""
from __future__ import annotations

import argparse
from multiprocessing.sharedctypes import Value
import geopandas
import os
import azure.core.credentials
import azure.data.tables
import shapely.geometry
import dataclasses
import json
import requests
import functools
import tlz

API = "https://api.planet.com/basemaps/v1/mosaics"


def parse_thumbnail(x):
    return x["_links"]["thumbnail"].split("?")[0].split("/", 7)[-1]


@dataclasses.dataclass
class Quad:
    row_key: str
    bbox: list[float]
    # e.g. gmap/11/637/1054.png
    thumbnail_id: str
    partition_key: str = "quad"

    def serialize(self):
        out = dataclasses.asdict(self)
        out["bbox"] = json.dumps(out["bbox"])
        out["PartitionKey"] = out.pop("partition_key")
        out["RowKey"] = out.pop("row_key")
        return out

    @classmethod
    def deserialize(cls, d: dict):
        d = dict(d)
        d["bbox"] = json.loads(d["bbox"])
        d["row_key"] = d.pop("RowKey")
        d["partition_key"] = d.pop("PartitionKey")
        return cls(**d)

    @classmethod
    def from_api(cls, item):
        return cls(
            partition_key="quad",
            row_key=item["id"],
            bbox=item["bbox"],
            thumbnail_id=parse_thumbnail(item),
        )

    def to_api(self, mosaic, planet_api_key):
        base = "https://api.planet.com/basemaps/v1/mosaics"
        quad_base = f"{base}/{mosaic['id']}/quads/{self.row_key}"
        self_link = f"{quad_base}?api_key={planet_api_key}"
        download_link = f"{quad_base}/full?api_key={planet_api_key}"
        items_link = f"{quad_base}/items?api_key={planet_api_key}"
        thumbnails_link = f"https://tiles.planet.com/basemaps/v1/planet-tiles/{mosaic['name']}/{self.thumbnail_id}?api_key={planet_api_key}"
        return {
            "_links": {
                "_self": self_link,
                "download": download_link,
                "items": items_link,
                "thumbnail": thumbnails_link,
            },
            "bbox": self.bbox,
            "id": self.row_key,
        }


def consume(session, request, key="items"):
    items = request.json()[key]
    i = 1
    while "_next" in request.json()["_links"]:
        print(f"Consuming page", i, end="\r")
        request = session.get(request.json()["_links"]["_next"])
        request.raise_for_status()
        items.extend(request.json()[key])
        i += 1
    return items


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


def cache_quads(bbox, name, planet_api_key, quad_table_credential):
    session = requests.Session()
    retries = requests.adapters.Retry(
        total=5, backoff_factor=1, status_forcelist=[502, 503, 504]
    )
    session.mount("http://", requests.adapters.HTTPAdapter(max_retries=retries))
    session.mount("https://", requests.adapters.HTTPAdapter(max_retries=retries))
    session.auth = requests.auth.HTTPBasicAuth(planet_api_key, "")
    quad_table = azure.data.tables.TableClient(
        "https://planet.table.core.windows.net",
        "quads",
        credential=quad_table_credential,
    )

    mosaic = mosaic_info(name, planet_api_key)
    r_quads = session.get(
        f"{API}/{mosaic['id']}/quads",
        params={"bbox": ",".join(map(str, bbox)), "_page_size": 500},
        stream=True,
    )
    r_quads.raise_for_status()
    items = consume(session, r_quads)
    quads = [Quad.from_api(item) for item in items]

    print("Caching quads")
    batches = tlz.partition_all(100, quads)
    results = []
    for batch in batches:
        operations = [("upsert", record.serialize()) for record in batch]
        b = quad_table.submit_transaction(operations)
        results.extend(b)
    print(f"Cached {len(quads)} quads", len(quads))


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

    parser.add_argument("--planet-api-key", default=os.environ.get("ETL_PLANET_API_KEY"))
    parser.add_argument(
        "--azure-table-credential",
        default=os.environ.get("ETL_QUADS_TABLE_CREDENTIAL"),
    )

    return parser.parse_args(args)


def main(args=None):
    args = parse_args(args)

    if sum(bool(x) for x in [args.bbox, args.geometry, args.file]) != 1:
        raise ValueError("Specify one of bbox, geometry, file")

    if args.bbox:
        shapes = [shapely.geometry.box(*args.bbox)]

    elif args.geometry:
        shape = shapely.geometry.shape(args.geometry)
        if isinstance(shape, shapely.geometry.MultiPolygon):
            shapes = shape
        else:
            shapes = [shape]
    else:
        df = geopandas.read_file(args.file)
        shapes = df.geometry.tolist()

    name = "planet_medres_normalized_analytic_2022-01_mosaic"

    if args.azure_table_credential:
        quad_table_credential = azure.core.credentials.AzureSasCredential(
            args.azure_table_credential
        )
    else:
        quad_table_credential = None

    for geom in shapes:
        cache_quads(geom.bounds, name, args.planet_api_key, quad_table_credential)

    # df = geopandas.read_file("...")
    # for geom in df.geometry.tolist():
    #     cache_quads(geom.bounds, name, args.planet_api_key, quad_table_credential)


if __name__ == "__main__":
    main()
