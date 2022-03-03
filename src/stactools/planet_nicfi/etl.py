import json
import os
import azure.storage.blob
import datetime
import functools
import pathlib

import pandas as pd
import pystac
import requests


API = "https://api.planet.com/basemaps/v1/mosaics"


def mosaics_for_datetime(timestamp):
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


def mosaics_for_date_range(start_datetime, end_datetime):
    out = []
    timestamps = pd.date_range(start_datetime, end_datetime, freq="MS")
    for timestamp in timestamps:
        out.extend(mosaics_for_datetime(timestamp))
    return sorted(set(out))


@functools.lru_cache
def mosaic_info(mosaic_name, planet_api_key):
    auth = requests.auth.HTTPBasicAuth(planet_api_key, "")
    r = requests.get(API, auth=auth, params={"name__is": mosaic_name})
    r.raise_for_status()
    assert len(r.json()["mosaics"]) == 1
    return r.json()["mosaics"][0]


def name_blob(mosaic_info, item_info):
    prefix = "analytic" if "analytic" in mosaic_info["name"] else "visual"
    return f"{prefix}/{mosaic_info['id']}/{item_info['id']}/data.tif"


def name_thumbnail(mosaic_info, item_info):
    return str(
        pathlib.Path(name_blob(mosaic_info, item_info)).with_name("thumbnail.png")
    )


def name_mosaic_info(mosaic_info):
    return f"metadata/mosaic/{mosaic_info['id']}.json"


def name_item_info(mosaic_info, item_info):
    return f"metadata/quad/{mosaic_info['id']}/{item_info['id']}.json"


def copy_item(mosaic, item_info, redownload=False, overwrite=True, credential=None):
    # pass a SAS token for credential
    container_client = azure.storage.blob.ContainerClient(
        "https://planet.blob.core.windows.net", "nicfi", credential=credential
    )
    blob_name = name_blob(mosaic, item_info)
    thumbnail_name = name_thumbnail(mosaic, item_info)
    mosaic_name = name_mosaic_info(mosaic)
    quad_name = name_item_info(mosaic, item_info)

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

    return blob_name, thumbnail_name, mosaic_name, quad_name


def consume(session, request, key="items"):
    items = request.json()[key]
    while "_next" in request.json()["_links"]:
        request = session.get(request.json()["_links"]["_next"])
        request.raise_for_status()
        items.extend(request.json()[key])
    return items


# def process_query(bbox, start_datetime, end_datetime, planet_api_key):
#     auth = requests.auth.HTTPBasicAuth(planet_api_key, "")
#     session = requests.Session()
#     retries = requests.adapters.Retry(
#         total=5, backoff_factor=1, status_forcelist=[502, 503, 504]
#     )
#     session.mount("http://", requests.adapters.HTTPAdapter(max_retries=retries))
#     session.auth = auth
#     mosaic_item_info_pairs = []

#     mosaic_names = mosaics_for_date_range(start_datetime, end_datetime)
#     bbox = [
#         -60.64453124152092,
#         -1.2303741772055612,
#         -60.468749991545344,
#         -1.054627942073222,
#     ]
#     for name in mosaic_names:
#         mosaic = mosaic_info(name)
#         r_quads = session.get(
#             f"{API}/{mosaic['id']}/quads",
#             params={"bbox": ",".join(map(str, bbox))},
#             stream=True,
#         )
#         r_quads.raise_for_status()
#         item_infos = consume(r_quads)
#         for item_info in item_infos:
#             mosaic_item_info_pairs.append((mosaic, item_info))

#     items = []
#     for mosaic, item_info in mosaic_item_info_pairs:
#         items.append(create_item(mosaic, item_info))

#     return items
