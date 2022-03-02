import logging
import datetime

import dateutil
import pystac
import pystac
import rasterio
import rio_stac
import rio_stac
import requests


logger = logging.getLogger(__name__)
PDF_MEDIA_TYPE = "application/pdf"

BANDS = {
    "analytic": [
            pystac.extensions.eo.Band({"name": "Blue", "common_name": "blue"}),
            pystac.extensions.eo.Band({"name": "Green", "common_name": "green"}),
            pystac.extensions.eo.Band({"name": "Red", "common_name": "red"}),
            pystac.extensions.eo.Band({"name": "NIR", "common_name": "nir", "description": "near-infrared"}),
    ],
    "visual": [
            pystac.extensions.eo.Band({"name": "Red", "common_name": "red"}),
            pystac.extensions.eo.Band({"name": "Green", "common_name": "green"}),
            pystac.extensions.eo.Band({"name": "Blue", "common_name": "blue"}),
        ]
}


def create_collection(kind) -> pystac.Collection:
    """Create a STAC Collection

    This function includes logic to extract all relevant metadata from
    an asset describing the STAC collection and/or metadata coded into an
    accompanying constants.py file.

    See `Collection<https://pystac.readthedocs.io/en/latest/api.html#collection>`_.

    Returns:
        Collection: STAC Collection object
    """
    assert kind in {"visual", "analytic"}

    providers = [
        pystac.Provider(
            name="Planet",
            description=(
                "Contact Planet at "
                "[planet.com/contact-sales](https://www.planet.com/contact-sales/)"
            ),
            url="http://planet.com",
            roles=["producer", "processor"],
        )
    ]
    links = [
        pystac.Link(
            rel=pystac.RelType.LICENSE,
            target="https://assets.planet.com/docs/Planet_ParticipantLicenseAgreement_NICFI.pdf",
            media_type=PDF_MEDIA_TYPE,
            title="Participant License Agreement.",
        ),
        pystac.Link(
            rel="documentation",
            target="https://assets.planet.com/docs/NICFI_UserGuidesFAQ.pdf",
            media_type=PDF_MEDIA_TYPE,
            title="Participant License Agreement.",
        ),
    ]

    collection = pystac.Collection(
        id=f"planet-nicfi-{kind}",
        title=f"Planet NICFI {kind}",
        description="{{ description.md }}",
        license="proprietary",
        providers=providers,
        catalog_type=pystac.CatalogType.RELATIVE_PUBLISHED,
        extent=pystac.Extent(
            pystac.SpatialExtent([[-180.0, -34.161818157002, 180.0, 30.145127179625]]),
            pystac.TemporalExtent(
                [datetime.datetime(2015, 12, 1, tzinfo=datetime.timezone.utc), None]
            ),
        ),
    )
    collection.add_links(links)
    descriptions = {
        "visual": (
            "a 'true-colour' representation of spatially accurate data with "
            "minimized haze, illumination, and topographic effects"
        ),
        "analytic": (
            "a 'ground truth' representation of spatially accurate data with "
            "minimized effects of atmosphere and sensor characteristics"
        ),
    }

    item_assets = {
        "thumbnail": pystac.extensions.item_assets.AssetDefinition(
            {
                "type": pystac.MediaType.PNG,
                "roles": ["thumbnail"],
                "title": "Thumbnail",
            },
        ),
        "data": pystac.extensions.item_assets.AssetDefinition(
            {
                "type": pystac.MediaType.COG,
                "roles": ["data"],
                "title": "Data",
                "description": descriptions[kind],
            },
        ),
    }

    item_assets_ext = pystac.extensions.item_assets.ItemAssetsExtension.ext(
        collection, add_if_missing=True
    )
    item_assets_ext.item_assets = item_assets
    eo_bands = {
        "analytic": [
            {"name": "Blue", "common_name": "blue", "description": "visible blue"},
            {"name": "Green", "common_name": "green", "description": "visible green"},
            {"name": "Red", "common_name": "red", "description": "visible red"},
            {"name": "NIR", "common_name": "nir", "description": "near-infrared"},
        ],
        "visual": [
            {"name": "Red", "common_name": "red", "description": "visible red"},
            {"name": "Green", "common_name": "green", "description": "visible green"},
            {"name": "Blue", "common_name": "blue", "description": "visible blue"},
        ]
    }
    collection.summaries.add("gsd", [4.77])
    collection.summaries.add("eo:bands", eo_bands[kind])

    return collection


def create_item(asset_href, mosaic, item_info, transform_href=lambda x: x):
    """
    Create a STAC item for a quad item from `mosaic`.
    """
    # TODO: the item should include the mosaic type (analytical, mosaic)
    # blob_name, thumbnail_name = copy_item(
    #     mosaic, item_info, redownload=redownload, overwrite=overwrite
    # )
    r_image = requests.get(transform_href(asset_href))
    r_image.raise_for_status()
    image = r_image.content

    # Done with I/O

    start_datetime = dateutil.parser.parse(mosaic["first_acquired"])
    end_datetime = dateutil.parser.parse(mosaic["last_acquired"])
    timestamp = start_datetime + (end_datetime - start_datetime) / 2

    properties = {
        "start_datetime": mosaic["first_acquired"],
        "end_datetime": mosaic["last_acquired"],
        "gsd": 4.77,
    }
    item_id = f"{mosaic['id']}-{item_info['id']}"

    with rasterio.MemoryFile(image) as f:
        item = rio_stac.create_stac_item(
            f,
            input_datetime=timestamp,
            properties=properties,
            id=item_id,
            with_proj=True,
            with_raster=True,
            asset_name="data",
            asset_roles=["data"],
            asset_media_type=str(pystac.MediaType.COG),
            asset_href=asset_href,
        )

    thumbnail_href = asset_href.rsplit("/", 1)[0] + "/thumbnail.png"
    item.add_asset(
        "thumbnail",
        pystac.Asset(
            thumbnail_href,
            media_type=pystac.MediaType.PNG,
            roles=["thumbnail"],
            title="Thumbnail",
        ),
    )
    item.add_link(
        pystac.Link(
            "via",
            # use .split to strip out the API key
            target=item_info["_links"]["_self"].split("?")[0],
            media_type=pystac.MediaType.JSON,
            title="Planet Item",
        )
    )
    item.add_link(
        pystac.Link(
            "via",
            target=mosaic["_links"]["_self"].split("?")[0],
            media_type=pystac.MediaType.JSON,
            title="Planet Mosaic",
        )
    )

    ext = pystac.extensions.eo.EOExtension.ext(item.assets["data"], add_if_missing=True)
    kind = "analytic" if "analytic" in mosaic["name"] else "visual"
    ext.bands = BANDS[kind]

    return item