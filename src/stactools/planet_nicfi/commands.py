import logging

import click

from stactools.planet_nicfi import stac

logger = logging.getLogger(__name__)


def create_planetnicfi_command(cli):
    """Creates the stactools-planet-nicfi command line utility."""

    @cli.group(
        "planetnicfi",
        short_help=("Commands for working with stactools-planet-nicfi"),
    )
    def planetnicfi():
        pass

    @planetnicfi.command(
        "create-collection",
        short_help="Creates a STAC collection",
    )
    @click.argument("kind")
    @click.argument("destination")
    @click.option(
        "--thumbnail", default=None, help="URL for the collection thumbnail asset."
    )
    @click.option(
        "--extra-field",
        default=None,
        help="Key-value pairs to include in extra-fields",
        multiple=True,
    )
    def create_collection_command(kind: str, destination: str, thumbnail: str, extra_field):
        """Creates a STAC Collection

        Args:
            kind (str)
            destination (str): An HREF for the Collection JSON
        """
        extra_fields = dict(k.split("=") for k in extra_field)
        collection = stac.create_collection(kind, thumbnail=thumbnail, extra_fields=extra_fields)

        collection.set_self_href(destination)
        collection.save_object()
        return None

    @planetnicfi.command("create-item", short_help="Create a STAC item")
    @click.argument("source")
    @click.argument("destination")
    @click.argument("planet_api_key")
    def create_item_command(source: str, destination: str):
        """Creates a STAC Item

        Args:
            source (str): HREF of the Asset associated with the Item
            destination (str): An HREF for the STAC Collection
        """
        item = stac.create_item(source)

        item.save_object(dest_href=destination)

        return None

    return planetnicfi
