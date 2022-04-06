import stactools.core

from stactools.planet_nicfi.stac import create_collection, create_item

__all__ = ["create_collection", "create_item"]

stactools.core.use_fsspec()


def register_plugin(registry):
    from stactools.planet_nicfi import commands

    registry.register_subcommand(commands.create_planetnicfi_command)


__version__ = "0.1.0"
