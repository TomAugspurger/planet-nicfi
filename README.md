# stactools-planet-nicfi

[![PyPI](https://img.shields.io/pypi/v/stactools-planet-nicfi)](https://pypi.org/project/stactools-planet-nicfi/)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/stactools-packages/planet-nicfi/main?filepath=docs/installation_and_basic_usage.ipynb)

- Name: planet-nicfi
- Package: `stactools.planet_nicfi`
- PyPI: https://pypi.org/project/stactools-planet-nicfi/
- Owner: @TomAugspurger
- Dataset homepage: http://example.com
- STAC extensions used:
  - [proj](https://github.com/stac-extensions/projection/)
- Extra fields:
  - `planet-nicfi:custom`: A custom attribute

A short description of the package and its usage.

## Examples

### STAC objects

- [Collection](examples/collection.json)
- [Item](examples/item/item.json)

### Command-line usage

Description of the command line functions

```bash
$ stac planet-nicfi create-item source destination
```

Use `stac planet-nicfi --help` to see all subcommands and options.

## Planetary Computer collection


```
stac planetnicfi create-collection visual visual-template.json \
   --thumbnail="https://planet-pulse-assets-production.s3.amazonaws.com/uploads/2020/09/nicfi_tropics_basemap_map_d1.jpg" \
   --extra-field "msft:short_description=Planet’s high-resolution, analysis-ready mosaics of the world’s tropics" \
   --extra-field "msft:storage_account=planet" \
   --extra-field "msft:container=nicfi" \
   --extra-field "msft:group_id=planet-nicfi"

stac planetnicfi create-collection analytic analytic-template.json \
   --thumbnail="https://planet-pulse-assets-production.s3.amazonaws.com/uploads/2020/09/nicfi_tropics_basemap_map_d1.jpg" \
   --extra-field "msft:short_description=Planet’s high-resolution, analysis-ready mosaics of the world’s tropics" \
   --extra-field "msft:storage_account=planet" \
   --extra-field "msft:container=nicfi" \
   --extra-field "msft:group_id=planet-nicfi"
```

## ETL

This repository also hosts the ETL code for extracting from the Planet API to Azure Blob Storage. See [etl](./src/azure/etl/README.md) for more.