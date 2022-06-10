# Planet NICFI ETL

This package  is responsible for extracting assets from the Planet API to Azure.

## Azure resources

We expect that the following Azure resources exist.

|                Resource                 |      Purpose      |
| --------------------------------------- | ----------------- |
| Storage container planet/nicfi          | Stores the assets |
| Storage container planet/nicfi-etl-data | Stores the  |
| Storage container planet/nicfi          | Stores the assets |

## Setup

You'll need to configure an environment variable with the following

|                  Key                  |                                   Description                                   |
| ------------------------------------- | ------------------------------------------------------------------------------- |
| ETL_PLANET_API_KEY                    | Your key for the Planet API                                                     |
| ETL_ASSET_CREDENTIAL                  | Read / write token to write assets to the planet/nicfi blob storage container   |
| ETL_QUADS_TABLE_CREDENTIAL            | Read / write token to write to the planet/quads storage table                   |
| ETL_ETL_TABLE_CREDENTIALS             | Read / write token to write to the planet/etl storage table                     |
| JUPYTERHUB_API_TOKEN                  | Your JupyterHub API token                                                       |
| DASK_GATEWAY__AUTH__TYPE              | jupyterhub                                                                      |
| DASK_GATEWAY__CLUSTER__OPTIONS__IMAGE | Should match what's in the docker-compose.yml                                   |
| DASK_GATEWAY__ADDRESS                 | https://pcc-staging.westeurope.cloudapp.azure.com/compute/services/dask-gateway |
| DASK_GATEWAY__PROXY_ADDRESS           | gateway://pcc-staging-dask.westeurope.cloudapp.azure.com:80                     |


## Testing


```bash
$ docker-compose run test
```