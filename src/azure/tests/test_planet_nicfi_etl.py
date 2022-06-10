import datetime
import os

import shapely.geometry
import stactools_nicfi_etl.etl


def test_integration():
    geometry = shapely.geometry.box(
        -60.64453124152092, -1.2303741772055612, -60.468749991545344, -1.054627942073222
    )
    start_datetime = datetime.datetime(2022, 1, 1)
    end_datetime = datetime.datetime(2022, 1, 2)
    planet_api_key = os.environ["ETL_PLANET_API_KEY"]
    asset_credential = os.environ["ETL_ASSET_CREDENTIAL"]
    nicfi_table_credential = os.environ["ETL_QUADS_TABLE_CREDENTIAL"]
    results = stactools_nicfi_etl.etl.process_geometry(
        geometry,
        start_datetime,
        end_datetime,
        planet_api_key,
        asset_credential,
        nicfi_table_credential,
    )

    assert len(results["finished"]) == 8
