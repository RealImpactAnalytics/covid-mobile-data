#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime as dt
import pyspark.sql.types as T

from covid_mobile_data_wb.cdr_aggregation.notebooks.modules.DataSource import DataSource
from covid_mobile_data_wb.cdr_aggregation.notebooks.modules.folder_utils import setup_folder
from covid_mobile_data_wb.cdr_aggregation.notebooks.modules.priority_aggregator import priority_aggregator


def execute_wb_code(
        spark,
        start_date,
        end_date,
        wb_path,
        country_code="country",
        operator_code="operator"):

    start_date = dt.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = dt.datetime.strptime(end_date, "%Y-%m-%d")

    # Gives necessary configuration to the external code
    datasource_configs = {
        "base_path": wb_path,  # code requires a data path
        "country_code": country_code,
        "telecom_alias": operator_code,
        "schema": T.StructType([
                T.StructField("msisdn", T.StringType(), True),
                T.StructField("call_datetime", T.StringType(), True),
                T.StructField("location_id", T.StringType(), True)
            ]),
        "data_paths": ["*.csv"],
        "filestub": "data-file",
        "geofiles": {
            "tower_sites": f'{country_code}_sites.csv',
            "admin1": f'{country_code}_admin1_shapefile.csv',
            "admin1_tower_map": f"{country_code}_admin1_tower_map.csv",
            "admin2": f'{country_code}_admin2_shapefile.csv',
            "admin2_tower_map": f"{country_code}_admin2_tower_map.csv",
            "admin3": f'{country_code}_admin3_shapefile.csv',
            "admin3_tower_map": f"{country_code}_admin3_tower_map.csv",
            "voronoi": f"{country_code}_voronoi_shapefile.csv",
            "voronoi_tower_map": f"{country_code}_voronoi_tower_map.csv",
            "distances": f"{country_code}_distances_pd_long.csv",
        },
        "shapefiles": ['admin2', 'admin3'],
        "dates": {'start_date': start_date, 'end_date': end_date},
        "load_seperator": ",",  # typo included
        "load_datemask": "yyyy-MM-dd HH:mm:ss",
        "load_mode": "DROPMALFORMED"
    }

    # Init the DataSource class
    ds = DataSource(spark, datasource_configs)

    # Create necessary folders
    setup_folder(ds)

    # Load full set of parquet files
    ds.load_standardized_parquet_file()

    print("="*20)
    ds.parquet_df.printSchema()
    ds.parquet_df.select("call_date").distinct().show(10)
    ds.parquet_df.select("location_id").distinct().show(10)
    ds.parquet_df.show(10)
    print("="*20)

    # Load geo data
    ds.load_geo_csvs()

    # Priority indicators for admin1
    agg_priority_admin1 = priority_aggregator(
        result_stub='/admin1/priority',
        datasource=ds,
        regions='admin1_tower_map')

    agg_priority_admin1.attempt_aggregation()

    # Priority indicators for admin2
    agg_priority_admin2 = priority_aggregator(
        result_stub='/admin2/priority',
        datasource=ds,
        regions='admin2_tower_map')

    agg_priority_admin2.attempt_aggregation()

    # Priority indicators for admin3
    agg_priority_admin3 = priority_aggregator(
        result_stub='/admin3/priority',
        datasource=ds,
        regions='admin3_tower_map')

    agg_priority_admin3.attempt_aggregation()

    # Priority indicators for tower-cluster
    agg_priority_tower = priority_aggregator(
        result_stub='/voronoi/priority',
        datasource=ds,
        regions='voronoi_tower_map')

    agg_priority_tower.attempt_aggregation()
