#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime as dt

from etl_code.covid_mobile_data.cdr_aggregation.notebooks.modules.DataSource import *
from etl_code.covid_mobile_data.cdr_aggregation.notebooks.modules.setup import *

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


def execute_wb_code():

    # Preprocess raw csv data
    STANDARDIZE_CSV_FILES = True

    # Instead of loading the full data, use these to create and load a random sample of users
    CREATE_SAMPLE = False
    LOAD_SAMPLE = False

    # Create standard GIS files for aggregations. This step does not
    # depend on the CDR data directly, but is needed to run the aggregations.
    # This only has to be done once per country and operator
    TOWER_CLUSTER = False
    VORONOY_TESSELATION = False

    # Run aggregation for different admin levels
    RUN_AGGREGATION_ADMIN2 = True
    RUN_AGGREGATION_ADMIN3 = True
    RUN_AGGREGATION_TOWER_CLUSTER = True

    # config_file = '../config_file.py'
    # exec(open(config_file).read())

    schema = StructType([
        StructField("msisdn", IntegerType(), True),
        StructField("call_datetime", StringType(), True),  # load as string, turned into datetime in standardize_csv_files()
        StructField("location_id", StringType(), True)
    ])

    datasource_configs = {
        "base_path": "/Users/balthazarcoutant/Documents/Github/covid-mobile-data/cdr_aggregation/data",  # folder path used in this docker env
        "country_code": "country",
        "telecom_alias": "operator",
        "schema": schema,
        "data_paths": ["*.csv"],
        "filestub": "data-file",
        "geofiles": {
            "tower_sites": 'ci_sites.csv',
            "admin1": 'ci_admin1_shapefile.csv',
            "admin1_tower_map": "ci_admin1_tower_mapping.csv",
            "admin2": 'ci_admin2_shapefile.csv',
            "admin2_tower_map": "ci_admin2_tower_mapping.csv",
            "admin3": 'ci_admin1_shapefile.csv',
            "admin3_tower_map": "ci_admin3_tower_mapping.csv",
            "voronoi": "country_voronoi_shapefile.csv",
            "voronoi_tower_map": "country_voronoi_tower_map.csv",
            "distances": "country_distances_pd_long.csv",
        },
        "shapefiles": ['admin2', 'admin3'],
        "dates": {'start_date': dt.datetime(2020, 3, 1),
                  'end_date': dt.datetime(2020, 3, 31)},
        "load_seperator": ",",
        "load_datemask": "yyyy-MM-dd HH:mm:ss",
        "load_mode": "DROPMALFORMED"
    }

    ds = DataSource(datasource_configs)
    ds.show_config()

    # # Import data

    # ## Load CDR data

    # ### Process/standardize raw data, save as parquet, and then load it

    # These processes only have to be done once for CSV batch.
    # Once .parquet files are saved for that CSV batch, aggregations
    # can run on them.

    # Load and standardize raw CDR csvs
    if STANDARDIZE_CSV_FILES:
        ds.standardize_csv_files(show=True)
        ds.save_as_parquet()

    # ### Load standardized data

    # Load full set of parquet files
    ds.load_standardized_parquet_file()

    # ## Load geo data
    ds.load_geo_csvs()

    ## Use this in case you want to cluster the towers and create a distance matrix
    if TOWER_CLUSTER:
        ds.create_gpds()
        from etl_code.covid_mobile_data.cdr_aggregation.notebooks.modules.tower_clustering import *

        clusterer = tower_clusterer(ds, 'admin2', 'ID_2')
        ds.admin2_tower_map, ds.distances = clusterer.cluster_towers()
        clusterer = tower_clusterer(ds, 'admin3', 'ADM3_PCODE')
        ds.admin3_tower_map, ds.distances = clusterer.cluster_towers()

    ## Use this in case you want to create a voronoi tesselation
    if VORONOY_TESSELATION:
        from etl_code.covid_mobile_data.cdr_aggregation.notebooks.modules.voronoi import *

        voronoi = voronoi_maker(ds, 'admin3', 'ADM3_PCODE')
        ds.voronoi = voronoi.make_voronoi()

    # # Run aggregations

    # ## Priority indicators for admin2

    if RUN_AGGREGATION_ADMIN2:
        agg_priority_admin2 = priority_aggregator(
            result_stub='/admin2/priority',
            datasource=ds,
            regions='admin2_tower_map')

        agg_priority_admin2.attempt_aggregation()

    # ## Priority indicators for admin3

    if RUN_AGGREGATION_ADMIN3:
        agg_priority_admin3 = priority_aggregator(
            result_stub='/admin3/priority',
            datasource=ds,
            regions='admin3_tower_map')

        agg_priority_admin3.attempt_aggregation()

    # ## Priority indicators for tower-cluster

    if RUN_AGGREGATION_TOWER_CLUSTER:
        agg_priority_tower = priority_aggregator(
            result_stub='/voronoi/priority',
            datasource=ds,
            regions='voronoi_tower_map')

        agg_priority_tower.attempt_aggregation()
