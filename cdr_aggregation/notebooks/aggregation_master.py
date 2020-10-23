#!/usr/bin/env python
# -*- coding: utf-8 -*-

from etl_code.covid_mobile_data.cdr_aggregation.notebooks.modules.DataSource import *
from etl_code.covid_mobile_data.cdr_aggregation.notebooks.modules.setup import *


config_file = '../config_file.py'
exec(open(config_file).read())

ds = DataSource(datasource_configs)
ds.show_config()

ds.standardize_csv_files(show=True)
ds.save_as_parquet()

ds.load_standardized_parquet_file()
ds.load_geo_csvs()

# # Run aggregations

# ## Flowminder indicators for admin2

agg_flowminder_admin2 = flowminder_aggregator(
    result_stub='/admin2/flowminder',
    datasource=ds,
    regions='admin2_tower_map')

agg_flowminder_admin2.attempt_aggregation()

# ## Flowminder indicators for admin3

agg_flowminder_admin3 = flowminder_aggregator(
    result_stub='/admin3/flowminder',
    datasource=ds,
    regions='admin3_tower_map')

agg_flowminder_admin3.attempt_aggregation()

# ## Priority indicators for admin2

agg_priority_admin2 = priority_aggregator(
    result_stub='/admin2/priority',
    datasource=ds,
    regions='admin2_tower_map')

agg_priority_admin2.attempt_aggregation(
    indicators_to_produce={
        'unique_subscribers_per_day': ['unique_subscribers', 'day'],
        'percent_of_all_subscribers_active_per_day': ['percent_of_all_subscribers_active', 'day'],
        'origin_destination_connection_matrix_per_day': ['origin_destination_connection_matrix', 'day'],
        'mean_distance_per_day': ['mean_distance', 'day'],
        'mean_distance_per_week': ['mean_distance', 'week'],
        'origin_destination_matrix_time_per_day': ['origin_destination_matrix_time', 'day'],
        'home_vs_day_location_per_day': ['home_vs_day_location_per_day', ['day', 'week']],
        'home_vs_day_location_per_day': ['home_vs_day_location_per_day', ['day', 'month']]})

# ## Priority indicators for admin3

agg_priority_admin3 = priority_aggregator(
    result_stub='/admin3/priority',
    datasource=ds,
    regions='admin3_tower_map')

agg_priority_admin3.attempt_aggregation(
    indicators_to_produce={
        'transactions_per_hour': ['transactions', 'hour'],
        'transactions_per_hour': ['transactions', 'hour']})

# ## Scaled priority indicators for admin2

agg_scaled_admin2 = scaled_aggregator(
    result_stub='/admin2/scaled',
    datasource=ds,
    regions='admin2_tower_map')

agg_scaled_admin2.attempt_aggregation()

# ## Priority indicators for tower-cluster

agg_priority_tower = priority_aggregator(
    result_stub='/voronoi/priority',
    datasource=ds,
    regions='voronoi_tower_map')

agg_priority_tower.attempt_aggregation(
    indicators_to_produce={
        'unique_subscribers_per_hour': ['unique_subscribers', 'hour'],
        'mean_distance_per_day': ['mean_distance', 'day'],
        'mean_distance_per_week': ['mean_distance', 'week']})

agg_priority_tower_harare = priority_aggregator(
    result_stub='/voronoi/priority/harare',
    datasource=ds,
    regions='voronoi_tower_map_harare')

agg_priority_tower_harare.attempt_aggregation(
    indicators_to_produce={
        'origin_destination_connection_matrix_per_day': ['origin_destination_connection_matrix', 'day']})

agg_priority_tower_bulawayo = priority_aggregator(
    result_stub='/voronoi/priority/bulawayo',
    datasource=ds,
    regions='voronoi_tower_map_bulawayo')

agg_priority_tower_bulawayo.attempt_aggregation(
    indicators_to_produce={
        'origin_destination_connection_matrix_per_day': ['origin_destination_connection_matrix', 'day']})
