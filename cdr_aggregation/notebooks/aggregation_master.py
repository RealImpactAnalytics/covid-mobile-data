#!/usr/bin/env python
# coding: utf-8

# # Production of indicators for the COVID19 Mobility Task Force
# 
# In this notebook we produce indicators for the [COVID19 Mobility Task Force](https://github.com/worldbank/covid-mobile-data).
# 
# [Flowminder](https://covid19.flowminder.org) indicators are produced to increase the availability of comparable datasets across countries, and have been copied without modification from the [Flowminder COVID-19 github repository](https://github.com/Flowminder/COVID-19) (except for the start and end dates). These have been supplemented by a set of *priority* indicators with data for ingestion into the dashboard in this repository.
# 
# In this notebook we produce indicators in the following four steps:
# 
# - **Import code**: The code for the aggregation is included in the 'custom_aggregation' and 'flowminder_aggregation' scripts
# - **Import data**: 
# To set up the data import we need to place the CDR data files into the `data/new/CC/telco/` folder, where we replace `CC` with the country code and `telco` with the company abbreviation. 
# We also need to place csv files with the tower-region mapping and distance matrices into the `data/support-data/CC/telco/geofiles` folder, and then modify the `data/support_data/config_file.py` to specify:
#     - *geofiles*: the names of the geofiles, 
#     - *country_code*: country code and company abbreviation,
#     - *telecom_alias*: the path to the `data` folder,
#     - *data_paths*: the names to the subfolders in `data/new/CC/telco/` that hold the csv files. Simply change this to `[*]` if you didn't create subfolders and want to load all files.
#     - *dates*: set the start and end date of the data you want to produce the indicators for.
#     
# Find more information about the `config_file.py` settings see the [github page](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation).
#     
# - **Run aggregations**: By default, we produce all flowminder and priority indicators. We've included 4 re-tries in case of failure, which we have experienced to help on databricks but is probably irrelevant in other settings. Note that before you can re-run these aggregations, you need to move the csv outputs that have been saved in `data/results/CC/telco/` in previous runs to another folder, else these indicators will be skipped. This prevents you from accidentally overwriting previous results. This way you can also delete the files only for the indicators you want to re-produce, and skip any indicatos you don't want to re-produce.
# 
# The outcome of this effort will be used to inform policy making using a [mobility indicator dashboard](https://github.com/worldbank/covid-mobile-data/tree/master/dashboard-dataviz).

# # Section switches

# In[ ]:



# Preprocess raw csv data
STANDARDIZE_CSV_FILES = False
SAVE_STAND_PARQUE_FILES = False    

# Load preprocesed data
LOAD_FULL_CSV_DATA = True
# Alternatively, specify and load hive table
SPECIFY_HIVE_TABLE = False

# Instead of loading the full data, use these to create and load a
# random sample of users
CREATE_SAMPLE = False
LOAD_SAMPLE = False

# Create standard GIS files for aggregations. This step does not 
# depend on the CDR data directly, but is needed to run the aggregations.
# This only has to be done once per country and operator

TOWER_CLUSTER = False
VORONOY_TESSELATION = False

# Run aggregation for different admin levels
RUN_AGGREGATION_ADMIN2 = False
RUN_AGGREGATION_ADMIN3 = False
RUN_AGGREGATION_TOWER_CLUSTER = False


# # Import code

# In[ ]:


get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')


# In[ ]:


from modules.DataSource import *


# In[ ]:


config_file = '../config_file.py'


# In[ ]:


exec(open(config_file).read())


# In[ ]:


ds = DataSource(datasource_configs)
ds.show_config()


# In[ ]:


from modules.setup import *


# # Import data

# ## Load CDR data

# ### Process/standardize raw data, save as parquet, and then load it

# In[ ]:


# These processes only have to be done once for CSV batch. 
# Once .parquet files are saved for that CSV batch, aggregations 
# can run on them. 

# Load and standardize raw CDR csvs
if STANDARDIZE_CSV_FILES:
    ds.standardize_csv_files(show=True)

# Export as parquet files 
if SAVE_STAND_PARQUE_FILES:
    ds.save_as_parquet()


# ### Load standardized data

# In[ ]:


# Load full set of parquet files
if LOAD_FULL_CSV_DATA:
    ds.load_standardized_parquet_file()
# Specify and load hive data
elif SPECIFY_HIVE_TABLE:
    ds.parquet_df = ds.spark.sql("""SELECT {} AS msisdn, 
                                           {} AS call_datetime, 
                                           {} AS location_id FROM {}""".format(ds.hive_vars['msisdn'],
                                                                               ds.hive_vars['call_datetime'],
                                                                               ds.hive_vars['location_id'],))
      
    


# ### Or load a sample file

# In[ ]:


## Use this in case you want to sample the data and run the code on the sample
if CREATE_SAMPLE:
    ds.sample_and_save(number_of_ids=1000)

# This will replace ds.parque_df with the created sample
if LOAD_SAMPLE:
    ds.load_sample('sample_feb_mar2020')
    ds.parquet_df = ds.sample_df


# ## Load geo data

# In[ ]:


ds.load_geo_csvs()


# In[ ]:


## Use this in case you want to cluster the towers and create a distance matrix
if TOWER_CLUSTER:
    ds.create_gpds()
    from modules.tower_clustering import *
    clusterer = tower_clusterer(ds, 'admin2', 'ID_2')
    ds.admin2_tower_map, ds.distances = clusterer.cluster_towers()
    clusterer = tower_clusterer(ds, 'admin3', 'ADM3_PCODE')
    ds.admin3_tower_map, ds.distances  = clusterer.cluster_towers()


# In[ ]:


## Use this in case you want to create a voronoi tesselation
if VORONOY_TESSELATION:
    from modules.voronoi import *
    voronoi = voronoi_maker(ds, 'admin3', 'ADM3_PCODE')
    ds.voronoi = voronoi.make_voronoi()


# # Run aggregations

# ## Priority indicators for admin2

# In[ ]:


if RUN_AGGREGATION_ADMIN2:
    agg_priority_admin2 = priority_aggregator(result_stub = '/admin2/priority',
                                   datasource = ds,
                                   regions = 'admin2_tower_map')

    agg_priority_admin2.attempt_aggregation()
    # You can produce a subset of indicators by passing a dictionary with keys as a file name and a list with indicator ids and aggregation levels.
    #agg_priority_admin2.attempt_aggregation(indicators_to_produce = {'unique_subscribers_per_day' : ['unique_subscribers', 'day'],
    #                                                                  'percent_of_all_subscribers_active_per_day' : ['percent_of_all_subscribers_active', 'day'],
    #                                                                  'origin_destination_connection_matrix_per_day' : ['origin_destination_connection_matrix', 'day'],
    #                                                                  'mean_distance_per_day' : ['mean_distance', 'day'],
    #                                                                  'mean_distance_per_week' : ['mean_distance', 'week'],
    #                                                                  'origin_destination_matrix_time_per_day' : ['origin_destination_matrix_time', 'day'],
    #                                                                  'home_vs_day_location_per_day' : ['home_vs_day_location_per_day', ['day','week']],
    #                                                                  'home_vs_day_location_per_day' : ['home_vs_day_location_per_day', ['day','month']]})


# ## Priority indicators for admin3

# In[ ]:


if RUN_AGGREGATION_ADMIN3:
    agg_priority_admin3 = priority_aggregator(result_stub = '/admin3/priority',
                                datasource = ds,
                                regions = 'admin3_tower_map')

    agg_priority_admin3.attempt_aggregation()


# ## Priority indicators for tower-cluster

# In[ ]:


if RUN_AGGREGATION_TOWER_CLUSTER:
    agg_priority_tower = priority_aggregator(result_stub = '/voronoi/priority',
                                   datasource = ds,
                                   regions = 'voronoi_tower_map')

    agg_priority_tower.attempt_aggregation()


# In[ ]:


if RUN_AGGREGATION_TOWER_CLUSTER:
    agg_priority_tower_harare = priority_aggregator(result_stub = '/voronoi/priority/harare',
                                   datasource = ds,
                                   regions = 'voronoi_tower_map_harare')

    agg_priority_tower_harare.attempt_aggregation(indicators_to_produce = {'origin_destination_connection_matrix_per_day' : ['origin_destination_connection_matrix', 'day']})


# In[ ]:


if RUN_AGGREGATION_TOWER_CLUSTER:
    agg_priority_tower_bulawayo = priority_aggregator(result_stub = '/voronoi/priority/bulawayo',
                                   datasource = ds,
                                   regions = 'voronoi_tower_map_bulawayo')

    agg_priority_tower_bulawayo.attempt_aggregation(indicators_to_produce = {'origin_destination_connection_matrix_per_day' : ['origin_destination_connection_matrix', 'day']})


# # Produce script

# In[1]:


get_ipython().system('jupyter nbconvert --to script *.ipynb')


# In[ ]:


test = pd.DataFrame([[np.nan,1, 2],[0,1,2]])


# In[ ]:


test = ds.spark.createDataFrame([[None,1, 1,2],[2,2,2,2]])


# In[ ]:


test.toPandas()


# In[ ]:


test.groupby('_4').sum().toPandas()


# In[ ]:


test.withColumn('f', F.col('_1') + F.col('_2')).toPandas()


# In[ ]:




