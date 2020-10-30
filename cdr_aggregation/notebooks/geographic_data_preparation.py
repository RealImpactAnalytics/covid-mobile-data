#!/usr/bin/env python
# -*- coding: utf-8 -*-

from modules.DataSource import *
from modules.setup import *


config_file = '../config_file.py'
exec(open(config_file).read())

ds = DataSource(datasource_configs)
ds.show_config()

ds.load_standardized_parquet_file()
ds.load_geo_csvs()

# Use this in case you want to cluster the towers and create a distance matrix

ds.create_gpds()

from modules.tower_clustering import *

clusterer = tower_clusterer(ds, 'admin2', 'ID_2')
ds.admin2_tower_map, ds.distances = clusterer.cluster_towers()
clusterer = tower_clusterer(ds, 'admin3', 'ADM3_PCODE')
ds.admin3_tower_map, ds.distances  = clusterer.cluster_towers()

# Use this in case you want to create a voronoi tesselation

from modules.voronoi import *

voronoi = voronoi_maker(ds, 'admin3', 'ADM3_PCODE')
ds.voronoi = voronoi.make_voronoi()
