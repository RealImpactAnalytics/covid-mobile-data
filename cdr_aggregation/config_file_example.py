import datetime as dt
import pyspark.sql.types as T

schema = T.StructType([  # for CDRs in CSV format: T.StringType() will be converted when needed
    T.StructField("msisdn", T.StringType(), True),
    T.StructField("call_datetime", T.StringType(), True),
    T.StructField("location_id", T.StringType(), True)
])

datasource_configs = {
    "base_path": "./data",  # folder path used in this docker env
    "country_code": "country",
    "telecom_alias": "operator",
    "schema": schema,
    "data_paths": ["*.csv"],
    "filestub": "data-file",  # common name of the results files
    "geofiles": {
        "tower_sites": "sites.csv",
        "distances": "distances_pd_long.csv",
        "admin1": "admin1_shapefile.csv",
        "admin1_tower_map": "admin1_tower_mapping.csv",
        "admin2": "admin2_shapefile.csv",
        "admin2_tower_map": "admin2_tower_mapping.csv",
        "admin3": "admin3_shapefile.csv",
        "admin3_tower_map": "admin3_tower_mapping.csv",
        "voronoi": "voronoi_shapefile.csv",
        "voronoi_tower_map": "voronoi_tower_map.csv",
    },
    "shapefiles": ["admin1", "admin2", "admin3", "voronoi"],
    "dates": {
        "start_date": dt.datetime(2020, 1, 1),
        "end_date": dt.datetime(2020, 1, 31)
    },
    "load_seperator": ",",  # for CDRs in CSV format
    "load_datemask": "yyyy-MM-dd HH:mm:ss",
    "load_mode": "DROPMALFORMED"
}
