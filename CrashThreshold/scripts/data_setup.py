"""
data_setup.py
------------------
This script loads PennDOT segments pre-joined to crash data,
joins it with county boundaries, and sets it up in postgres for analysis.
"""

import geopandas as gpd
import pandas as pd
from sqlalchemy_utils import database_exists, create_database
import env_vars as ev
from env_vars import GIS_ENGINE, ENGINE, DATA_ROOT



def crash_data_setup():

    shape = "RMS_Joined"

    #create database and enable postgis
    if not database_exists(ENGINE.url):
        create_database(ENGINE.url)
    ENGINE.execute("CREATE EXTENSION IF NOT EXISTS postgis;")

    #read crash segment shapefile and write to postgres
    segments = gpd.read_file(fr"{ev.DATA_ROOT}/{shape}.shp")
    segments_clean = segments[segments.geometry.type == 'LineString']
    segments_touse = segments_clean[(segments_clean.LANE_CNT_1 >= 4)|(segments_clean.lane_cnt >= 4)]
    seg_no_hwy = segments_touse[(segments_touse.st_rt_no != '0095') & (segments_touse.st_rt_no != '0076') & (segments_touse.st_rt_no != '0476')]
    seg_no_hwy.to_postgis('crash_segments', con=ENGINE, if_exists="replace")

if __name__ == "__main__":
    crash_data_setup()
