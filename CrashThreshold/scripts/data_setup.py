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
    segments_touse.to_postgis('crash_segments', con=ENGINE, if_exists="replace")
    #s = len(segments_touse)
    #print(f'Segments: {s}')
    '''
    #read county boundaries from gis database
    Q_counties = """
    select *
    from boundaries.countyboundaries c 
    where dvrpc_reg  = 'Yes'
    and state = 'PA';"""
    counties = gpd.GeoDataFrame.from_postgis(
        Q_counties, 
        con = GIS_ENGINE,
        geom_col = "shape",
    )
    #write to postgis
    counties.to_postgis('county_boundaries', con=ENGINE, if_exists="replace")
   
    #join segments to counties
    Q_join = fr"""
    with segs as(
        select *, st_transform(geometry, 26918) as tshape 
        from crash_segments 
        ), 
    boundaries as (
        select *
        from county_boundaries c 
        where dvrpc_reg  = 'Yes'
        and state = 'PA'
        )
    select s.*, b.co_name
    from segs s
    join boundaries b 
    on ST_Contains(b.shape, s.tshape);
    """
    joined = gpd.GeoDataFrame.from_postgis(
        Q_join,
        con = ENGINE,
        geom_col = "tshape"
    )
    #write dataframe to postgres database
    joined.to_postgis('crashes_bycounty', con=ENGINE, if_exists="replace")
    t = len(joined)
    print(f'Joined: {t}')
    print("To postgis: Complete")
    

    #return joined
    '''

if __name__ == "__main__":
    crash_data_setup()
