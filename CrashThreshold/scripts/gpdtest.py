import geopandas as gpd
import pandas as pd
from sqlalchemy_utils import database_exists, create_database
import env_vars as ev
from env_vars import ENGINE
pd.options.mode.chained_assignment = None  # default='warn'

#read in segments joined to county boundaries from postgres and create dataframe
Q_grab = """
    SELECT *
    FROM crashes_bycounty
    """
gdf = gpd.read_postgis(Q_grab, ENGINE, geom_col='tshape', crs=26918)

#gdf = gpd.GeoDataFrame(df, crs="EPSG:26918", geometry=df['tshape'])
print(gdf.dtypes)