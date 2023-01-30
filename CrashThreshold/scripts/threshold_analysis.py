import geopandas as gpd
import pandas as pd
from sqlalchemy_utils import database_exists, create_database
import env_vars as ev
from env_vars import ENGINE

#read in segments joined to county boundaries from postgres and create dataframe
Q_grab = """
    SELECT *
    FROM crashes_bycounty
    """
df = pd.read_sql_query(Q_grab, ENGINE)
df.head()