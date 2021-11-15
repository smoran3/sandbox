import psycopg2 as psql
import pandas as pd
import geopandas as gpd
import os
from shapely import geometry
from sqlalchemy import create_engine

#read shapefile into geodataframe
fp = "G:/Shared drives/Bike-Friendly Resurfacing/NJ_SLD/SHP/NJreg_SLD.shp"
gdf = gpd.read_file(fp)

#transform progjection from 3424 to 26918
gdf = gdf.to_crs(epsg=26918)

#write dataframe to postgis
db_connection_url = "postgresql://postgres:root@localhost:5432/BFR_NJ_SLD"
engine = create_engine(db_connection_url)
gdf.to_postgis("sld_njreg", con=engine, if_exists='replace')


#connection to DB
con = psql.connect(
    host = "localhost",
    port = 5432,
    database = "BFR_NJ_SLD",
    user = "postgres",
    password = "root"
)
cur = con.cursor()


# create dataframes containing results from each query
# bike lanes
#bl1: if lanes are narrowed, there is room for bike lanes but not parking
bl1= gpd.GeoDataFrame.from_postgis("""
WITH tblA AS(
    select *
    from sld_njreg s
    where "Juris" = 'County'
    and "Lanes" > 0
    and "Shoulder" < 5
    and ("PaveWid" - ("Shoulder"*2))/"Lanes" > 11
    and ("PaveWid" + ("Shoulder"*2)) - ("Lanes"*11) between 10 and 16
)
SELECT *
FROM tblA
WHERE "Speed" <= 40 
AND "aadt" <= 20000;

""", con, geom_col= "geometry")

#bl2: if lanes are narrowed, there is room for bike lanes and parking
bl2 = gpd.GeoDataFrame.from_postgis("""
WITH tblA AS(
select *
from sld_njreg s
where "Juris" = 'County'
and "Lanes" > 0
and "Shoulder" < 5
and ("PaveWid" - ("Shoulder"*2))/"Lanes" > 11
and ("PaveWid" + ("Shoulder"*2)) - ("Lanes"*11) > 26
)
SELECT *
FROM tblA
WHERE "Speed"<= 40 
AND "aadt" <= 20000
;

""", con, geom_col= "geometry")

#bl3: if lanes are 11', threre is room for bike lanes, but not parking
bl3 = gpd.GeoDataFrame.from_postgis("""
WITH tblA AS(
select *
from sld_njreg s
where "Juris" = 'County'
and "Shoulder" >= 5
and ("PaveWid" + ("Shoulder"*2)) - ("Lanes"*11) between 10 and 16
)
SELECT *
FROM tblA
WHERE "Speed"<= 40 
AND "aadt" <= 20000
;

""", con, geom_col= "geometry")

#bl4: if lanes are 11', there is room for bike lanes and parking
bl4 = gpd.GeoDataFrame.from_postgis(
"""WITH tblA AS(
select *
from sld_njreg s
where "Juris" = 'County'
and "Shoulder" >= 5
and ("PaveWid" + ("Shoulder"*2)) - ("Lanes"*11) > 26
)
SELECT *
FROM tblA
WHERE "Speed"<= 40 
AND "aadt" <= 20000;
""", con, geom_col= "geometry")

#sharrows
#s1: lanes cannot be narrowed and shoulders are not wide enough for bike lanes; speeds/volume low
s1 = gpd.GeoDataFrame.from_postgis(
"""WITH tblA AS(
select *
from sld_njreg s
where "Juris" = 'County'
and "Lanes" > 0
and "Shoulder" < 5
and ("PaveWid" - ("Shoulder"*2))/"Lanes" <= 11
)
SELECT *
FROM tblA
WHERE "Speed"<= 25 
AND "aadt" <= 3000;
""", con, geom_col= "geometry")

#s2: even if lanes are narrowed, there is not enough spare room for bike lanes; speed/volume low
s2 = gpd.GeoDataFrame.from_postgis(
"""WITH tblA AS(
select *
from sld_njreg s
where "Juris" = 'County'
and "Lanes" > 0
and "Shoulder" < 5
and ("PaveWid" - ("Shoulder"*2))/"Lanes" > 11
and ("PaveWid" + ("Shoulder"*2)) - ("Lanes"*11) < 10
)
SELECT *
FROM tblA
WHERE "Speed"<= 25 
AND "aadt" <= 3000;
""", con, geom_col= "geometry")

#s3: if lanes are narrowed, there is enough room for parking, not bike lanes; speed/volume low
s3 = gpd.GeoDataFrame.from_postgis(
"""WITH tblA AS(
select *
from sld_njreg s
where "Juris" = 'County'
and "Lanes" > 0
and "Shoulder" < 5
and ("PaveWid" - ("Shoulder"*2))/"Lanes" > 11
and ("PaveWid" + ("Shoulder"*2)) - ("Lanes"*11) between 17 and 25
)
SELECT *
FROM tblA
WHERE "Speed"<= 25 
AND "aadt" <= 3000;
""", con, geom_col= "geometry")

#add column with category code
bl1['code']='bl1'
bl2['code']='bl2'
bl3['code']='bl3'
bl4['code']='bl4'
s1['code']='s1'
s2['code']='s2'
s3['code']='s3'

#combine dataframes to single table
frames = [bl1, bl2, bl3, bl4, s1, s2, s3]
result = pd.concat(frames)

#write to shapefile
result.to_file("G:/Shared drives/Bike-Friendly Resurfacing/NJ_SLD/SHP/facility_results.shp")
