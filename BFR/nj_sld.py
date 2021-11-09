import psycopg2 as psql
import pandas as pd
import geopandas as gpd
import os

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
bl1 = pd.read_sql("""
WITH tblA AS(
    select *
    from njreg_sld ns 
    where juris = 'County'
    and lanes > 0
    and shoulder < 5
    and (pavewid - (shoulder*2))/lanes > 11
    and (pavewid + (shoulder*2)) - (lanes*11) between 10 and 16
)
SELECT *
FROM tblA
WHERE speed<= 40 
AND aadt <= 20000;

""", con)

#bl2: if lanes are narrowed, there is room for bike lanes and parking
bl2 = pd.read_sql("""
WITH tblA AS(
select *
from njreg_sld ns 
where juris = 'County'
and lanes > 0
and shoulder < 5
and (pavewid - (shoulder*2))/lanes > 11
and (pavewid + (shoulder*2)) - (lanes*11) > 26
)
SELECT *
FROM tblA
WHERE speed<= 40 
AND aadt <= 20000
;

""", con)

#bl3: if lanes are 11', threre is room for bike lanes, but not parking
bl3 = pd.read_sql("""
WITH tblA AS(
select *
from njreg_sld ns 
where juris = 'County'
and shoulder >= 5
and (pavewid + (shoulder*2)) - (lanes*11) between 10 and 16
)
SELECT *
FROM tblA
WHERE speed<= 40 
AND aadt <= 20000
;

""", con)

#bl4: if lanes are 11', there is room for bike lanes and parking
bl4 = pd.read_sql(
"""WITH tblA AS(
select *
from njreg_sld ns 
where juris = 'County'
and shoulder >= 5
and (pavewid + (shoulder*2)) - (lanes*11) > 26
)
SELECT *
FROM tblA
WHERE speed<= 40 
AND aadt <= 20000;
""", con)

#sharrows
#s1: lanes cannot be narrowed and shoulders are not wide enough for bike lanes; speeds/volume low
s1 = pd.read_sql(
"""WITH tblA AS(
select *
from njreg_sld ns 
where juris = 'County'
and lanes > 0
and shoulder < 5
and (pavewid - (shoulder*2))/lanes <= 11
)
SELECT *
FROM tblA
WHERE speed<= 25 
AND aadt <= 3000;
""", con)

#s2: even if lanes are narrowed, there is not enough spare room for bike lanes; speed/volume low
s2 = pd.read_sql(
"""WITH tblA AS(
select *
from njreg_sld ns 
where juris = 'County'
and lanes > 0
and shoulder < 5
and (pavewid - (shoulder*2))/lanes > 11
and (pavewid + (shoulder*2)) - (lanes*11) < 10
)
SELECT *
FROM tblA
WHERE speed<= 25 
AND aadt <= 3000;
""", con)

#s3: if lanes are narrowed, there is enough room for parking, not bike lanes; speed/volume low
s3 = pd.read_sql(
"""WITH tblA AS(
select *
from njreg_sld ns 
where juris = 'County'
and lanes > 0
and shoulder < 5
and (pavewid - (shoulder*2))/lanes > 11
and (pavewid + (shoulder*2)) - (lanes*11) between 17 and 25
)
SELECT *
FROM tblA
WHERE speed<= 25 
AND aadt <= 3000;
""", con)

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

#result['geom'] = gpd.GeoSeries.from_wkt(df['geom'])


#convert to geodataframe
result['geometry'] = gpd.GeoSeries.from_wkb(result['geom'])
g = gpd.GeoDataFrame(result, geometry='geometry')

#write to postgeres


#write to shapefile
#path = "G:/Shared drives/Bike-Friendly Resurfacing/NJ_SLD/SHP/"
g.to_file("G:/Shared drives/Bike-Friendly Resurfacing/NJ_SLD/SHP/facility_results.shp")