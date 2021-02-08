import psycopg2 as psql

#use postgis shapefile importer to import shapefiles into DB

#connection to DB
    con = psql.connect(
        #daisy's ip address = 10.1.1.190
        host = "10.1.1.190",
        port = 5432,
        database = "rethinkcon",
        user = "postgres",
        password = "sergt"
    )


#which bi-directional links have different LTS (same link no)
#which LTS are they and are we ok with just cutting them if they are LTS 4
WITH tblA AS(
SELECT no, lts, geometry, COUNT(*) AS cnt
FROM camco_ltsp3_links_muni
GROUP BY no, lts
ORDER by cnt DESC
)
SELECT *
FROM tblA
WHERE cnt = 1

#if this anlaysis is to include LTS 4, need to find another way to cut highways/limited access roads
#perhaps typeno?