import psycopg2 as psql
import csv

#connect to DB
#connection to DB
    con = psql.connect(
        #daisy's ip address = 10.1.1.190
        host = "10.1.1.190",
        port = 5432,
        database = "BikeStress_p3",
        user = "postgres",
        password = "sergt"
    )