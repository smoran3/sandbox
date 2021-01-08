import psycopg2 as psql
import pandas as pd
import os
from tqdm import tqdm

con = psql.connect(
    #daisy's ip address = 10.1.1.190
    host = "10.1.1.190",
    port = 5432,
    database = "septagtf_20191030",
    user = "postgres",
    password = "sergt"
)

#SEPTA GTFS Route Types
#1 = subway
#2 = regional rail
#3 = bus
#0 = trolley/nhsl

#names of routes within UCity Study Area
bus = ['12', '124', '125', '13', '21', '29', '30', '31', '40', '42', '44', '49', '62', '64', '78', '9', 'LUCYGO', 'LUCYGR']
trolley = ['10','11','13','34','36']
regrail = ['AIR','CHE', 'CHW', 'CYN', 'LAN', 'MED', 'FOX', 'NOR', 'PAO', 'TRE', 'WAR', 'WIL', 'WTR']
subway = ['MFL']

##### SQL QUERIES #####
#trips for the above selected route that run on wednesdays; both directions
#total number of records here is in the ballpark of "total trips" value from route statistics
q_numtrips = """
WITH tblA AS(
	SELECT *
	FROM gtfs_routes r
	WHERE r.route_short_name = '{0}'
	)
SELECT t.trip_id
FROM gtfs_trips t
INNER JOIN tblA a
ON t.route_id = a.route_id
INNER JOIN gtfs_calendar c
ON t.service_id = c.service_id
WHERE wednesday = 1;
"""
q_ampeak = """
SELECT *
FROM gtfs_stop_times
WHERE trip_id = '{0}'
AND stop_sequence = 1
AND arrival_time > '06:00:00'
AND arrival_time < '09:00:00'
"""
q_pmpeak = """
SELECT *
FROM gtfs_stop_times
WHERE trip_id = '{0}'
AND stop_sequence = 1
AND arrival_time > '15:00:00'
AND arrival_time < '18:00:00'
"""

def tripcounter(mode, name):
    #result lists
    route = []
    allday_trips = []
    ampeak_trips = []
    pmpeak_trips = []
    for i in xrange(0, len(mode)):
        print mode[i]
        route.append(mode[i])
        cur = con.cursor()
        cur.execute(q_numtrips.format(mode[i]))
        trips = cur.fetchall()
        allday_trips.append(len(trips))

        amtrip_counter = 0
        pmtrip_counter = 0
        for j in tqdm(xrange(0, len(trips))):
            #print trips[j][0]
            cur.execute(q_ampeak.format(trips[j][0]))
            amtrips = cur.fetchall()
            if len(amtrips) > 0:
                amtrip_counter += 1
            

            cur.execute(q_pmpeak.format(trips[j][0]))
            pmtrips = cur.fetchall()
            if len(pmtrips) > 0:
                pmtrip_counter += 1
             
        ampeak_trips.append(amtrip_counter)
        pmpeak_trips.append(pmtrip_counter)

    print ampeak_trips
    print pmpeak_trips

    df = pd.DataFrame(list(zip(route, allday_trips, ampeak_trips, pmpeak_trips)), 
               columns =['route', 'allday', 'ampeak', 'pmpeak']) 

    df.to_csv(r'D:\dvrpc_shared\Sandbox\FY21_UCity\%s.csv' % name, index=False)

tripcounter(trolley, 'trolley_trips')
tripcounter(subway, 'subway_trips')
tripcounter(regrail, 'regrail_trips')
tripcounter(bus, 'bus_trips')