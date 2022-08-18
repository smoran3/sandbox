# find the number of zones you can get to via transit from each zone in a certain amount of time
# and the number of zones oyu can get to via car from each zone
# updated 8/18/22 for Brett LRP eval

import numpy
import csv
import os
from VisumTools import helpers as h
import win32com.client



def runYear(year, yearFiles):
    '''
    #look for version files in the run folder
    # change rundir based on year of model run
    versionFilePath = fr"M:/Conformity/2022/Completed Model Runs/{yearFiles}"
                
    #open version files
    Visum = win32com.client.Dispatch("Visum.Visum.180")

    #load the version file
    Visum.LoadVersion(versionFilePath)
    '''
    #Pull zone numbers from Version file
    Zone_Number = h.GetMulti(Visum.Net.Zones,"No")
    #convert the zone numbers to integers (they come out of visum as float)
    for i in xrange(0,len(Zone_Number)):
        Zone_Number[i] = int(Zone_Number[i])
    #pull travel times from matrix
    TransitJourneyTime = numpy.array(h.GetSkimMatrix(Visum, 490))
    VehicleTravelTime = numpy.array(h.GetSkimMatrix(Visum, 290))
    
    #read csv with essiential service counts; read in s strings, so convert to integer to do things with them
    with open(r'D:/dvrpc_shared/Sandbox/ModelAccessTime/ES_ByTAZ.csv','rb') as IO:
        r = csv.reader(IO)
        header = r.next() #read header row and move on to next before starting for loop
        #create array to hold integers from csv
        ES_byTAZ = []
        for row in r:
            Integers = []
            for x in row:
                Integers.append(int(x))
            ES_byTAZ.append(Integers)

    #create nested dictionary
    Zone = {}
    #create dictionaries within the zone dictionary
    for i in xrange(0, len(ES_byTAZ)):
    #Each column within the csv is its own dictionary; the keys are the field titles, the values are the values in the columns from the csv called out by the index number (which starts at 0)
        Zone[ES_byTAZ[i][0]] = {
            "Jobs":             ES_byTAZ[i][2],
            "ParkTrail":        ES_byTAZ[i][3],
            "ActivityCenter":   ES_byTAZ[i][4],
            "Grocery":          ES_byTAZ[i][5],
            "HealthFac":        ES_byTAZ[i][6],
            "SchoolU":          ES_byTAZ[i][7]
        }


    def tabluate(modeMatrix, modeName):
        #there are some zones in the model that are not included in the csv  - all outside of the region
        #create a dictionary to store these zones to make sure we aren't missing anything important
        NoDataZones = {}
            
        #create file and open for writing as the for loop iterates
        with open(fr'D:/dvrpc_shared/Sandbox/ModelAccessTime/{modeName}_AM_{year}.csv','wb') as IO:
            w = csv.writer(IO)
            #write the header row
            w.writerow(['Zone_Number','Jobs','ParkTrail','ActivityCenter','Grocery','HealthFac','SchoolU','Count'])    
            #for each origin zone
            for i in xrange(0,len(modeMatrix)):
                #i is origin zones, j is destination zones
                #OZone is the zone number in position i
                OZone = Zone_Number[i]
                #create blank dictionary
                OZoneDict = {}
                #create counter starting at 0
                CountZones = 0
                #for each destination zone from that origin zone (i)
                for j in xrange(0, len(modeMatrix[i])):
                    #if weighted journey time at inteserction of Origin Zone [i] and Destination zone [j] in the AVG_JRT_NoWeight array is less than 45 minues
                    if modeMatrix[i][j]<45:
                        # and if the zone is in the csv with essential service attributes
                        if Zone.has_key(Zone_Number[j]):
                            #add that destination zone number as a key in OZone Dict
                            #the values for that key are the values in the zone dictionary counting the essential services for that destination zone
                            OZoneDict[Zone_Number[j]] = Zone[Zone_Number[j]]
                        #OZoneDict has the zone numbers of the zones that can be accessed in 45 mins (Dzones) as the key and the attributes from the Zone Dictionary as the value
                        else: 
                            #if the key (destination zones) is not in the csv, add it and its corresponding origin zone to the NoDataZones csv for tracking purposes
                            #.has_key is a dictionary function to see if a certain key is contained in the csv
                            #NoDataZones in a dictionary and the values are arrays
                            #the keys are the OZone numbers and the values are arrays of DZone numbers within 45 minutes of the OZone that are not in the csv
                            if not NoDataZones.has_key(Zone_Number[i]):
                                NoDataZones[OZone] = []
                            NoDataZones[OZone].append(Zone_Number[j])
                        #every time the if statement evaluates to true, count the zone (even if there is no data in the CSV for that zone because it's external); the resulting countZones variable will be the count of zones accessible within 45 minues via transit
                        CountZones += 1
                        
                    
                #create placeholder dictionary that starts at 0 and can be added to
                Service = {
                    "Jobs":              0,
                    "ParkTrail":         0,
                    "ActivityCenter":    0,
                    "Grocery":           0,
                    "HealthFac":         0,
                    "SchoolU":           0
                }
                #for each DZone and its corresponding attributes from the Zone dictionary, add the values (additivly) for those attributes to the place holder dictionary (Service)
                #this happens for every DZone accessible from a certain OZone in OZoneDict
                for key,value in OZoneDict.items():
                    Service["Jobs"]             += value["Jobs"]
                    Service["ParkTrail"]        += value["ParkTrail"]
                    Service["ActivityCenter"]   += value["ActivityCenter"]
                    Service["Grocery"]          += value["Grocery"]   
                    Service["HealthFac"]        += value["HealthFac"]   
                    Service["SchoolU"]          += value["SchoolU"]
                #wrtie the resulting totals before processing the next OZone
                w.writerow([OZone,
                    Service["Jobs"]            ,
                    Service["ParkTrail"]       ,
                    Service["ActivityCenter"]  ,
                    Service["Grocery"]         ,
                    Service["HealthFac"]       ,
                    Service["SchoolU"],
                    CountZones])
                    #this file saves and closes automatically when the for loop is finished (reaches the end of xrange - len(matrix))
                    
        #write out NoDataZones to csv for reference
        #the resulting csv has 2 columns, 1 is the origin zone, 2 is the destination zone within 45 minutes with no data. origin zones are repeated for however many destination zones have no data.
        with open(fr'D:/dvrpc_shared/Sandbox/ModelAccessTime/{modeName}_NoDataZones_AM_{year}.csv','wb') as IO:
            w = csv.writer(IO)    
            w.writerow(['OZone','DZone'])
            for OZone in NoDataZones:
                for DZone in NoDataZones[OZone]:
                    w.writerow([OZone, DZone])

        

    tabulate(TransitJourneyTime, "Transit")
    tabulate(VehicleTravelTime, "Vehicle")

    
baseYear = '2017_Updated_5_4/2017_AM.ver'
futureYear = '2050/2050_AM.ver'

runYear("2017", baseYear)
runYear("2050", futureYear)