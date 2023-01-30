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

#calculate the percent of total crashes in each category for all district 6 segments
df['perc_vu']      = ((df['ped_count']+df['bike_count'])/df['total_cras'])
df['perc_fatal']   = (df['fatal_coun']/df['total_cras'])
df['perc_serious'] = (df['serious_in']/df['total_cras'])
df['perc_rear']    = (df['rear_end']/df['total_cras'])
df['perc_angle']   = (df['angle']/df['total_cras'])
df['perc_left']    = (df['left_turns']/df['total_cras'])


##### UNIVERSE = ALL QUALIFYING DISTRICT 6 ROADS3#####
#calculate the mean and standard deviation for each percentage field for all district 6 segments
vu_mean, fatal_mean, serious_mean, rear_mean, angle_mean, left_mean = df[['perc_vu', 'perc_fatal', 'perc_serious', 'perc_rear', 'perc_angle', 'perc_left']].mean()
vu_sd, fatal_sd, serious_sd, rear_sd, angle_sd, left_sd = df[['perc_vu', 'perc_fatal', 'perc_serious', 'perc_rear', 'perc_angle', 'perc_left']].std()

#create dictionaries to iterate over in function
types = ['vu', 'fatal', 'serious', 'rear', 'angle', 'left']
means = [vu_mean, fatal_mean, serious_mean, rear_mean, angle_mean, left_mean]
sds   = [vu_sd, fatal_sd, serious_sd, rear_sd, angle_sd, left_sd]

meandict = dict(zip(types, means))
sd_dict  = dict(zip(types, sds))

#flag segments by value relative to the mean and SD
# in column names, d = district, c = county, av = above average, 
# osd = above one standard deviation, tsd = above two standard deviations

def flag_segments(type, universe):
      col  = fr'perc_{type}'
      mean = meandict[type]
      sd  = sd_dict[type]
      oneval = mean+sd
      twoval = mean+sd+sd
      #above average
      df.loc[df[col] >  mean, fr'{universe}_av_{type}'] = 'True'
      df.loc[df[col] <= mean, fr'{universe}_av_{type}'] = 'False'
      #above one SD
      df.loc[df[col] >  oneval, fr'{universe}_osd_{type}'] = 'True'
      df.loc[df[col] <= oneval, fr'{universe}_osd_{type}'] = 'False'
      #above two SD
      df.loc[df[col] >  twoval, fr'{universe}_tsd_{type}'] = 'True'
      df.loc[df[col] <= twoval, fr'{universe}_tsd_{type}'] = 'False'

for key in meandict:
    flag_segments(key, 'd')

#output



##### UNIVERSE = QUALIFYING ROADS IN EACH COUNTY #####
#re-grab segments to start with a fresh table
cdf = pd.read_sql_query(Q_grab, ENGINE)

#calculate the percent of total crashes in each category for all district 6 segments
df['perc_vu']      = ((df['ped_count']+df['bike_count'])/df['total_cras'])
df['perc_fatal']   = (df['fatal_coun']/df['total_cras'])
df['perc_serious'] = (df['serious_in']/df['total_cras'])
df['perc_rear']    = (df['rear_end']/df['total_cras'])
df['perc_angle']   = (df['angle']/df['total_cras'])
df['perc_left']    = (df['left_turns']/df['total_cras'])


#calculate the mean and standard deviation for each percentage field for each county
c_vu_mean      = df.groupby('co_name')['perc_vu'].mean()
c_fatal_mean   = df.groupby('co_name')['perc_fatal'].mean()
c_serious_mean = df.groupby('co_name')['perc_serious'].mean()
c_rear_mean    = df.groupby('co_name')['perc_rear'].mean()
c_angle_mean   = df.groupby('co_name')['perc_angle'].mean()
c_left_mean    = df.groupby('co_name')['perc_left'].mean()

c_vu_sd      = df.groupby('co_name')['perc_vu'].std()
c_fatal_sd   = df.groupby('co_name')['perc_fatal'].std()
c_serious_sd = df.groupby('co_name')['perc_serious'].std()
c_rear_sd    = df.groupby('co_name')['perc_rear'].std()
c_angle_sd   = df.groupby('co_name')['perc_angle'].std()
c_left_sd    = df.groupby('co_name')['perc_left'].std()

#create dictionaries to iterate over in function
types = ['vu', 'fatal', 'serious', 'rear', 'angle', 'left']
c_means = [c_vu_mean, c_fatal_mean, c_serious_mean, c_rear_mean, c_angle_mean, c_left_mean]
c_sds   = [c_vu_sd, c_fatal_sd, c_serious_sd, c_rear_sd, c_angle_sd, c_left_sd]

c_meandict = dict(zip(types, c_means))
c_sd_dict  = dict(zip(types, c_sds))

#function to flag segments based on county averages
def county_flag_segments(type, universe):
      col  = fr'perc_{type}'
      mean = c_meandict[type][universe]
      sd  = c_sd_dict[type][universe]
      oneval = mean+sd
      twoval = mean+sd+sd
      #above average
      df.loc[df[col] >  mean, fr'{universe}_av_{type}'] = 'True'
      df.loc[df[col] <= mean, fr'{universe}_av_{type}'] = 'False'
      #above one SD
      df.loc[df[col] >  oneval, fr'{universe}_osd_{type}'] = 'True'
      df.loc[df[col] <= oneval, fr'{universe}_osd_{type}'] = 'False'
      #above two SD
      df.loc[df[col] >  twoval, fr'{universe}_tsd_{type}'] = 'True'
      df.loc[df[col] <= twoval, fr'{universe}_tsd_{type}'] = 'False'

#list of counties to iterate over
counties = ['Bucks', 'Chester', 'Delaware', 'Montgomery', 'Philadelphia']
for county in counties:
    for key in c_meandict:
        county_flag_segments(key, county)