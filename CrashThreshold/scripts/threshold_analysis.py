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
df.to_sql('district_thresholds', ENGINE, if_exists= 'replace')



##### UNIVERSE = QUALIFYING ROADS IN EACH COUNTY #####
#re-grab segments to start with a fresh table
df = pd.read_sql_query(Q_grab, ENGINE)

#calculate the percent of total crashes in each category for all district 6 segments
df['perc_vu']      = ((df['ped_count']+df['bike_count'])/df['total_cras'])
df['perc_fatal']   = (df['fatal_coun']/df['total_cras'])
df['perc_serious'] = (df['serious_in']/df['total_cras'])
df['perc_rear']    = (df['rear_end']/df['total_cras'])
df['perc_angle']   = (df['angle']/df['total_cras'])
df['perc_left']    = (df['left_turns']/df['total_cras'])


#create county subsets
def create_county_subsets(county):
    dfsub = df[df['co_name'] == county]
    return  dfsub


Bucks_df        = create_county_subsets('Bucks')
Chester_df      = create_county_subsets('Chester')
Delaware_df     = create_county_subsets('Delaware')
Montgomery_df   = create_county_subsets('Montgomery')
Philadelphia_df = create_county_subsets('Philadelphia')

counties = ['Bucks', 'Chester', 'Delaware', 'Montgomery', 'Philadelphia']
df_list = [Bucks_df, Chester_df, Delaware_df, Montgomery_df, Philadelphia_df]

county_dict = dict(zip(counties, df_list))


#calculate the mean and standard deviation for each percentage field for each county
counties = ['Bucks', 'Chester', 'Delaware', 'Montgomery', 'Philadelphia']
means = {}
sds = {}
crashtypes = ['vu', 'fatal', 'serious', 'rear', 'angle', 'left']
for county in counties: 
    means[county] = {}
    sds[county] = {}
    for crashtype in crashtypes:
        fieldname = f'perc_{crashtype}'
        means[county][crashtype] = county_dict[county][fieldname].mean()
        sds[county][crashtype] = county_dict[county][fieldname].std()


#function to flag segments based on county averages
def county_flag_segments(type, county):
      df = county_dict[county]
      col  = fr'perc_{type}'
      mean = means[county][type]
      sd  = sds[county][type]
      oneval = mean+sd
      twoval = mean+sd+sd
      #above average
      df.loc[df[col] >  mean, fr'c_av_{type}'] = 'True'
      df.loc[df[col] <= mean, fr'c_av_{type}'] = 'False'
      #above one SD
      df.loc[df[col] >  oneval, fr'c_osd_{type}'] = 'True'
      df.loc[df[col] <= oneval, fr'c_osd_{type}'] = 'False'
      #above two SD
      df.loc[df[col] >  twoval, fr'c_tsd_{type}'] = 'True'
      df.loc[df[col] <= twoval, fr'c_tsd_{type}'] = 'False'

#list of counties to iterate over
counties = ['Bucks', 'Chester', 'Delaware', 'Montgomery', 'Philadelphia']
for county in counties:
    for key in means[county]:
        county_flag_segments(key, county)


#combine county subsets back into single dataframe
df_combine = pd.concat(Bucks_df, Chester_df, Delaware_df, Montgomery_df, Philadelphia_df)
#convert to geodataframe
#gdf = gpd.GeodataFrame(df_combine, crs = "EPSG:26918", geometry = tshape)
#output
df.to_sql('county_thresholds', ENGINE, if_exists= 'replace')

