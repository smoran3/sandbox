import geopandas as gpd
import pandas as pd
from sqlalchemy_utils import database_exists, create_database
import env_vars as ev
from env_vars import ENGINE
import geoalchemy2
pd.options.mode.chained_assignment = None  # default='warn'

#read in segments joined to county boundaries from postgres and create dataframe
Q_grab = """
    select *
    FROM crash_segments
    """
df = gpd.read_postgis(Q_grab, ENGINE, geom_col='geometry', crs=4326)

#calculate the percent of total crashes in each category for all district 6 segments
df['perc_vu']      = ((df['PED_COUNT']+df['BIKE_COUNT'])/df['TOTAL_CRAS'])
df['perc_fatal']   = (df['FATAL_COUN']/df['TOTAL_CRAS'])
df['perc_serious'] = (df['SERIOUS_IN']/df['TOTAL_CRAS'])
df['perc_rear']    = (df['REAR_END']/df['TOTAL_CRAS'])
df['perc_angle']   = (df['ANGLE']/df['TOTAL_CRAS'])
df['perc_left']    = (df['LEFT_TURNS']/df['TOTAL_CRAS'])

#subset by volume threshold to shrink universe of segments used to calculate averages; apples to apples comparison
def volume_subset(lower, upper, df):
    dfsub = df[(df['VPHPD'] > lower) & (df['VPHPD'] <= upper)]
    return  dfsub

volbin1 = volume_subset(0, 850, df)
volbin2 = volume_subset(850, 1250, df)
volbin3 = volume_subset(1250, 1500, df)

def calc_averages(df):
    #calculate the mean and standard deviation for each percentage field for all district 6 segments within volume bin
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
    
    return df

volbin1_result = calc_averages(volbin1)
volbin2_result = calc_averages(volbin2)
volbin3_result = calc_averages(volbin3)

#combine volume subsets back into single dataframe
dataframesList = [volbin1_result, volbin2_result, volbin3_result]
df_combine = gpd.GeoDataFrame(pd.concat(dataframesList, ignore_index = True))

#output
df_combine.to_postgis('district_thresholds_by_vol', con = ENGINE, if_exists= 'replace')
df_combine.to_file(fr"{ev.DATA_ROOT}/district_thresholds.shp") 








#####################################################################################

##### UNIVERSE = QUALIFYING ROADS IN EACH COUNTY, broken into volume bins #####
#re-grab segments to start with a fresh table
df = gpd.read_postgis(Q_grab, ENGINE, geom_col='geometry', crs=4326)

#calculate the percent of total crashes in each category for all district 6 segments
df['perc_vu']      = ((df['PED_COUNT']+df['BIKE_COUNT'])/df['TOTAL_CRAS'])
df['perc_fatal']   = (df['FATAL_COUN']/df['TOTAL_CRAS'])
df['perc_serious'] = (df['SERIOUS_IN']/df['TOTAL_CRAS'])
df['perc_rear']    = (df['REAR_END']/df['TOTAL_CRAS'])
df['perc_angle']   = (df['ANGLE']/df['TOTAL_CRAS'])
df['perc_left']    = (df['LEFT_TURNS']/df['TOTAL_CRAS'])


#create county subsets
def create_county_subsets(county):
    dfsub = df[df['CTY_CODE'] == county]
    return  dfsub


Bucks_df        = create_county_subsets('09')
Chester_df      = create_county_subsets('15')
Delaware_df     = create_county_subsets('23')
Montgomery_df   = create_county_subsets('46')
Philadelphia_df = create_county_subsets('67')

counties = ['Bucks', 'Chester', 'Delaware', 'Montgomery', 'Philadelphia']
df_list = [Bucks_df, Chester_df, Delaware_df, Montgomery_df, Philadelphia_df]

county_dict = dict(zip(counties, df_list))

def volume_subset(lower, upper, df):
    dfsub = df[(df['VPHPD'] > lower) & (df['VPHPD'] <= upper)]
    return  dfsub


def calc_county_averages(county):
    df = county_dict[county]
    volbin1 = volume_subset(0, 850, df)
    volbin2 = volume_subset(850, 1250, df)
    volbin3 = volume_subset(1250, 1500, df)
    volbin_list = [volbin1, volbin2, volbin3]
    volbin_names = ['volbin1', 'volbin2', 'volbin3']

    volbin_dict = dict(zip(volbin_names, volbin_list))

    def flag_segments(name):
        means = {}
        sds = {}
        crashtypes = ['vu', 'fatal', 'serious', 'rear', 'angle', 'left']
        means[county] = {}
        sds[county] = {}
        for crashtype in crashtypes:
            fieldname = f'perc_{crashtype}'
            means[county][crashtype] = volbin_dict[name][fieldname].mean()
            sds[county][crashtype] = volbin_dict[name][fieldname].std()
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
        
        return df_calculated
    
    #combine vol bins for county
    ###NEED TO EDIT THESE
    dataframesList = [Bucks_df, Chester_df, Delaware_df, Montgomery_df, Philadelphia_df]
    df_calculated = gpd.GeoDataFrame(pd.concat(dataframesList, ignore_index = True))

    
    return df_calculated

#run function for each county
counties = ['Bucks', 'Chester', 'Delaware', 'Montgomery', 'Philadelphia']
for county in counties:
    calc_county_averages(county)


#combine county subsets back into single dataframe
dataframesList = [Bucks_df, Chester_df, Delaware_df, Montgomery_df, Philadelphia_df]
df_combine = gpd.GeoDataFrame(pd.concat(dataframesList, ignore_index = True))

#output
df_combine.to_postgis('county_thresholds', con = ENGINE, if_exists= 'replace')
df_combine.to_file(fr"{ev.DATA_ROOT}/county_thresholds.shp") 

