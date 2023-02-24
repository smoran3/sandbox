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

