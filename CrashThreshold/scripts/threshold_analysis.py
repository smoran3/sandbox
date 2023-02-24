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

#### UNIVERSE = QUALIFYING ROADS IN EACH COUNTY, broken into volume bins #####

#calculate the percent of total crashes in each category for all segments
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

    def flag_segments(volname):
        means = {}
        sds = {}
        crashtypes = ['vu', 'fatal', 'serious', 'rear', 'angle', 'left']
        means[county] = {}
        sds[county] = {}
        for crashtype in crashtypes:
            fieldname = f'perc_{crashtype}'
            means[county][crashtype] = volbin_dict[volname][fieldname].mean()
            sds[county][crashtype] = volbin_dict[volname][fieldname].std()
            mean = means[county][crashtype]
            sd  = sds[county][crashtype]
            oneval = mean+sd
            twoval = mean+sd+sd
            #above average
            df.loc[df[fieldname] >  mean, fr'c_av_{crashtype}'] = 'True'
            df.loc[df[fieldname] <= mean, fr'c_av_{crashtype}'] = 'False'
            #above one SD
            df.loc[df[fieldname] >  oneval, fr'c_osd_{crashtype}'] = 'True'
            df.loc[df[fieldname] <= oneval, fr'c_osd_{crashtype}'] = 'False'
            #above two SD
            df.loc[df[fieldname] >  twoval, fr'c_tsd_{crashtype}'] = 'True'
            df.loc[df[fieldname] <= twoval, fr'c_tsd_{crashtype}'] = 'False'
        
        return df
    
    v1 = flag_segments('volbin1')
    v2 = flag_segments('volbin2')
    v3 = flag_segments('volbin3')

    #combine vol bins for county
    v_list = [v1, v2, v3]
    df_calculated = gpd.GeoDataFrame(pd.concat(v_list, ignore_index = True))

    return df_calculated

#run function for each county
Bucks_v_df = calc_county_averages('Bucks')
Chester_v_df = calc_county_averages('Chester')
Delaware_v_df = calc_county_averages('Delaware')
Montgomery_v_df = calc_county_averages('Montgomery')
Philadelphia_v_df = calc_county_averages('Philadelphia')


#combine county subsets back into single dataframe
CountyFramesList = [Bucks_v_df, Chester_v_df, Delaware_v_df, Montgomery_v_df, Philadelphia_v_df]
df_allcounties = gpd.GeoDataFrame(pd.concat(CountyFramesList, ignore_index = True))

#output
df_allcounties.to_postgis('county_thresholds', con = ENGINE, if_exists= 'replace')
df_allcounties.to_file(fr"{ev.DATA_ROOT}/county_thresholds.shp") 

