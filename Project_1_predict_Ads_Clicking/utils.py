import pandas as pd 
import numpy as np 
from datetime import datetime, timezone
import sys 
import pytz
from tqdm import tqdm

from zipfile import ZipFile
import pickle
def from_utc_to_local_time_US(df_2):
    USA_tz = pytz.country_timezones('US')
    
    vec_state = ['Alabama(AL)', 'Alaska(AK)', 'Arizona(AZ)','American Samoa',
                 'Arkansas(AR)', 'California(CA)', 'Colorado(CO)', 'Connecticut(CT)',
                 'Delaware(DE)', 'District of Columbia(DC)', 'Florida(FL)', 'Georgia(GA)',
                 'Guam (GU)', 'Hawaii(HI)', 'Idaho(ID)', 'Illinois(IL)', 'Indiana(IN)', 'Iowa(IA)',
                 'Kansas(KS)', 'Kentucky(KY)', 'Louisiana(LA)', 'Maine(ME)', 'Maryland(MD)',
                 'Massachusetts(MA)', 'Michigan(MI)', 'Minnesota(MN)', 'Mississippi(MS)',
                 'Missouri(MO)', 'Montana(MT)', 'Nebraska(NE)', 'Nevada(NV)',
                 'New Hampshire(NH)', 'New Jersey(NJ)', 'New Mexico(NM)', 'New York(NY)',
                 'North Carolina(NC)', 'North Dakota(ND)', 'Northern Mariana Islands(MP)',
                 'Ohio(OH)', 'Oklahoma(OK)', 'Oregon(OR)', 'Pennsylvania(PA)',
                 'Puerto Rico (PR)', 'Rhode Island(RI)', 'South Carolina(SC)',
                 'South Dakota(SD)', 'Tennessee(TN)', 'Texas(TX)', 'United States Virgin Islands(USVI)',
                 'Utah(UT)', 'Vermont(VT)', 'Virginia(VA)', 'Washington(WA)',
                 'West Virginia(WV)', 'Wisconsin(WI)', 'Wyoming(WY)']
    vec_tz = ['Central Standard Time (CST)', 'Alaska Standard Time (AKST)', 'Mountain Standard Time (MST)',
              'Samoa Time (SST)',  'Central Standard Time (CST)', 'Pacific Standard Time (PST)',
              'Mountain Standard Time (MST)', 'Eastern Standard Time (EST)', 'Eastern Standard Time (EST)',
              'Eastern Standard Time (EST)', 'Most of the state: Eastern Standard Time (EST)',
              'Eastern Standard Time (EST)', 'Chamorro Standard Time (ChST)', 'Hawaii-Aleutian Standard Time (HST)',
              'Most of the state: Mountain Standard Time (MST)', 'North of theSalmon River: Pacific Standard Time (PST)',
              'Central Standard Time (CST)', 'Most of the state: Eastern Standard Time (EST)',
              'Central Standard Time (CST)', 'Most of the state: Central Standard Time (CST)',
              'Western half of the state: Central Standard Time (CST)'
              'Central Standard Time (CST)', 'Eastern Standard Time (EST)', 'Eastern Standard Time (EST)',
              'Eastern Standard Time (EST)', 'Most of the state: Eastern Standard Time (EST)',
              'Central Standard Time (CST)', 'Central Standard Time (CST)', 'Central Standard Time (CST)',
              'Mountain Standard Time (MST)', 'Most of the state: Central Standard Time (CST)',
              'Most of the state: Pacific Standard Time (PST)', 'Eastern Standard Time (EST)',
              'Eastern Standard Time (EST)','Mountain Standard Time (MST)','Eastern Standard Time (EST)',
              'Eastern Standard Time (EST)','Most of the state: Central Standard Time (CST)',
              'Chamorro Standard Time (ChST)',
              'Eastern Standard Time (EST)','Central Standard Time (CST)',
              'Most of the state: Pacific Standard Time (PST)',
              'Eastern Standard Time (EST)','Atlantic Standard Time (AST)',
              'Eastern Standard Time (EST)',
              'Eastern Standard Time (EST)',
              'Western South Dakota: Mountain Standard Time (MST)',
              'Middle Tennessee, plus Marion County: Central Standard Time (CST)',
              'Most of the state: Central Standard Time (CST)',
              'Atlantic Standard Time (AST)','Mountain Standard Time (MST)',
              'Eastern Standard Time (EST)','Eastern Standard Time (EST)','Pacific Standard Time (PST)',
              'Eastern Standard Time (EST)','Central Standard Time (CST)','Mountain Standard Time (MST)']
    
    state_tz_df = pd.DataFrame(data = {'state': vec_state, 'TimeZone': vec_tz})
    state_tz_df.head()
    
    #--new dataframe for converting 2-letter-state to timezone:
    ST_list = []
    TZ_list = []
    for st, tz in zip(state_tz_df.state,state_tz_df.TimeZone):
        start1 = st.find('(') 
        end1 = st.find(')')
        if not(start1 == -1) and not(end1 == -1):
            ST_list.append(st[start1+1:end1])
            s1 = tz.find('(') 
            e1 = tz.find(')')
            TZ_list.append(tz[s1+1:e1])
    
    state_tz_df_short = pd.DataFrame({'state':ST_list, 'TimeZone':TZ_list})
    
    df_tz_list = []
    for st in df_2['user_state']:
        df_tz_list.append(state_tz_df_short['TimeZone'][state_tz_df_short.state == st].values[0])
    
    df_2['timezone'] = pd.DataFrame(df_tz_list)
    
    pytz_zones = []
    for t in df_2['timezone']:
        if t == 'CST':
            pytz_zones.append('US/Central')
        elif t == 'EST':
            pytz_zones.append('US/Eastern')
        elif t == 'MST':
            pytz_zones.append('US/Mountain')
        elif t == 'PST':
            pytz_zones.append('US/Pacific')
        elif t == 'HST':
            pytz_zones.append('US/Hawaii')
        elif t == 'AKST':
            pytz_zones.append('US/Alaska')
            
        else:
            pass
            # print("Hallo", t)
    df_2['pytz_zones'] = pd.DataFrame(pytz_zones)
    
    
    fmt = '%Y-%m-%d %H:%M:%S %Z%z'
    
    local_time = []
    year = []
    month = []
    day = []
    weekday = []
    hour = []
    counter = 0
    n = len(df_2['utc_time'])
    for utc1, local1 in zip(df_2['utc_time'], df_2['pytz_zones']):
        # print(utc1, local1)
        if counter % 100000 ==0:
            print(counter, "/", n)
        
        counter += 1
        e = pytz.timezone(local1)
        time_from_utc = datetime.fromtimestamp(utc1/1e3, tz=timezone.utc)
        time_from = time_from_utc.astimezone(e)
        local_time.append(time_from.strftime(fmt))
        year.append(time_from.year)
        month.append(time_from.month)
        day.append(time_from.day)
        weekday.append(time_from.weekday())
        hour.append(time_from.hour)
        
        
    # df_2['local_time'] = pd.DataFrame(local_time)
    df_2['year'] = pd.DataFrame(year)
    df_2['month'] = pd.DataFrame(month)
    df_2['day'] = pd.DataFrame(day)
    df_2['weekday'] = pd.DataFrame(weekday)
    df_2['hour'] = pd.DataFrame(hour)
    df_2.drop([ 'pytz_zones', 'utc_time'], axis=1, inplace = True)
    #'timezone',
    return df_2

def app_features_extraction(df_3):
    app_features =  ['category', 'score', 'reviews','price','free', 'size']

    # app_details = ZipFile("data/play_apps.zip")
    # app_file ='play_apps/a008.com.fc2.blog.androidkaihatu.datecamera2'
    # app = pickle.loads(app_details.read(app_file))

    app_category = []
    app_score = []
    app_reviews = []
    app_price = []
    app_free = []
    app_size = []
    app_info = []
    count = 0
    for i in df_3['app_id']:
        # 
        
        count += 1
        app_path = 'play_apps/' + i
        
        try:
            app_details = ZipFile("data/play_apps.zip")
            app = pickle.loads(app_details.read(app_path))
            
            app_category.append(app_helper(app, app_features[0])[0])
            app_score.append(app_helper(app, app_features[1]))
            app_reviews.append(app_helper(app, app_features[2]))
            app_price.append(app_helper(app, app_features[3]))
            app_free.append(app_helper(app, app_features[4]))
            app_size.append(app_helper(app, app_features[5]))
            app_info.append(1)
        except:
            try:
                app_details = ZipFile("data/play_apps_2.zip")
                app = pickle.loads(app_details.read(app_path))
            
                app_category.append(app_helper(app, app_features[0])[0])
                app_score.append(app_helper(app, app_features[1]))
                app_reviews.append(app_helper(app, app_features[2]))
                app_price.append(app_helper(app, app_features[3]))
                app_free.append(app_helper(app, app_features[4]))
                app_size.append(app_helper(app, app_features[5]))
                print("yalla")
                app_info.append(1)
            except:
                # print(count)
                # print(app_path)
                # print()
                app_category.append(None)
                app_score.append(None)
                app_reviews.append(None)
                app_price.append(None)
                app_free.append(None)
                app_size.append(None)
                app_info.append(0)
    
    
    df_3['app_category'] = app_category 
    df_3['app_score '] = app_score 
    df_3['app_reviews'] = app_reviews 
    df_3['app_price'] = app_price 
    df_3['app_free'] = app_free 
    df_3['app_size'] = app_size 
    df_3['app_info'] = app_info
    
    df_3.drop(['app_id'], axis = 1, inplace = True)
    return df_3


def app_helper(app, category):
    try:
        d = app[category]
        return d 
    
    except:
        return None
 
    
def app_specification(df):
    df['app_category'] = None 
    df['app_score'] =  None 
    df['app_reviews'] =  None 
    df['app_price'] = None 
    df['app_free'] = None 
    df['app_size'] =  None 
    df['app_info'] = None 
    app_features =  ['category', 'score', 'reviews','price','free', 'size']
    all_unique_apps = df.app_id.unique()
    n = len(df)
    count = 0
    for id_app in all_unique_apps:
        
       boolean_condition = df.app_id == id_app
       
       num_samples = sum(boolean_condition)
        
       app_path = 'play_apps/' + id_app
        
       try:
            app_details = ZipFile("data/play_apps.zip")
            app = pickle.loads(app_details.read(app_path))
            
            app_category = app_helper(app, app_features[0])[0]
            app_score = app_helper(app, app_features[1])
            app_reviews = app_helper(app, app_features[2])
            app_price = app_helper(app, app_features[3])
            app_free = app_helper(app, app_features[4])
            app_size = app_helper(app, app_features[5])
            app_info = 1
       except:
            try:
                app_details = ZipFile("data/play_apps_2.zip")
                app = pickle.loads(app_details.read(app_path))
                
                app_category = app_helper(app, app_features[0])[0]
                app_score = app_helper(app, app_features[1])
                app_reviews = app_helper(app, app_features[2])
                app_price = app_helper(app, app_features[3])
                app_free = app_helper(app, app_features[4])
                app_size = app_helper(app, app_features[5])
                app_info = 1
            except:
                # print(count,"/",n)
                # print(app_path)
                # app_category.append(None)
                # app_score.append(None)
                # app_reviews.append(None)
                # app_price.append(None)
                # app_free = None
                # app_size = None
                # app_info.append(0)
                count += num_samples
                continue
                
       df.loc[ boolean_condition, 'app_category'] =  app_category
       df.loc[boolean_condition, 'app_score '] =  app_score 
       df.loc[boolean_condition,'app_reviews'] =  app_reviews
       df.loc[boolean_condition,'app_price'] = app_price
       df.loc[boolean_condition,'app_free'] = app_free 
       df.loc[boolean_condition,'app_size'] =  app_size 
       df.loc[boolean_condition,'app_info'] = app_info
       count += num_samples
       print(count,"/",n)
       
    return df

    
def from_app_id_to_features(df):
    app_details = ZipFile("data/play_apps.zip")
    apps = []
    for app_id in tqdm(df['app_id'].unique()):
        app_file = f'play_apps/{app_id}'
        try:
            app = pickle.loads(app_details.read(app_file)) 
            apps.append(app)
        except Exception as e:
            pass
    df2 = pd.DataFrame(apps)
    
    app_features =  ['category', 'score', 'reviews','price', 'size','app_id', 'bids']
    
    for j in df2.columns:
        if j not in app_features:
            df2.drop([j], axis = 1, inplace = True)
            # print("hallo")
            
    df = df.merge(df2, on='app_id', how='left')
    return df
    
    