from datetime import datetime
import pandas as pd
import numpy as np
import sqlite3

def load_data(path):
    cwb1_path = path + '2017-01_06.csv'
    cwb2_path = path + '2017-07_12.csv'
    cwb3_path = path + '2018-01_06.csv'
    column = ['ObsTime','StnPres','SeaPres','Temperature','Td dew point','RH','WS','WD','WSGust','WDGust','Precp','PrecpHour','SunShine','GloblRad','Visb','st_id','st_name','date']
    df_cwb1 = pd.read_csv(cwb1_path)
    df_cwb1.reset_index(inplace=True)
    df_cwb2 = pd.read_csv(cwb2_path)
    df_cwb2.reset_index(inplace=True)
    df_cwb3 = pd.read_csv(cwb3_path)
    df_cwb3.reset_index(inplace=True)
    df_cwb1.columns = column
    df_cwb2.columns = column
    df_cwb3.columns = column
    combine_df = pd.concat([df_cwb1, df_cwb2, df_cwb3], ignore_index=True)
    combine_df['new_date'] = combine_df.apply(generate_time, axis=1)
    combine_df.sort_values(by = ['new_date'])
    return combine_df

def load_data2(path):
    column = ['ObsTime','StnPres','SeaPres','Temperature','Td dew point','RH','WS','WD','WSGust','WDGust','Precp','PrecpHour','SunShine','GloblRad','Visb','st_id','st_name','date']
    cwb_path = path + '1_month.csv'
    df_cwb = pd.read_csv(cwb_path)
    df_cwb.reset_index(inplace=True)
    df_cwb.columns = column
    df_cwb['new_date'] = df_cwb.apply(generate_time, axis=1)
    df_cwb['new_wd'] = df_cwb.apply(get_direct, axis = 1)
    df_cwb.sort_values(by = ['new_date'])
    return df_cwb

def get_direct(x):
    degree = x['WD']
    try:
        degree = int(degree)
    except:
        return 0

    scale = 365 / 8
    init = scale / 2

    if degree > (365 - init) or degree <= init:
        return 1
    elif degree > init and degree <= init + scale:
        return 2
    elif degree > init + scale and degree <= init +  2 * scale:
        return 3
    elif degree > init +  2 * scale and degree <= init +  3 * scale:
        return 4
    elif degree > init +  3 * scale and degree <= init +  4 * scale:
        return 5
    elif degree > init +  4 * scale and degree <= init +  5 * scale:
        return 6
    elif degree > init +  5 * scale and degree <= init +  6 * scale:
        return 7
    elif degree > init +  6 * scale and degree <= init +  7 * scale:
        return 8
    else:
        return 0


def generate_time(x):
    new_date = '%s %s:00:00' % (x['date'], str(int(x['ObsTime'])-1).zfill(2) )
    return datetime.strptime(new_date, '%Y-%m-%d %H:%M:%S')

def save_to_sqlite(df):
    conn = sqlite3.connect('./db/airbox.db')
    df.to_sql('cwb', conn, index = False, if_exists = 'replace')
    conn.close()


if __name__ == '__main__':

    #combine_df = load_data('./cwb_data/')
    combine_df = load_data2('./one_month_data/cwb_data/')
    save_to_sqlite(combine_df)
    
