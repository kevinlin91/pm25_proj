from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import sqlite3
import time

def load_data(path):
    airbox1_path = path + 'airbox.csv'
    airbox2_path = path + 'airbox2.csv'
    df_airbox1 = pd.read_csv(airbox1_path)
    df_airbox1['city'] = np.nan
    df_airbox2 = pd.read_csv(airbox2_path)
    combine_df = pd.concat([df_airbox1, df_airbox2], ignore_index=True)
    combine_df['new_date'] = combine_df.apply(generate_time, axis=1)
    combine_df.sort_values(by = ['new_date'])
    return combine_df

def generate_time(x):
    hour, minute, seconed = x['Time'].split(':')
    next_day = 0
    if int(minute) > 30:
        hour = int(hour)+1
    if hour == 24:
        hour = 0
        next_day = 1
    if next_day == 0:
        new_date = '%s %s:00:00' % (x['Date'], str(hour).zfill(2))
        result = datetime.strptime(new_date, '%Y-%m-%d %H:%M:%S')
    else:
        new_date = '%s %s:00:00' % (x['Date'], str(hour).zfill(2))
        result = datetime.strptime(new_date, '%Y-%m-%d %H:%M:%S')
        result = result + timedelta(days=1)
    return result

def save_to_sqlite(df):
    conn = sqlite3.connect('./db/airbox.db')
    df.to_sql('airbox', conn, index = False, if_exists = 'replace')
    conn.close()
    


if __name__ == '__main__':
    start_time = time.time()
    combine_df = load_data('./airbox_data/')
    save_to_sqlite(combine_df)
    print("--- %s seconds ---" % (time.time() - start_time))    
    

