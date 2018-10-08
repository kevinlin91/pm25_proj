import pandas as pd
import pickle
import sqlite3


def load_data(path):
    neighbor_path = path + 'near.pickle'
    data = pickle.load(open(neighbor_path, 'rb'))
    return data

def extract_neighbor(data):
    neighbor = {'airbox_id':[], 'station_id':[] }
    for key in data.keys():
        neighbor['airbox_id'].append(key)
        neighbor['station_id'].append(data[key][0][0])
    df = pd.DataFrame(neighbor)
    return df

def save_to_sqlite(df):
    conn = sqlite3.connect('./db/airbox.db')
    df.to_sql('neighbor', conn, index = False, if_exists = 'replace')
    conn.close()

if __name__ == '__main__':
    data = load_data('./neighbor_data/')
    save_to_sqlite(extract_neighbor(data))
   
    
