import build_airbox_table 
import build_cwb_table 
import build_neighbor_table 
import time


def build_db():

    print ('start to load neightbor data')
    neighbor = build_neighbor_table.load_data('./neighbor_data/')
    build_neighbor_table.save_to_sqlite(build_neighbor_table.extract_neighbor(neighbor))
    print ('neightbor data done')

    print ('start to load cwb data')
    #cwb_df = build_cwb_table.load_data2('./one_month_data/cwb_data/')
    cwb_df = build_cwb_table.load_data('./cwb_data/')
    build_cwb_table.save_to_sqlite(cwb_df)
    print ('cwb data done')

    print ('start to load airbox data')
    #airbox_df = build_airbox_table.load_data('./one_month_data/airbox_data/')
    airbox_df = build_airbox_table.load_data('./airbox_data/')
    build_airbox_table.save_to_sqlite(airbox_df)
    print ('arbox data done')


if __name__ == '__main__':
    start_time = time.time()
    build_db()
    print("--- %s seconds ---" % (time.time() - start_time))
