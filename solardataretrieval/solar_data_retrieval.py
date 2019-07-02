from sys import path
path.append('..')
import numpy as np
import pandas as pd
from solardataretrieval import utilities
from solardatatools import make_time_series, standardize_time_axis, make_2d, plot_2d

def retrieval(cassandra_cluster_path, number_of_sites, number_of_days, quantile_percent, sorted_days):
    #run the functions to generate list of random selected IDs
    all_sites = utilities.site_ID_reader(cassandra_cluster_path)
    site_IDs_selected = np.random.choice(all_sites, number_of_sites)
    #run the function to get random selected IDs from database
    #the output is pandas dataframe
    sites_data_selected = utilities.get_data_from_Cassandra(site_IDs_selected, cassandra_cluster_path)
    #select sensors per site randomly (note that sites can have multiple inverters/sensors)
    sensor_IDs_selected = []
    for site_id in site_IDs_selected:
        site1 = sites_data_selected.loc[sites_data_selected['site'] == site_id]
        sensor_IDs = site1.sensor.unique()
        #print("sensor_IDs per site")
        #print(sensor_IDs)
        selected_sensor = sensor_IDs[0]
        #selected_sensor = np.random.choice(sensor_IDs, 1)[0]
        sensor_IDs_selected.append(selected_sensor)
    #print("sensor_IDs_all selected")
    #print (sensor_IDs_selected)
        #df_sensor = site1.loc[site1['sensor'] == np.random.choice(sensor_IDs, 1)[0]]
        #df_sensor.replace(-999999.0, np.NaN, inplace=True)
        #df_ts, info = make_time_series(df_sensor, return_keys=True, localize_time=-8, timestamp_key='ts',
        #                value_key='meas_val_f', name_key='meas_name',
        #                groupby_keys=['site', 'sensor'])
        #try:
        #    key_string = info[0][1]
        #except IndexError:
        #    print("YES")
        #    new_site_selection = np.random.choice(all_sites, 1)[0]
        #    site_IDs_selected = np.append(new_site_selection)
    data_pipeline_output, quantile_values_list, site_errors, sensors, sites = utilities.data_pipeline_process(sites_data_selected, site_IDs_selected, sensor_IDs_selected, quantile_percent,number_of_days, sorted_days)

    return(data_pipeline_output, quantile_values_list, site_errors, sensors, sites)
