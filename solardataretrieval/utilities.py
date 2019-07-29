from sys import path
path.append('..')
from solardatatools import make_time_series, standardize_time_axis, make_2d, plot_2d
from cassandra.cluster import Cluster
import pandas as pd
import time
import numpy as np
from solardatatools import *
from solardatatools.utilities import local_median_regression_with_seasonal, basic_outlier_filter
from solardatatools.clear_day_detection import filter_for_sparsity
from os.path import expanduser
home = expanduser('~')
with open(home + '/.aws/credentials') as f:
        lns = f.readlines()
        key = lns[3].split(' = ')[1].strip('\n')
        secret = lns[4].split(' = ')[1].strip('\n')

import s3fs

#this function returns all IDs of solar installations that exist in SunPower Cassandra database
#the path of cassandra_cluster should be a string, example '/Users/elpiniki/.aws/cassandra_cluster'
#the output is a list of IDs as strings
def site_ID_reader(cassandra_cluster_path):
    with open(cassandra_cluster_path) as f:
        cluster_ip = f.readline().strip('\n')
        cluster = Cluster([cluster_ip])
        session = cluster.connect('measurements')
        cql = """
            select distinct site
            from measurement_raw
            """
        rows = session.execute(cql)
        df_siteIDs = pd.DataFrame(list(rows), )
        site_IDs = df_siteIDs.site.tolist()
    return(site_IDs)

#this function gets data from Cassandra based on the random selected IDs
def get_data_from_Cassandra(sites_selected, cassandra_cluster_path):
    s = ""
    for j in range(0,len(sites_selected)):
        if j < len(sites_selected)-1:
            s += "'%s', " %str(sites_selected[j])
        else:
            s += "'%s'" %str(sites_selected[len(sites_selected)-1])

    start_time = time.time()
    sites_to_get = s
    #connecting to Cassandra
    print ("getting data from Cassandra - please wait")
    with open(cassandra_cluster_path) as f:
        cluster_ip = f.readline().strip('\n')
        cluster = Cluster([cluster_ip])
        session = cluster.connect('measurements')
        cql = """
            select site, meas_name, ts, sensor, meas_val_f
            from measurement_raw
            where site in ({})
            and meas_name = 'ac_power';
            """.format(sites_to_get)
        rows = session.execute(cql)
        df = pd.DataFrame(list(rows), )
    print("selection completed")
    print("---waiting time: %s seconds---" % (time.time() - start_time))
    return (df)

#this function preprocess the data and normlize them based on a quantile
#this function retuns two datafiles: power signals and a list of qualtile value per site
def data_pipeline_process(data_input, site_IDs, sensor_IDs,quantile_percent,number_of_days, sorted_days):
    power_signals_selected_days_all = []
    site_errors = 0
    quantile_values = []
    sensor_IDs_output = []
    side_IDs_output = []
    for i in range(0,len(sensor_IDs)):
        try:
            df_sensor = data_input.loc[data_input['sensor'] == sensor_IDs[i]]
            df_sensor.replace(-999999.0, np.NaN, inplace=True)
            df_ts, info = make_time_series(df_sensor, return_keys=True, localize_time=-8, timestamp_key='ts',
                        value_key='meas_val_f', name_key='meas_name',
                        groupby_keys=['site', 'sensor'])
            #print(df_ts)
            key_string = info[0][1]
            power_signals_1 = make_2d(standardize_time_axis(df_ts),
                    key=key_string,
                    zero_nighttime=True,
                    interp_missing=True)
        #power_signals_1 is an np.array: do calculations on that
        #check zweros and nans to find valid dates in data
            valid_indices = filter_for_sparsity(power_signals_1, solver='MOSEK')
            day_numbers = np.arange(power_signals_1.shape[1])
            good_day_numbers = day_numbers[valid_indices]
        #list_of_days = list(range(0,len(power_signals[0]),1))
            list_of_selected_dates = np.random.choice(good_day_numbers, size=number_of_days, replace=False)
            power_signals = (power_signals_1 / np.quantile(power_signals_1, quantile_percent))
            quantile_values.append(np.quantile(power_signals_1, quantile_percent))
            sensor_IDs_output.append(sensor_IDs[i])
            side_IDs_output.append(site_IDs[i])
            if sorted_days is True:
                list_of_selected_dates.sort()
            if np.quantile(power_signals_1, quantile_percent) > 0.0 or np.quantile(power_signals_1, quantile_percent) != np.nan:
                power_signals_selected_days = power_signals[:,list_of_selected_dates]
                if sensor_IDs[i] == sensor_IDs[0]:
                    power_signals_selected_days_all = power_signals_selected_days
                else:
                    power_signals_selected_days = power_signals_selected_days
                    try:
                        power_signals_selected_days_all = np.concatenate([power_signals_selected_days_all, power_signals_selected_days], axis=1)
                    except:
                        site_errors = site_errors+1
                        quantile_values.remove(quantile_values[len(quantile_values)-1])
                        sensor_IDs_output.remove(sensor_IDs_output[len(sensor_IDs_output)-1])
                        side_IDs_output.remove(side_IDs_output[len(side_IDs_output)-1])
                        pass
            else:
                pass
        except:
            pass
    return power_signals_selected_days_all, quantile_values, site_errors, sensor_IDs_output, side_IDs_output


def AWS_upload(data_to_upload_df,number_of_sites, site_errors, number_of_days, kind):
    #upload to aws
    bytes_to_write = data_to_upload_df.to_csv(None).encode()
    fs = s3fs.S3FileSystem(key=key, secret=secret)
    #path needs to change!
    file_path = 's3://pv.insight.misc/data_samples/s{}_d{}_{}.csv'.format(number_of_sites,number_of_days, kind)
    with fs.open(file_path, 'wb') as f:
        f.write(bytes_to_write)
