"""
This module contains code for data retrieval and sampling.
Preprocess of data took place in advance of this file.

"""
from solardataretrieval import utilities
import boto3
from solardatatools.clear_day_detection import filter_for_sparsity
from io import BytesIO
import pandas as pd
import numpy as np
import sys
from sys import path
path.append('..')

def get_summary_file():
    """
    This function gets the summary file (.csv) for sunpower data that was generated by preprossing.
    param: not applicable
    return: summary file as a dataframe
    """
    key, secret_key = utilities.get_credentials()
    s3 = boto3.client('s3', aws_access_key_id=key, aws_secret_access_key=secret_key)
    summary_df = pd.read_csv('s3://pv.insight.sunpower.preprocessed/summary_file.csv', index_col=0)
    return summary_df

class Retrieval():
    """ This class includes three functions for data sampling pipeline. Initially, the data are filtered_indexes
    based on the user filter threshold parameters. The data are randmoly retrieved from the filtered data based on user input of
    number of sites and days per site. The selected data are uploaded in AWS S3 bucket including power output values and metadata.

    Parameters
    ----------
    Data sampling: number_of_sites, number_of_days, quantile_percent
    Filter thresholds: sparsity_th, quality_th, day_samples

    Returns
    ----------
    Random selection of sites and days per site, uploaded data on AWS S3 Bucket.
    """
    def __init__(self, number_of_sites, number_of_days, quantile_percent, sparsity_th, quality_th, day_samples):
        self.number_of_sites = number_of_sites
        self.number_of_days = number_of_days
        self.quantile_percent = quantile_percent
        self.sparsity_th = sparsity_th
        self.quality_th = quality_th
        self.day_samples = day_samples
        self.summary_df = get_summary_file()

    def filter_data(self):
        filter_list = []
        filter_list.append(self.summary_df['overall_spaersity'] < self.sparsity_th)
        filter_list.append(self.summary_df['overall_quality'] > self.quality_th)
        filter_list.append(self.summary_df['time_sample'] == self.day_samples)
        df_filter = pd.DataFrame(data=filter_list).T
        filtered_indexes = np.alltrue(df_filter, axis=1)
        return filtered_indexes

    def data_retrieval(self):
        df_meta_data = pd.DataFrame(columns=['site_ID', 'sensor_ID', 'start_timestamp', 'end_timestamp', 'duration_days', 'time_sample', 'quantile_95', 'overall_spaersity', "overall_quality", "days_selected"])
        summary_file_filtered = self.summary_df[self.filter_data()]
        all_sites = summary_file_filtered.site_ID.unique()
        site_IDs_selected = np.random.choice(all_sites, self.number_of_sites)
        index_to_download = [] #from original summary file!
        sensor_IDs_selected = []
        for site_id in site_IDs_selected:
            site1 = summary_file_filtered.loc[summary_file_filtered['site_ID'] == site_id]
            sensor_IDs = site1.sensor_ID.unique()
            selected_sensor = np.random.choice(sensor_IDs, 1)[0]
            index_to_download.append(summary_file_filtered.loc[(summary_file_filtered['site_ID'] == site_id) & (summary_file_filtered['sensor_ID'] == selected_sensor)].index.tolist()[0])
            sensor_IDs_selected.append(selected_sensor)
        counter = 1
        for data_index in index_to_download:
            key, secret_key = utilities.get_credentials()
            s3 = boto3.client('s3', aws_access_key_id=key, aws_secret_access_key=secret_key)
            response = s3.get_object(Bucket='pv.insight.sunpower.preprocessed', Key='matrices/{0:04d}.npy'.format(data_index))
            body = response['Body'].read()
            power_signals_1 = np.load(BytesIO(body))
            valid_indices = filter_for_sparsity(power_signals_1, solver='MOSEK')
            day_numbers = np.arange(power_signals_1.shape[1])
            good_day_numbers = day_numbers[valid_indices]
            list_of_selected_dates = np.random.choice(good_day_numbers, size=self.number_of_days, replace=False)
            power_signals = (power_signals_1 / np.quantile(power_signals_1, self.quantile_percent))
            power_signals_selected_days = power_signals[:,list_of_selected_dates]
            if data_index == index_to_download[0]:
                power_signals_selected_days_all = power_signals_selected_days
            else:
                power_signals_selected_days = power_signals_selected_days
                power_signals_selected_days_all = np.concatenate([power_signals_selected_days_all, power_signals_selected_days], axis=1)
            df_meta_data = df_meta_data.append(self.summary_df.iloc[data_index,:])
            df_meta_data.loc[data_index, 'days_selected'] = str(list_of_selected_dates)
            total = len(index_to_download)
            utilities.progress(counter, total, status='', bar_length=60)
            counter = counter + 1
        df_data_input = pd.DataFrame(data=power_signals_selected_days_all[:])
        return df_data_input, df_meta_data

    def data_upload(self):
        df_data_input, df_meta_data = self.data_retrieval()
        upload_data = utilities.AWS_upload(df_data_input,self.number_of_sites, self.number_of_days, "data_input")
        upload_quantile_values = utilities.AWS_upload(df_meta_data,self.number_of_sites, self.number_of_days, "metadata")

if __name__ == "__main__":
    main()
