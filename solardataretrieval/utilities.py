from sys import path
path.append('..')
from solardatatools import make_time_series, standardize_time_axis, make_2d, plot_2d
from cassandra.cluster import Cluster
import pandas as pd
import time
import numpy as np
import sys
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

def progress(count, total, status='', bar_length=60):
    """
    Python command line progress bar in less than 10 lines of code. · GitHub
    https://gist.github.com/vladignatyev/06860ec2040cb497f0f3
    :param count: the current count, int
    :param total: to total count, int
    :param status: a message to display
    :return:
    """
    bar_len = bar_length
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
    sys.stdout.flush()

def AWS_upload(data_to_upload_df,number_of_sites, number_of_days, kind):
    #upload to aws
    bytes_to_write = data_to_upload_df.to_csv(None).encode()
    fs = s3fs.S3FileSystem(key=key, secret=secret)
    file_path = 's3://pv.insight.misc/data_samples/s{}_d{}_{}.csv'.format(number_of_sites,number_of_days, kind)
    with fs.open(file_path, 'wb') as f:
        f.write(bytes_to_write)
