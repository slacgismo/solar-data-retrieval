import sys
from sys import path
path.append('..')
from os.path import expanduser
home = expanduser('~')
import s3fs
import pandas as pd
import numpy as np

def get_credentials():
    """
    This function gets credentials for service client connection with AWS.
    param: not applicable
    return: access key and secret access key
    """
    with open(home + '/.aws/credentials') as f:
        lns = f.readlines()
    my_dict = {l.split(' = ')[0]: l.split(' = ')[1][:-1] for l in lns if len(l.split(' = ')) == 2 }
    return my_dict['aws_access_key_id'], my_dict['aws_secret_access_key']

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
    """
    This function uploads data on AWS S3 Bucket.
    param: data_to_upload_df,number_of_sites, number_of_days, kind
    return: not applicable
    """
    #upload to aws
    key, secret_key = get_credentials()
    bytes_to_write = data_to_upload_df.to_csv(None).encode()
    fs = s3fs.S3FileSystem(key=key, secret=secret_key)
    file_path = 's3://pv.insight.misc/data_samples/s{}_d{}_{}.csv'.format(number_of_sites,number_of_days, kind)
    with fs.open(file_path, 'wb') as f:
        f.write(bytes_to_write)
