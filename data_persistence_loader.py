from hdfs import InsecureClient
from datetime import datetime
import os
import pandas as pd
from pyarrow import fs
import pyarrow.parquet as pq

hdfs_client = InsecureClient('http://10.4.41.68:9870', user='bdm')

arrow_client = fs.HadoopFileSystem(host='http://10.4.41.68:9870', port=9870, user='bdm')

#####TO DO:
# FIRST: get fs.HadoopFileSystem connection to work. its the pyarrow connection to filesystem
# else, find alternative to that.
#
# SECOND: using pq.write_to_dataset(table, rootpath, partition_cols=['asdf','qwer'], filesystem=arrow_client)
# save as partitioned parquet file over district

def idealista_files_list():
    # create a list of file and sub directories
    # names in the data directory
    list_of_files = hdfs_client.list('landing_temporal/idealista/')
    idealista_files = list()
    # Iterate over all the entries
    for entry in list_of_files:
        fullPath = 'landing_temporal/idealista/' + entry
        if fullPath[-5:] == '.json':
            idealista_files.append(fullPath)

    return idealista_files


def idealista_to_df():
    idealista_files = idealista_files_list()

    df_list = []

    for file in idealista_files:
        with hdfs_client.read(file, encoding='UTF-8') as reader:
            new_df = pd.read_json(reader, orient='records')
            df_list.append(new_df)

    df = pd.concat(df_list, ignore_index=True)

    return df


print(idealista_to_df())



