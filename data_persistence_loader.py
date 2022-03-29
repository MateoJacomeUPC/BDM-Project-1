from hdfs import InsecureClient
from datetime import datetime
import pandas as pd
from pyarrow import fs
import pyarrow.parquet as pq
import os


hdfs_cli = InsecureClient('http://10.4.41.68:9870', user='bdm')

hdfs_pa = fs.HadoopFileSystem("meowth.fib.upc.es:27000?user=bdm")

#####TO DO:
#
# SECOND: using pq.write_to_dataset(table, rootpath, partition_cols=['asdf','qwer'], filesystem=arrow_client)
# save as partitioned parquet file over district

print(hdfs_pa.get_file_info(fs.FileSelector("landing_temporal/idealista/", recursive=True)))


def idealista_files_list():
    # create a list of file and sub directories
    # names in the data directory
    list_of_files = hdfs_cli.list('landing_temporal/idealista/')
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
        with hdfs_cli.read(file, encoding='UTF-8') as reader:
            new_df = pd.read_json(reader, orient='records')
            df_list.append(new_df)

    df = pd.concat(df_list, ignore_index=True)

    return df


print(idealista_to_df())


#######set of comands that worked going by hand
# import pandas as pd
# import pyarrow as pa
# import pyarrow.parquet as pq
# from pyarrow import fs
#
# hdfs_pa = fs.HadoopFileSystem("meowth.fib.upc.es:27000?user=bdm")
# hdfs_pa.copy_file('/user/bdm/landing_temporal/idealista/2020_01_02_idealista.json', '/user/bdm')
# with hdfs_pa.open('/user/bdm/landing_temporal/idealista/2020_01_02_idealista.json') as reader:
#         df = pd.read_json(reader, orient='records')
# with hdfs_pa.open_input_file('/user/bdm/landing_temporal/idealista/2020_01_02_idealista.json') as reader:
#         df = pd.read_json(reader, orient='records')
# df
# table = pa.Table.from_pandas(df)
# table
# pq.write_to_dataset(table, '/user/bdm/test.parquet', partition_cols=['neighborhood'], filesystem=hdfs_pa)
# table2 = pq.read_table('/user/bdm/test.parquet/', filesystem=hdfs_pa)
# table2
