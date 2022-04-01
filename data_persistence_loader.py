from hdfs import InsecureClient
import pandas as pd
from pyarrow import fs
import pyarrow.parquet as pq
import os
import pyarrow as pa
import pyarrow.json as pj
from datetime import datetime

# Set connections: Pyarrow fs for manipulating files, HdsfCLI for listing files (not a function in Pyarrow fs)
hdfs_cli = InsecureClient('http://10.4.41.68:9870', user='bdm')


hdfs_pa = fs.HadoopFileSystem("hdfs://meowth.fib.upc.es:27000?user=bdm")


def idealista_files_list(file_extension='.jsonl'):
    # create a list of file and sub directories
    # names in the data directory
    list_of_files = hdfs_cli.list('landing_temporal/idealista/')
    idealista_files = []
    ext_len = -len(file_extension)
    # Iterate over all the entries
    for entry in list_of_files:
        full_path = 'landing_temporal/idealista/' + entry
        if full_path[ext_len:] == file_extension:
            idealista_files.append(full_path)
    return idealista_files


# def idealista_to_pa_table():
#     idealista_files = idealista_files_list()
#
#     table_list = []
#     empty_jsons = 0
#
#     h = round(len(idealista_files) / 10)
#     n = 0
#     print(datetime.now(tz=None), '  -  ', 'JSONL readings started', sep='')
#
#     for file in idealista_files:
#         with hdfs_cli.read(file) as reader:
#             try:
#                 partial_table = pj.read_json(reader)
#                 table_list.append(partial_table)
#             except:
#                 empty_jsons += 1
#
#         if n % h == 0:
#             pct = round(n / h * 10)
#             if pct not in (0, 100):
#                 print(datetime.now(tz=None), '  -  ', pct, '% of idealista files processed', sep = '')
#         n += 1
#
#     print(datetime.now(tz=None), '  -  ', 'JSONL readings complete', sep='')
#
#     table = pa.concat_tables(table_list, bool_promote=True)
#     if empty_jsons > 0:
#         print('There were {} empty json files'.format(empty_jsons))
#
#     return table


def idealista_to_df():
    idealista_files = idealista_files_list(file_extension='.json')

    df_list = []

    h = round(len(idealista_files) / 10)
    n = 0
    print(datetime.now(tz=None), '  -  ', 'Pandas JSON readings started', sep='')

    for file in idealista_files:
        with hdfs_cli.read(file, encoding='UTF-8') as reader:
            new_df = pd.read_json(reader, orient='records')
            new_df['sourceFile'] = file
            df_list.append(new_df)

        # if n % h == 0:
        #     pct = round(n / h * 10)
        #     if pct not in (0, 100):
        #         print(datetime.now(tz=None), '  -  ', pct, '% of idealista files processed', sep = '')
        # n += 1

    print(datetime.now(tz=None), '  -  ', 'Pandas JSON readings complete', sep='')

    df = pd.concat(df_list, ignore_index=True)

    return df


def define_idealista_schema():
    fields = [
        pa.field('propertyCode', pa.int32()),
        pa.field('thumbnail', pa.string()),
        pa.field('externalReference', pa.string()),
        pa.field('numPhotos', pa.int16()),
        pa.field('floor', pa.string()),
        pa.field('price', pa.int32()),
        pa.field('propertyType', pa.string()),
        pa.field('operation', pa.string()),
        pa.field('size', pa.float32()),
        pa.field('exterior', pa.bool_()),
        pa.field('rooms', pa.int8()),
        pa.field('bathrooms', pa.int8()),
        pa.field('address', pa.string()),
        pa.field('province', pa.string()),
        pa.field('municipality', pa.string()),
        pa.field('district', pa.string()),
        pa.field('country', pa.string()),
        pa.field('neighborhood', pa.string()),
        pa.field('latitude', pa.float32()),
        pa.field('longitude', pa.float32()),
        pa.field('showAddress', pa.bool_()),
        pa.field('url', pa.string()),
        pa.field('distance', pa.int16()),
        pa.field('hasVideo', pa.bool_()),
        pa.field('status', pa.string()),
        pa.field('newDevelopment', pa.bool_()),
        pa.field('hasLift', pa.bool_()),
        pa.field('priceByArea', pa.float32()),
        pa.field('detailedType', pa.list_(pa.struct([pa.field('typology', pa.string()),
                                                     pa.field('subTypology', pa.string(), nullable=True)]))),
        pa.field('suggestedTexts', pa.list_(pa.struct([pa.field('subtitle', pa.string()),
                                                       pa.field('title', pa.string())]))),
        pa.field('hasPlan', pa.bool_()),
        pa.field('has3DTour', pa.bool_()),
        pa.field('has360', pa.bool_()),
        pa.field('hasStaging', pa.bool_()),
        pa.field('topNewDevelopment', pa.bool_()),
        pa.field('sourceFile', pa.string()),
        pa.field('parkingSpace', pa.list_(pa.struct([pa.field('hasParkingSpace', pa.bool_(), nullable=True),
                                                     pa.field('isParkingSpaceIncludedInPrice', pa.bool_(), nullable=True),
                                                     pa.field('parkingSpacePrice', pa.float32(), nullable=True)]))),
        pa.field('newDevelopmentFinished', pa.bool_())
    ]

    schema = pa.schema(fields)

    return schema


def persist_idealista_as_parquet():
    df = idealista_to_df()
    df["floor"] = df["floor"].astype(str)
    df["hasLift"] = df["hasLift"].astype(bool)

    # print(df.head().to_string())
    table = pa.Table.from_pandas(df, schema=define_idealista_schema())
    print()
    print(table.schema)
    pq.write_table(table, 'landing_persistent/idealista.parquet', filesystem=hdfs_pa, row_group_size=134217728) #128 mb




persist_idealista_as_parquet()
print()
print(pq.read_table('landing_persistent/idealista.parquet', filesystem=hdfs_pa).to_pandas())

#######set of comands that worked going by hand

#
# hdfs_pa = fs.HadoopFileSystem("meowth.fib.upc.es:27000?user=bdm")
# hdfs_pa.copy_file('/user/bdm/landing_temporal/idealista/2020_01_02_idealista.json', '/user/bdm')

# with hdfs_pa.open_input_file('/user/bdm/landing_temporal/idealista/2020_01_02_idealista.json') as reader:
#         df = pd.read_json(reader, orient='records')
# print(df)
# table = pa.Table.from_pandas(df)
# table
# pq.write_to_dataset(table, '/user/bdm/test.parquet', partition_cols=['neighborhood'], filesystem=hdfs_pa)
# table2 = pq.read_table('/user/bdm/test.parquet/', filesystem=hdfs_pa)
# table2





