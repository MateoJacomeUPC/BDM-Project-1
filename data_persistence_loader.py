from hdfs import InsecureClient
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from pyarrow import fs
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


def batch_idealista_to_df():
    idealista_files = idealista_files_list(file_extension='.json')[:-10] #leave 10 files out to implement fresh loads

    df_list = []

    print(datetime.now(tz=None), '  -  ', 'Pandas JSON readings started', sep='')

    for file in idealista_files:
        with hdfs_cli.read(file, encoding='UTF-8') as reader:
            new_df = pd.read_json(reader, orient='records')
            new_df['sourceFile'] = file[17:]  # remove landing_temporal/ from path, it should always come from there
            df_list.append(new_df)

    print(datetime.now(tz=None), '  -  ', 'Pandas JSON readings complete', sep='')

    df = pd.concat(df_list, ignore_index=True)
    df['load_time'] = datetime.now()

    return df, idealista_files


def initial_idealista_schema():
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
        pa.field('detailedType', pa.struct([pa.field('typology', pa.string()),
                                            pa.field('subTypology', pa.string(), nullable=True)])),
        pa.field('suggestedTexts', pa.struct([pa.field('subtitle', pa.string()),
                                              pa.field('title', pa.string())])),
        pa.field('hasPlan', pa.bool_()),
        pa.field('has3DTour', pa.bool_()),
        pa.field('has360', pa.bool_()),
        pa.field('hasStaging', pa.bool_()),
        pa.field('topNewDevelopment', pa.bool_()),
        pa.field('parkingSpace', pa.struct([pa.field('hasParkingSpace', pa.bool_(), nullable=True),
                                            pa.field('isParkingSpaceIncludedInPrice', pa.bool_(), nullable=True),
                                            pa.field('parkingSpacePrice', pa.float32(), nullable=True)])),
        pa.field('sourceFile', pa.string()),
        pa.field('load_time', pa.timestamp(unit='ns'))
    ]

    schema = pa.schema(fields)

    return schema


def persist_batch_idealista_as_parquet(delete_temporal_files=False):
    # read the combined dataframe and list of combined files
    df, list_of_files = batch_idealista_to_df()

    # convert datatypes to avoid pyarrow problems
    df["floor"] = df["floor"].astype(str)
    df["hasLift"] = df["hasLift"].astype(bool)

    #convert dataframe to pyarrow table, sort by neighborhood and write to persistent landing zone
    table = pa.Table.from_pandas(df, schema=initial_idealista_schema())
    indices = pc.sort_indices(table, sort_keys=[("neighborhood", "ascending")])
    table = pc.take(table, indices)
    pq.write_table(table, 'landing_persistent/idealista.parquet', filesystem=hdfs_pa,
                   row_group_size=134217728)  # 128 mb

    #write a log with the files loaded in the batch process
    fields = [pa.field('file', pa.string()), pa.field('load_time', pa.timestamp('ns'))]
    arrays = [pa.array(list_of_files), pa.array([datetime.now(tz=None)] * len(list_of_files))]
    log = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
    pq.write_table(log, 'pipeline_metadata/LOG_batch_load_temporal_to_persistent.parquet', filesystem=hdfs_pa,
                   row_group_size=134217728)

    #delete json files from landing
    if delete_temporal_files:
        for entry in list_of_files:
            hdfs_cli.delete(entry)


def read_parquet(hdfs_path):
    table = pq.read_table(hdfs_path, filesystem=hdfs_pa)
    return table


def fresh_idealista_to_df():
    # Read fresh idealista files into a df
    # TO DO: should check the log file to get the ones not already loaded
    log = read_parquet('pipeline_metadata/LOG_batch_load_temporal_to_persistent.parquet')

    idealista_files = idealista_files_list(file_extension='.json')[-10:]
    df_list = []
    for file in idealista_files:
        with hdfs_cli.read(file, encoding='UTF-8') as reader:
            new_df = pd.read_json(reader, orient='records')
            new_df['sourceFile'] = file[17:]  # remove landing_temporal/ from path, it should always come from there
            df_list.append(new_df)

    df = pd.concat(df_list, ignore_index=True)
    df['load_time'] = datetime.now()

    return df, idealista_files


def persist_fresh_idealista_as_parquet(delete_temporal_files=False):
    # read fresh df and list of fresh files, also turn df turn to pyarrow table.
    fresh_df, list_of_files = fresh_idealista_to_df()

    # convert datatypes to avoid pyarrow problems
    fresh_df["floor"] = fresh_df["floor"].astype(str)
    fresh_df["hasLift"] = fresh_df["hasLift"].astype(bool)

    # read persisted table
    old_table = read_parquet('landing_persistent/idealista.parquet')

    # compare schemas. read schema from pandas to avoid pyarrow table conversion without defined schema
    fresh_schema, old_schema = pa.Table.from_pandas(fresh_df).schema, old_table.schema
    diff_fields = set(fresh_schema) - set(old_schema)
    diff_field_names = set(fresh_schema.names) - set(old_schema.names)

    if len(diff_field_names) == 0:
        # convert dataframe to pyarrow table. Combine with old table. Write as parquet to hdfs.
        fresh_table = pa.Table.from_pandas(fresh_df, schema=initial_idealista_schema())
        full_table = pa.concat_tables([old_table, fresh_table])

    else:
        # adapt old schema to new one with automatic conversion. notice the user so action can be taken later.
        for field in diff_fields:
            if field.name not in old_schema.names:
                print('\n\nSome new columns have been added to the Parquet file. Updating datatypes may be interesting.'
                      '\n\n')
                old_table = old_table.append_column(field.name, pa.nulls(old_table.num_rows, type=field.type))

        # convert fresh dataframe to pyarrow table following the adapted schema
        fresh_table = pa.Table.from_pandas(fresh_df, schema=old_table.schema)

        full_table = pa.concat_tables([old_table, fresh_table])

    # sort and write table to parquet
    indices = pc.sort_indices(full_table, sort_keys=[("neighborhood", "ascending")])
    full_table = pc.take(full_table, indices)
    pq.write_table(full_table, 'landing_persistent/idealista.parquet', filesystem=hdfs_pa,
                   row_group_size=134217728)  # 128 mb

    # write a log with the files loaded in the batch process
    fields = [pa.field('file', pa.string()), pa.field('load_time', pa.timestamp('ns'))]
    arrays = [pa.array(list_of_files), pa.array([datetime.now(tz=None)] * len(list_of_files))]
    log = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
    pq.write_table(log, 'pipeline_metadata/LOG_fresh_load_temporal_to_persistent.parquet', filesystem=hdfs_pa,
                   row_group_size=134217728)

    # delete json files from landing
    if delete_temporal_files:
        for entry in list_of_files:
            hdfs_cli.delete(entry)


def clean_directory_of_filetype(dir, file_extension):
    # This function is only for cleaning the dir of some files created during testing.
    list_of_files = hdfs_cli.list(dir)
    # Iterate over all the entries in the dir
    for entry in list_of_files:
        # Create full path
        fullPath = dir + '/' + entry
        # Deletes file if it matches the file extension
        ext_len = -len(file_extension)
        if fullPath[ext_len:] == file_extension:
            hdfs_cli.delete(fullPath)


clean_directory_of_filetype('landing_persistent/', '.parquet')

persist_batch_idealista_as_parquet()

print(pq.read_table('landing_persistent/idealista.parquet', filesystem=hdfs_pa).to_pandas())
print()

persist_fresh_idealista_as_parquet()

print(pq.read_table('landing_persistent/idealista.parquet', filesystem=hdfs_pa).to_pandas())
print()

print(pq.read_table('pipeline_metadata/LOG_batch_load_temporal_to_persistent.parquet', filesystem=hdfs_pa).to_pandas())





