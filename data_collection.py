from hdfs import InsecureClient
from datetime import datetime
import json
import os
import posixpath as psp

hdfs_cli = InsecureClient('http://10.4.41.68:9870', user='bdm')

### IMPORTANT NOTE: In one of our laptops (Windows 10) running the script a first time yields an error regarding
### UTF-8 encoding. Re-running it solves it for some reason we can't really undestand.

### Upload files
def local_files_list(dir_name):
    # create a list of file and sub directories
    # names in the data directory
    list_of_files = os.listdir(dir_name)
    all_files = list()
    # Iterate over all the entries
    for entry in list_of_files:
        # Create full path
        fullPath = dir_name + '/' + entry
        # If entry is a directory then get the list of files in this directory
        if os.path.isdir(fullPath):
            all_files = all_files + local_files_list(fullPath)
        else:
            all_files.append(fullPath)

    return all_files


def hdfs_files_list(dir_name):
    fpaths = [psp.join(dpath, fname)
              for dpath, _, fnames in hdfs_cli.walk(dir_name)
              for fname in fnames]
    return fpaths


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


def load_to_landing_temporal():
    all_local_files = local_files_list('Data')
    all_hdfs_files = None
    try:
        all_hdfs_files = hdfs_files_list('landing_temporal')
    except:
        pass

    # check for new files
    hdfs_filenames = [i.split('/')[-1].split('.')[0] for i in all_hdfs_files] if all_hdfs_files is not None else []
    files_to_process = [i for i in all_local_files if i.split('/')[-1].split('.')[0] not in hdfs_filenames]

    if len(files_to_process) == 0:
        print('No new files to process.')
        return

    # track process timing
    h = round(len(files_to_process) / 10)
    n = 0
    print(datetime.now(tz=None), '  -  ', 'Loading started', sep = '')

    # load files not in the temporal landing zone
    for out_file in files_to_process:
        in_file = 'landing_temporal/' + out_file[5:]

        with open(out_file, encoding='UTF-8') as reader, hdfs_cli.write(in_file, encoding='UTF-8') as writer:
            for line in reader:
                writer.write(line)

        if n % h == 0:
            pct = round(n / h * 10)
            if pct not in (0, 100):
                print(datetime.now(tz=None), '  -  ', pct, '% of files processed', sep = '')

        n += 1

    print(datetime.now(tz=None), '  -  ', 'Loading complete', sep = '')


load_to_landing_temporal()
