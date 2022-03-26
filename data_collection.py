from hdfs import InsecureClient
from datetime import datetime
import os


### CLIENT COMMANDS
# Retrieving a file or folder content summary.
#       content = client.content('dat')

# Listing all files inside a directory.
#       fnames = client.list('dat')

# Retrieving a file or folder status.
#       status = client.status('dat/features')

# Renaming ("moving") a file.
#       client.rename('dat/features', 'features')

# Deleting a file or folder.
#       client.delete('dat', recursive=True)
###

client = InsecureClient('http://10.4.41.68:9870', user='bdm')


### Upload files

def files_list(dir_name):
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
            all_files = all_files + files_list(fullPath)
        else:
            all_files.append(fullPath)

    return all_files


def load_to_landing_temporal():
    all_files = files_list('Data')
    h = round(len(all_files) / 10)
    n = 0

    print(datetime.now(tz=None), '  -  ', 'Loading started', sep = '')

    for out_file in all_files:
        in_file = 'landing_temporal/' + out_file[5:]
        with open(out_file, encoding='UTF-8') as reader, client.write(in_file, encoding='UTF-8') as writer:
            for line in reader:
                writer.write(line)

        if n % h == 0:
            pct = round(n / h * 10)
            if pct not in (0, 100):
                print(datetime.now(tz=None), '  -  ', pct, '% done', sep = '')

        n += 1

    print(datetime.now(tz=None), '  -  ', 'Loading complete', sep = '')


load_to_landing_temporal()

