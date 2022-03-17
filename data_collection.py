from hdfs import InsecureClient

client = InsecureClient('http://10.4.41.72:9870', user='bdm')

################# Testing stuff
#Writing part of a file.
with open('Data/opendatabcn-income/2017_Distribució_territorial_renda_familiar.csv', encoding='UTF-8') as reader, \
        client.write('foldertest/test_2018_renda.csv', encoding= 'UTF-8') as writer:
    for line in reader:
        writer.write(line)

with client.read('foldertest/test_2018_renda.csv', encoding = 'UTF-8') as reader:
    print(reader.read())
#################

### Upload files

