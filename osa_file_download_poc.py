import os
import sys
import subprocess
import datetime
import logging
import argparse

logging.basicConfig(level=logging.INFO)

azCopy = '/home/prgvk/azcopy/azcopy_linux_amd64_10.10.0/azcopy'

azureSasToken = '?sv=2019-12-12&st=2021-05-07T09%3A25%3A14Z&se=2022-05-08T09%3A25%3A00Z&sr=c&sp=rl&sig=p%2FD6alNjEN0rblUXPh3wqv5iz%2F7MSroA5%2BFLT9M7lPU%3D'

blobContainerName = 'osa-extracts'

azureAccountName = 'sbyccpriridatafeedsa'



localDir = '/externaldata01/dev/sobeys/osa/all'

regions=['atlantic','ontario','west','quebec']
trDate = '2021-04-17'
day = 14
for r in range(day):
    for region in regions:
        azureDir = f'/osa/data/release_output/daily/all_item/{region}/CALENDAR_DT%3D{trDate}/'
        
        print(azureDir)

        azureCopyCommand = f'{azCopy} copy "https://{azureAccountName}.blob.core.windows.net/{blobContainerName}{azureDir}{azureSasToken}" "{localDir}" --overwrite=ifSourceNewer --check-md5 FailIfDifferent --from-to=BlobLocal --recursive;'

        logging.info('Preparing Download.')
        
        os.system(azureCopyCommand)
        
        logging.info('DownloadDone.')
    
    trDate = (datetime.datetime.strptime(trDate, '%Y-%m-%d') - datetime.timedelta(1)).strftime('%Y-%m-%d')
    
    

for dir in os.listdir(localDir):
    currentDir = f'{localDir}/{dir}'
    newDir = f'{localDir}/{dir.lower()}'
    print(currentDir)
    print(newDir)
    os.rename(currentDir, newDir)
    
parser = argparse.ArgumentParser(description='''Utility to download and load SERVICE_LEVEL feed from Azure.''')
parser.add_argument('-config', '--config_file', help='Input config file')
parser.add_argument('-day_', '--meta', help='Meta data file')
parser.add_argument('-b', '--binary-file', help='Name of the output binary file')
opts = parser.parse_args()
        
        
meta_file = opts.meta
ascii_file = opts.ascii_file
op_bin_file = opts.binary_file
meta_cols_string = ''
headers = []
data_types = []
num_of_records = 0

# print '-' * 70
# print ""
# print "Meta Data file: " + meta_file
# print "Text extract file: " + ascii_file
# print "Binary extract file: " + op_bin_file
# print ""
# print '-' * 70
