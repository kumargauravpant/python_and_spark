import sys
import glob
import os
import pysftp

folder_list = sys.argv[1]
base_path = '/externaldata01/dev/sams_club/'
sftp_path = '/upload'


for folder in folder_list.split(','):
    print(f'current folder is {folder}')
    #sftp_path = os.path.join('/upload' , folder)
    print(f'Uploading file in sftp: {sftp_path}')
    
    os.chdir(os.path.join(base_path, folder))
    files = glob.glob('*.gz')
    with pysftp.Connection('mft.retailsolutions.com',username='IRI_Sams', password='5YW82jss') as sftp:
        sftp.timeout = 3600
        #sftp.mkdir(sftp_path)
        with sftp.cd(sftp_path):
            for file in files:
                print(f'uploading file {file}')
                sftp.put(file)

    

