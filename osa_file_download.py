import configparser
import subprocess
import datetime
import os
import sys
import logging
import gc 
import pandas as pd
from azure_directory_client import DirectoryClient

logging.basicConfig(level=logging.INFO)

class ParquetFileConvertor:
    
    def __init__(self, file_type, connection_string, container_name):
        self.client = DirectoryClient(connection_string, container_name)
        self.file_type = file_type

    def get_next_file_date(self, config, file_type):
        query = "select max(parquet_file_date) from {} where file_type='{}';".format(config['control_table'],file_type)
        logging.info('Transaction Date Query: {}'.format(query))
        query_result = subprocess.check_output(['hive', '-e','{}'.format(query)])
        max_file_date = query_result.decode("utf-8").rstrip('\n')
        logging.info('Max file date is {}'.format(max_file_date))

        if max_file_date is None or max_file_date == 'NULL':
            return None
        elif self.get_transaction_date(file_type) == max_file_date:
            return max_file_date
        else:
            if file_type == 'ALL':
                next_file_date = (datetime.datetime.strptime(max_file_date, '%Y-%m-%d') + datetime.timedelta(7)).strftime('%Y-%m-%d')
            else:
                next_file_date = (datetime.datetime.strptime(max_file_date, '%Y-%m-%d') + datetime.timedelta(1)).strftime('%Y-%m-%d')
            return next_file_date

    def insert_process_logs(self, config, file_type, parquet_file_name, parquet_file_date, region, data_file_name, process_date):
        logging.info('Preparing control table insert.')
        insert_query = "insert into {} (file_type,parquet_file_name,parquet_file_date,region,data_file_name,process_date) values ('{}','{}','{}','{}','{}','{}');".format(
            config['control_table'], file_type,parquet_file_name,parquet_file_date,region,data_file_name,process_date)
        
        insert_status = subprocess.call(['hive', '-e','{}'.format(insert_query)])
        #check status
        if insert_status != 0:
            logging.info('Failed to insert into control table. {}'.format(insert_query))
            raise Exception('DataInsertFailure: Failed to insert into Control table.')
        else:
            logging.info('Control data inserted successfully.')

    def get_parquet_file_name(self, azure_folder_name):
        return self.client.ls_parquet_files(azure_folder_name,False)
        
    def download_parquet_files(self, azure_file_name, parquet_file_name):
        logging.info('Downloading file {}'.format(azure_file_name))
        self.client.download(azure_file_name,parquet_file_name)
        logging.info('{} file downloaded successfully as {}'.format(azure_file_name, parquet_file_name))

    def convert_parquet_to_delimited(self, parquet_file, transaction_date):
        logging.info('Preparing to convert {} file into delimited.'.format(parquet_file))
        dir_name = os.path.dirname(parquet_file)
        delimited_file_name = os.path.splitext(os.path.basename(parquet_file))[0]
        df = pd.read_parquet(parquet_file, engine='pyarrow')
        df['TX_DATE'] = transaction_date.replace('-','')
        df['FILE_DOWNLOAD_DATE'] = datetime.date.today().strftime('%Y%m%d')
        df.to_csv(dir_name+'/'+delimited_file_name+'.dat', sep='|')
        del df
        gc.collect()
        logging.info('{} Parquet file is converted into delimited successfully.'.format(parquet_file))
        # Removing Parquet file
        logging.info('Removing the Parquet file {}'.format(parquet_file))
        os.remove(parquet_file)
        return delimited_file_name+'.dat'

    def get_transaction_date(self, file_type):
        if file_type == 'ALL':
            return (datetime.date.today() - datetime.timedelta((datetime.date.today().weekday() - 5)%7)).strftime('%Y-%m-%d')
        else:
            return (datetime.date.today() - datetime.timedelta(2)).strftime('%Y-%m-%d')



def load_config_file(config_file, file_type):
    configParser = configparser.RawConfigParser()
    configParser.read(config_file)
    return dict(configParser.items(file_type))


#Get the transaction date for file download
def get_transaction_date(fileConvertor, config_dict, file_type):
    next_file_date = fileConvertor.get_next_file_date(config_dict, file_type)
    if next_file_date is None:
        return fileConvertor.get_transaction_date(file_type)
    else:
        return next_file_date

def download_files(fileConvertor, config, region, file_type, transaction_date):

    # Get the file list form Azure
    if config['azure_base_path'] == '':
        raise Exception('azure_base_path can not be empty')
    elif not config['azure_base_path'].endswith('/'):
        folder_name = config['azure_base_path'] + '/' + region + '/CALENDAR_DT=' + transaction_date + '/'
    else:
        folder_name = config['azure_base_path'] + region + '/CALENDAR_DT=' + transaction_date + '/'

    files = fileConvertor.get_parquet_file_name(folder_name)
    
    try:
        file_to_dowload=folder_name+files[0]
    except IndexError:
        logging.error('File is not available for date: {}. Exiting...'.format(transaction_date))
        sys.exit(1)

    data_file_name = 'osa_' + file_type.lower() + "_items_" + region + '_' + transaction_date + '.parquet'

    if config['file_download_path'] == '':
        raise Exception('EmptyFilePath: file_download_path can not be empty')
    elif not config['file_download_path'].endswith('/'):
        result_file_name = config['file_download_path'] + '/' +  data_file_name
    else:
        result_file_name = config['file_download_path'] + data_file_name

    logging.info('Preparing to download file : {}'.format(file_to_dowload))

    # Download Parquet files
    
    fileConvertor.download_parquet_files(file_to_dowload, result_file_name)
    
    ('File {} downloaded successfully to {}'.format(file_to_dowload, result_file_name))

    # convert Parquet files to delimited
    
    data_file_name = fileConvertor.convert_parquet_to_delimited(result_file_name, transaction_date)

    #insert_process_logs
    fileConvertor.insert_process_logs(config, file_type, file_to_dowload, transaction_date, region, data_file_name, datetime.date.today().strftime('%Y-%m-%d'))

    

def main():

    try:
        CONFIG_FILE_NAME = sys.argv[1]
        FILE_TYPE = sys.argv[2]
    except IndexError:
        logging.info('usage: osa_file_download.py CONFIG_FILE_NAME FILE_TYPE <ALL/KEY>')
        logging.error('error: the following arguments are required: CONFIG_FILE_NAME FILE_TYPE')
        sys.exit(1)
    
    if FILE_TYPE.islower():
        FILE_TYPE = FILE_TYPE.upper()

    config = load_config_file(CONFIG_FILE_NAME, FILE_TYPE)
    
    fileConvertor = ParquetFileConvertor(FILE_TYPE, config['connection_string'], config['azure_blob_container_name'])

    transaction_date = get_transaction_date(fileConvertor, config, FILE_TYPE) if 'transaction_date' not in config or config['transaction_date'] is None else config['transaction_date']

    logging.info('The transaction date is {}'.format(transaction_date))

    day_range = int(config['day_range']) if 'day_range' in config else 1
    load_type = config['type'].upper() if 'type' in config else 'REGULAR'
    
    logging.info(f'Load type is {load_type}')
    
    for day in range(day_range):
        logging.info('Iteration {} of {}'.format(day+1, day_range))
        logging.info('transaction date is {}'.format(transaction_date))
        for region in config['regions'].split(','):
            download_files(fileConvertor, config, region, FILE_TYPE, transaction_date)
            
        if load_type != 'HISTORY':
            transaction_date = (datetime.datetime.strptime(transaction_date, '%Y-%m-%d') - datetime.timedelta(1)).strftime('%Y-%m-%d')
        else:
            transaction_date = (datetime.datetime.strptime(transaction_date, '%Y-%m-%d') + datetime.timedelta(1)).strftime('%Y-%m-%d')
    
                
                
if __name__ == "__main__":
    main()
