from glob import glob
import os
import sys
import subprocess
import logging
import pandas as pd

def get_schema(sheet):
    print(f'fetching the schema information for {sheet}')
    schema_file = pd.read_excel('/opt/supply_chain/dev/sams_club/raw_data_load/MADRiD_Feed_Details.xls', sheet_name=sheet, header = None)
    # col_df = schema_file.loc[:,[1]][3:]
    # schema_df = schema_file.loc[:,[2]][3:]
    # schema_list = list(zip(col_df[1], schema_df[2]))
    # schema = []
    
    # for column,data_type in schema_list:
    #     schema.append(column + ' ' + data_type)
    
    schema = list(schema_file[3:][1] + ' ' + schema_file[3:][3])
        
    return ','.join(schema)

def create_ext_table(table_name,file_type):
    print(f'creating table {table_name}')
    schema = get_schema(file_type)
    query = f'''create table if not exists {table_name}
    ( {schema} ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
    WITH SERDEPROPERTIES ( "separatorChar" = "|", "quoteChar" = "\\"") stored as textfile;'''
    print(f'create table query = {query}')
    status_code = subprocess.call(['hive', '-e', query]) 
    if status_code !=0:
        logging.info(f"Failed to create table {table_name}.")
        raise Exception("IOException: Failed to create table.")
    
def load_ext_table(local_path, hdfs_path):
    print(f'Started moving data from {local_path} to {hdfs_path}')
    file_list = glob(local_path + '/*')
    for file in file_list:
        print(f'loading file {file}')
        status_code = subprocess.call(['hadoop', 'fs', '-put', file, hdfs_path])
        if status_code !=0:
            logging.info("Failed to move data into ext table.")
            raise Exception("IOException: Failed to move data into raw table.")
    
def invalidate_metadata(table_name):
    print(f'invalidating table {table_name}')
    invalidate_query = f"msck repair table {table_name}"
    status_code = subprocess.call(['hive', '-e', invalidate_query])
    if status_code !=0:
        logging.info("Failed to invalidate ext table.")
        raise Exception("IOException: Failed to invalidate ext table.")

def main():
    folder_list = sys.argv[1]
    local_base_path = '/externaldata01/dev/sams_club/'
    hdfs_base_path = '/sams_supply_chain/data/db/sams_supply_chain.db/external/'
    db_name = 'sams_supply_chain_external'
    
    for folder in folder_list.split(','):
        print(f'processing for {folder}')
        
        table_name = f'{db_name}.{folder}_raw_ext'
        create_ext_table(table_name,folder)
        
        local_path = os.path.join(local_base_path, folder)
        hdfs_path = os.path.join(hdfs_base_path, folder.lower() + '_raw_ext/')
        print(local_path)
        load_ext_table(local_path, hdfs_path)
        
        invalidate_metadata(table_name)
    
if __name__ == "__main__":
    main()