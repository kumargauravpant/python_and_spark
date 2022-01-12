import os
import sys
import subprocess
import datetime
import logging
import configparser
import argparse
import shutil

logging.basicConfig(level=logging.INFO)


def load_config_file(config_file, file_type):
    configParser = configparser.RawConfigParser()
    configParser.read(config_file)
    return dict(configParser.items(file_type))


def get_transaction_date(file_type):
    if file_type == "ALL":
        return (
            datetime.date.today()
            - datetime.timedelta((datetime.date.today().weekday() - 5) % 7)
        ).strftime("%Y-%m-%d")
    else:
        return (datetime.date.today() - datetime.timedelta(2)).strftime("%Y-%m-%d")


def load_arguments():
    parser = argparse.ArgumentParser(
        usage="osa_file_download.py <CONFIG_FILE_NAME> <ALL/KEY> <DAY_RANGE>",
        description="Utility to download OSA feeds from azure blob storeage",
    )
    parser.add_argument("-c", "--config_file", help="Config file path", required=True)
    parser.add_argument("-t", "--feed_type", help="ALL/KEY", required=True)
    parser.add_argument(
        "-r", "--day_range", help="day range to download the files for"
    )
    return parser


def download_files(
    config, region, transaction_date, file_landing_folder
):  
    azure_dir = config['azure_base_path']+region+'/CALENDAR_DT%3D'+transaction_date+'/'

    file_download_command = """{} copy "https://{}.blob.core.windows.net/{}{}{}" "{}" --overwrite=ifSourceNewer --check-md5 FailIfDifferent --from-to=BlobLocal --recursive ;""".format(
        config["az_copy"],
        config["azure_account_name"],
        config["azure_blob_container"],
        azure_dir,
        config["azure_sas_token"],
        file_landing_folder,
    )
    
    download_status = os.system(file_download_command)
    print(f'download Status:{download_status}')
    
    if download_status != 0:
        logging.error(f"Failed to download OSA file for date {transaction_date}")
        raise Exception("FileDownloadException: Failed to download the file from azure container.")
    

def get_landing_folder(local_path):
    file_download_date = datetime.date.today().strftime("%Y%m%d")
    landing_folder = local_path + "file_id=" + file_download_date
    # Creating the Download Folder
    process_status = subprocess.call(["mkdir", "-p", landing_folder])

    if process_status != 0:
        logging.info(f"Failed to create the download Folder  {landing_folder}")
        raise Exception(f"IOException: Failed to create the download Folder {landing_folder}.")
    else:
        logging.info(f"Download Folder {landing_folder} created successfully.")
        return landing_folder
    
def metadata_files_cleanup(file_dir):
    metadata_files = ['_started*','_committed*','_SUCCESS*']
    
    for file in metadata_files:
        status_code = subprocess.call(['find', file_dir, '-name', file, '-delete'])
        if status_code != 0:
            logging.info("Failed to remove metadata files")
            raise Exception("IOException: Failed to remove metadata files.")
        
def create_partitions(file_path):
    for dir in os.listdir(file_path):
        currentDir = f'{file_path}/{dir}'
        newDir = f'{file_path}/{dir.lower()}'
        print(currentDir)
        print(newDir)
        os.rename(currentDir, newDir)
    
def clean_hive_tables(table_name, date_list):
    for date in date_list:
        drop_partition_query = f"alter table {table_name} drop if exists partition(calendar_dt='{date}')"
        status_code = subprocess.call(['hive', '-e', drop_partition_query]) 
        if status_code !=0:
            logging.info("Failed to remove partitions from table.")
            raise Exception("IOException: Failed to remove partitions from table.")

def load_raw_table(local_path, hdfs_path):
    status_code = subprocess.call(['hadoop', 'fs', '-put', local_path, hdfs_path])
    if status_code !=0:
        logging.info("Failed to move data into raw table.")
        raise Exception("IOException: Failed to move data into raw table.")
        
def invalidate_metadata(table_name):
    invalidate_query = f"msck repair table {table_name}"
    status_code = subprocess.call(['hive', '-e', invalidate_query])
    if status_code !=0:
        logging.info("Failed to invalidate raw table.")
        raise Exception("IOException: Failed to invalidate raw table.")
    
def archive_files(local_path, archive_folder):
    curr_time = datetime.datetime.now().strftime('%H%M%S')
    new_local_path = f'{local_path}_{curr_time}'
    os.rename(local_path, new_local_path)
    shutil.move(new_local_path, archive_folder)
    

def main():

    try:
        argument_parser = load_arguments().parse_args()
    except:
        sys.exit(1)

    file_type = str(argument_parser.feed_type).upper()
    config = load_config_file(argument_parser.config_file, file_type)
    day_range = (
        # int(argument_parser.day_range) if "day_range" not in config else int(config["day_range"])
        int(config["day_range"]) if argument_parser.day_range is None else int(argument_parser.day_range)
    )
    transaction_date = (
        get_transaction_date(file_type)
        if "transaction_date" not in config or config["transaction_date"] is None
        else config["transaction_date"]
    )

    logging.info(f"The Transaction Date to start the load is {transaction_date}")

    load_type = config["type"].upper() if "type" in config else "REGULAR"

    logging.info(f"Load type is {load_type}")

    landing_folder = get_landing_folder(config["file_download_path"])

    downloaded_days = []
    for day in range(day_range):
        logging.info(f"Iteration {day+1} of {day_range}")
        logging.info(f"Downloading file for transaction_date {transaction_date}")

        for region in config["regions"].split(","):
            download_files(
                config,
                region,
                transaction_date,
                landing_folder
            )

        downloaded_days.append(transaction_date)
        if load_type != "HISTORY":
            transaction_date = (
                datetime.datetime.strptime(transaction_date, "%Y-%m-%d")
                - datetime.timedelta(1)
            ).strftime("%Y-%m-%d")
        else:
            transaction_date = (
                datetime.datetime.strptime(transaction_date, "%Y-%m-%d")
                + datetime.timedelta(1)
            ).strftime("%Y-%m-%d")

    logging.info("File Download is completed.")
    
    logging.info("Initializing the metadata files cleanup.")
    metadata_files_cleanup(landing_folder)
    
    logging.info("Creating partitions folders.")
    create_partitions(landing_folder)

    logging.info("Removing partitions from table.")
    clean_hive_tables(config['hive_table'], downloaded_days)
    
    logging.info(f"Loading Raw table {config['hive_table']}.")
    load_raw_table(landing_folder, config['table_hdfs_location'])
    
    logging.info(f"Raw table {config['hive_table']} loaded successfully.")
    logging.info("Invalidating Metadata.")
    invalidate_metadata(config['hive_table'])
    
    logging.info("Archiving Files from Local.")
    archive_files(landing_folder, config['archive_path'])
    
    logging.info("Process Completed successfully.")

if __name__ == "__main__":
    main()