import os
import sys
import subprocess
import datetime
import logging
import configparser
import argparse
import shutil

logging.basicConfig(level=logging.INFO)


def load_config_file(config_file):
    configParser = configparser.RawConfigParser()
    configParser.read(config_file)
    return dict(configParser.items('SL'))


def get_file_date():
    return datetime.date.today().strftime("%Y%m%d")


def load_arguments():
    parser = argparse.ArgumentParser(
        usage="sl_file_download.py -c <CONFIG_FILE_NAME> -t <TRANSACTION_DATE>(Optional)",
        description="Utility to download SL feeds from azure blob storeage",
    )
    parser.add_argument("-c", "--config_file", help="Config file path", required=True)
    parser.add_argument("-t", "--file_date", help="Transaction Date")
    parser.add_argument(
        "-r", "--day_range", help="day range to download the History Files"
    )
    return parser


def download_files(config, file_regex):
    file_download_command = """{} copy "https://{}.blob.core.windows.net/{}{}/{}" "{}" --overwrite=ifSourceNewer --check-md5 FailIfDifferent --from-to=BlobLocal --include-pattern '{}' --recursive ;""".format(
        config["az_copy"],
        config["azure_account_name"],
        config["azure_blob_container"],
        os.path.join(config["azure_base_path"], config["sl_folder_name"]),
        config["azure_sas_token"],
        config["file_download_path"],
        file_regex,
    )

    download_status = os.system(file_download_command)
    print(f"download Status:{download_status}")

    if download_status != 0:
        logging.error(f"Failed to download SL file {file_regex}")
        raise Exception(
            "FileDownloadException: Failed to download the file from azure container."
        )


def get_file_regex(config, file_date):
    return config["file_suffix"] + file_date + "*" + config["file_ext"]


def load_raw_table(config):
    file_dir = os.path.join(config["file_download_path"],config["sl_folder_name"])
    for file in os.listdir(file_dir):
        status_code = subprocess.call(
            ["hadoop", "fs", "-put", "-f", os.path.join(file_dir,file), config["table_hdfs_location"]]
        )
        if status_code != 0:
            logging.info("Failed to move data into raw table.")
            raise Exception("IOException: Failed to move data into raw table.")


def invalidate_metadata(table_name):
    invalidate_query = f"msck repair table {table_name}"
    status_code = subprocess.call(["hive", "-e", invalidate_query])
    if status_code != 0:
        logging.info("Failed to invalidate raw table.")
        raise Exception("IOException: Failed to invalidate raw table.")


def archive_files(config):
    internal_file_path = os.path.join(config["file_download_path"], config["sl_folder_name"])
    for file in os.listdir(internal_file_path):
        file_path = os.path.join(internal_file_path, file)
        shutil.move(file_path, config["archive_path"])
    os.removedirs(internal_file_path)
    
def truncate_table(table_name):   
    truncate_query = f"truncate table {table_name}"
    status_code = subprocess.call(['hive', '-e', truncate_query]) 
    if status_code !=0:
        logging.info(f"Failed to truncate {table_name} table.")
        raise Exception("IOException: Failed to truncate table.")


def main():

    try:
        argument_parser = load_arguments().parse_args()
    except:
        sys.exit(1)

    config = load_config_file(argument_parser.config_file)
    day_range = (
        1 if argument_parser.day_range is None else int(argument_parser.day_range)
    )
    file_date = (
        get_file_date()
        if argument_parser.file_date is None
        else argument_parser.file_date
    )

    logging.info(f"The file date to download is {file_date}")

    load_type = config["type"].upper() if "type" in config else "REGULAR"

    logging.info(f"Load type is {load_type}")

    file_regex = get_file_regex(config, file_date)

    for day in range(day_range):
        logging.info(f"Iteration {day+1} of {day_range}")
        logging.info(f"Downloading file {file_regex}")

        download_files(config, file_regex)

        if load_type != "HISTORY":
            file_date = (
                datetime.datetime.strptime(file_date, "%Y%m%d") - datetime.timedelta(1)
            ).strftime("%Y%m%d")
        else:
            file_date = (
                datetime.datetime.strptime(file_date, "%Y%m%d") + datetime.timedelta(1)
            ).strftime("%Y%m%d")

        file_regex = get_file_regex(config, file_date)

    logging.info("File Download is completed.")

    if os.path.isdir(os.path.join(config["file_download_path"], config["sl_folder_name"])):
        
        logging.info(f"Cleaning up the target table.")
        truncate_table(config['hive_table'])
        
        logging.info(f"Loading Raw table {config['hive_table']}.")
        load_raw_table(config)
        logging.info(f"Raw ext table {config['hive_table']} loaded successfully.")

        logging.info("Invalidating Metadata.")
        invalidate_metadata(config["hive_table"])

        logging.info("Archiving Files from Local.")
        archive_files(config)
    else:
        logging.info("Files are not available to download")
        sys.exit(1)

    logging.info("Process Completed successfully.")


if __name__ == "__main__":
    main()
