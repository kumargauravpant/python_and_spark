import argparse
import sys
import subprocess

HDFS_TMP_PATH = '/tmp/sams_po_dim'
LOCAL_FILE_PATH = '/opt/supply_chain/prd/sams_club/history_load/sams_po_dim.dat'
def get_spark_session():
    """
    Spark Session Initialization
    :return: spark
    """
    #    logger.info("Spark Initialisation Started ")
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.config(
            "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
        )
        .config("spark.app.name", "sams_po_dim_extract")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.sql.parquet.writeLegacyFormat", "true")
        .config("spark.sql.hive.convertMetastoreParquet", "false")
        .config("hive.mapred.supports.subdirectories", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.default.parallelism", "200")
        .config("spark.sql.crossJoin.enabled", "true")
        .config("spark.sql.execution.arrow.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )
    #    logger.info("Spark Intialised")
    spark._jsc.hadoopConfiguration().set(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
    )
    return spark

def get_dat_file(hdfs_path, local_path):
    status_code = subprocess.call(['hadoop', 'fs', '-get', hdfs_path+'/*.csv', local_path])
    if status_code !=0:
        print("Failed to get the dat file from hdfs.")
        raise Exception("IOException: Failed to get the dat file from hdfs.")
    
def main():

    spark = get_spark_session()
    query = """SELECT DISTINCT
        po_dim_key
    , po_num_avp_key
    , po_order_date_avp_key
    , po_last_chg_date_avp_key
    , po_ship_date_avp_key
    , po_cancel_date_avp_key
    , po_must_arrive_date_avp_key
    , po_orig_mabd_date_avp_key
    , po_original_cancel_date_avp_key
    , po_process_date_avp_key
    , po_received_date_avp_key
    , new_item_flag_avp_key
    , po_status_avp_key
    , step_status_avp_key
    , transfer_type_avp_key
    , po_vendor_num_avp_key
    , po_vendor_num_dept_avp_key
    , po_vendor_num_seq_avp_key
    , col_ppd_avp_key
    , otb_month_avp_key
    , sams_po_type_avp_key
    , dept_num_avp_key
    , receiver_num_avp_key
    , carrier_name_receiving_avp_key
    , rcvr_lock_sw_avp_key
    , rcvd_flag_polr_avp_key
    , pa_print_flag_avp_key
    , fresh_flag_avp_key
    , mds_fam_id_avp_key
    , madrid_po_type_avp_key
    , madrid_po_id_avp_key
    , segment_status_avp_key
    , event_desc_avp_key
    , seg_status_avp_key
    , item_desc_avp_key
    , retailer_sku_avp_key
    , cat_avp_key
    , cat_desc_avp_key
    , subcat_avp_key
    , subcat_desc_avp_key
    , signing_desc_avp_key
    , upc_avp_key
    , rdh_total_store_avp_key
    , rdh_gmm_avp_key
    FROM
        sams_supply_chain.sams_po_dim limit 10"""
        
    print('Reading data from hive')
    raw_df = spark.sql(query)

    # Replacing null values with 30000
    df = raw_df.na.fill(value=30000)

    # Writing data into csv file 
    print("Creating DAT file")
    df.repartition(1).write.mode("overwrite").csv('/tmp/sams_po_dim', sep='|')
    print("DAT file is created")
    spark.stop()
    print('Fetching the dat file')
    get_dat_file(HDFS_TMP_PATH, LOCAL_FILE_PATH)


if __name__ == "__main__":
    main()
