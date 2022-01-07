import configparser
import logging

logging.basicConfig(level=logging.INFO)

CONFIG_FILE='./config/sams_po_attr_keying.cfg'
TABLE_LIST_FILE='./config/sams_po_attr_tables.lst'
DB_NAME='sams_supply_chain'
OUTPUT_FORMAT='orc'

def get_spark_session():
    """
    Spark Session Initialization
    :return: spark
    """
    logging.info("Spark Initialisation Started ")
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.config(
            "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
        )
        .config("spark.app.name", "sams_po_attr_keying")
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
    logging.info("Spark Intialised")
    spark._jsc.hadoopConfiguration().set(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
    )
    return spark

def load_config_file(file_type):
    configParser = configparser.RawConfigParser()
    configParser.read(CONFIG_FILE)
    return dict(configParser.items(file_type))

def get_formatted_query(sql, max_seq='', file_id=''):
    return sql.replace(':db_name', DB_NAME).replace(':max_seq', max_seq).replace(':file_id', file_id)

def main():

    spark = get_spark_session()
    logging.info('Fetching File_ID') 
    file_id_df = spark.sql('select * from sams_supply_chain.latest_file_id')
    file_id = str(file_id_df.select(file_id_df.columns[0]).first()[0])
    print(f'File_id:{file_id}')
    logging.info('Loading data into: SAMS_PO_ATTR_TEMP1') 
    config = load_config_file('SAMS_PO_ATTR_TEMP1')
    extract_sql = config['extract_sql']
    extract_query = get_formatted_query(extract_sql, file_id=file_id)
    print(extract_query)
    df = spark.sql(extract_query)
    df.write.format(OUTPUT_FORMAT).insertInto(DB_NAME+'.SAMS_PO_ATTR_TEMP1', overwrite=True)
    
    with open(TABLE_LIST_FILE, 'r') as table_list:
        for line in table_list:
            table = line.strip().upper()
            logging.info(f'Started processing table: {table}')
            config = load_config_file(table)
            
            logging.info(f'Loading config proprties for table: {table}')
            max_seq_sql = config['max_seq_sql']
            extract_sql = config['extract_sql']
            
            logging.info(f'Finding the max sequence in table: {table}')
            max_sql = get_formatted_query(max_seq_sql)
            print(max_sql)
            max_seq_result = spark.sql(max_sql)
            max_seq = str(max_seq_result.select(max_seq_result.columns[0]).first()[0])
            if max_seq is None:
                max_seq = '30000'
            
            logging.info(f'Extracting records from: {table}')    
            extract_query = get_formatted_query(extract_sql, max_seq=max_seq)
            print(extract_query)
            df = spark.sql(extract_query)
            
            logging.info(f'Loading data into: {table}')
            df.write.format(OUTPUT_FORMAT).insertInto(DB_NAME+'.'+table, overwrite=False)
            
    logging.info('Loading data into: SAMS_PO_DIM')        
    config = load_config_file('SAMS_PO_DIM')        
    extract_sql = config['extract_sql']
    extract_query = get_formatted_query(extract_sql)
    print(extract_query)
    df = spark.sql(extract_query)
    df.write.format(OUTPUT_FORMAT).insertInto(DB_NAME+'.SAMS_PO_DIM', overwrite=False)
    
    logging.info('Loading data into: SAMS_PO_DIM_PRD')        
    config = load_config_file('SAMS_PO_DIM_PRD')        
    extract_sql = config['extract_sql']
    extract_query = get_formatted_query(extract_sql)
    print(extract_query)
    df = spark.sql(extract_query)
    df.write.format(OUTPUT_FORMAT).insertInto(DB_NAME+'.SAMS_PO_DIM_PRD', overwrite=True)
    logging.info('Job Completed Successfully') 
    spark.stop()

if __name__ == "__main__":
    main()