def get_spark_session():
    '''
    Spark Session Initialization
    :return: spark
    '''
    logger.info("Spark Initialisation Started ")
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config('spark.sql.sources.partitionOverwriteMode', 'dynamic') \
        .config('hive.exec.dynamic.partition.mode', 'nonstrict') \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("hive.mapred.supports.subdirectories", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    logger.info("Spark Intialised")
    spark._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    return spark

spark = get_spark_session()
query = open("/home/prgvk/png/store_sql.hql").read()
print(query)
print("Creating DF")
df = spark.sql(query % ('42812','42822','42812','42822'))
print("count")
print(df.count())
print("writing data into hdfs")
df.repartition(1).write.partitionBy("process_date").save(
    "/user/prgvk/png_extract/", format="parquet", mode="append"
)
print('Completed')

spark.stop()