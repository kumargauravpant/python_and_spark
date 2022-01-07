import argparse
import sys

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
        .config("spark.app.name", "oos_fact_adjustment_total_tsv34")
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


def load_arguments():
    parser = argparse.ArgumentParser(
        usage="""oos_fact_adjustment_total_tsv34_load.py <ws_exec_tdk_start> <ws_exec_tdk_end> <ws_retailer_id_list> <hdfs_path>""",
        description="Utility to load oos history data",
    )
    parser.add_argument("-ws_exec_tdk_start", "--ws_exec_tdk_start", help="ws_exec_tdk_start", required=True)
    parser.add_argument("-ws_exec_tdk_end", "--ws_exec_tdk_end", help="ws_exec_tdk_end", required=True)
    parser.add_argument(
        "-ws_retailer_id_list",
        "--ws_retailer_id_list",
        help="retailer_id_list",
        required=True,
    )
    parser.add_argument(
        "-hdfs_path", "--hdfs_path", help="hdfs_path", required=True,
    )
    return parser


try:
    argument_parser = load_arguments().parse_args()
except:
    sys.exit(1)
spark = get_spark_session()
query = f"""select
    item_dim_key
  , sum(in_stock_count)                                                                                         as total_in_stock_count
  , sum(in_stock_and_oos_count)                                                                                 as total_in_stock_and_oos_count
  , cast(least(bround(sum(in_stock_count) * 1.1), sum(in_stock_and_oos_count)) - sum(in_stock_count) as bigint) as total_adjust_count
  , sum(adjust_weight)                                                                                          as total_adjust_weight
  , tm_dim_key_day
  , retailer_dim_key
from
    supply_chain_oos.oos_fact_retailer_item_fips_cluster_tsv34
where
    tm_dim_key_day between {argument_parser.ws_exec_tdk_start} and {argument_parser.ws_exec_tdk_end}
    and retailer_dim_key in ({argument_parser.ws_retailer_id_list})
group by
    item_dim_key
  , tm_dim_key_day
  , retailer_dim_key
"""
print(query)
print("Creating DF")
df = spark.sql(query)
print("writing data into hdfs")
df.coalesce(50).write.partitionBy("tm_dim_key_day", "retailer_dim_key").save(
    argument_parser.hdfs_path, format="parquet", mode="append"
)
print("Completed")

spark.stop()