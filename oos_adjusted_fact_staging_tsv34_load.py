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
        .config("spark.app.name", "oos_adjusted_fact_staging_tsv34")
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
        usage="""oos_fact_adjustment_tsv34_load.py <ws_exec_tdk_start> <ws_exec_tdk_end> <ws_retailer_id_list> <hdfs_path>""",
        description="Utility to load oos history data",
    )
    parser.add_argument("-ws_exec_tdk_start", "--ws_exec_tdk_start", help="ws_exec_tdk_start", required=True)
    parser.add_argument("-ws_exec_tdk_end", "--ws_exec_tdk_end", help="ws_exec_tdk_end", required=True)
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
    f.venue_dim_key
  , f.item_dim_key
  , f.sales_gap
  , f.item_quantity
  , f.item_net_amount
  , nvl(a.oos_status_dim_key, f.oos_status_dim_key) as oos_status_dim_key
  , f.oos_gap_threshold
  , f.unmodified_oos_gap_threshold
  , f.unit_sales_cy_ya_ratio
  , f.item_spike_tm_dim_key_day
  , f.item_reset_tm_dim_key_day
  , f.subcat_spike_tm_dim_key_day
  , f.subcat_reset_tm_dim_key_day
  , f.retailer_dim_key
  , f.tm_dim_key_day
from
    (
        select    *
        from
            supply_chain_oos.oos_fact_tsv34
        where
            tm_dim_key_day between {argument_parser.ws_exec_tdk_start} and {argument_parser.ws_exec_tdk_end}
    )
    f
    left outer join
        (
            select    *
            from
                supply_chain_oos.oos_fact_adjustment_tsv34
            where
                tm_dim_key_day between {argument_parser.ws_exec_tdk_start} and {argument_parser.ws_exec_tdk_end}
        )
        a
        on
            f.venue_dim_key        = a.venue_dim_key
            and f.item_dim_key     = a.item_dim_key
            and f.tm_dim_key_day   = a.tm_dim_key_day
            and f.retailer_dim_key = a.retailer_dim_key
"""
print(query)
print("Creating DF")
df = spark.sql(query)
print("writing data into hdfs")
df.coalesce(50).write.partitionBy("tm_dim_key_day").save(
    argument_parser.hdfs_path, format="parquet", mode="append"
)
print("Completed")

spark.stop()