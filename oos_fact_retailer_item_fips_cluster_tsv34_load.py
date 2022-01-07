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
        .config("spark.app.name", "oos_fact_retailer_item_fips_cluster_tsv34")
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
        usage="""oos_fact_retailer_item_fips_cluster_tsv34_load.py <ws_exec_tdk_start> <ws_exec_tdk_end> <ws_retailer_id_list> <hdfs_path>""",
        description="Utility to load oos history data",
    )
    parser.add_argument(
        "-ws_exec_tdk_start",
        "--ws_exec_tdk_start",
        help="ws_exec_tdk_start",
        required=True,
    )
    parser.add_argument(
        "-ws_exec_tdk_end", "--ws_exec_tdk_end", help="ws_exec_tdk_end", required=True
    )
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
  , fips_cluster_id
  , in_stock_count
  , in_stock_and_oos_count
  , in_stock_rate
  , adjusted_rate
  , in_stock_and_oos_count * (adjusted_rate - in_stock_rate) as adjust_weight
  , tm_dim_key_day
  , retailer_dim_key
from
    (
        select
            item_dim_key
          , fips_cluster_id
          , retailer_dim_key
          , in_stock_count
          , in_stock_and_oos_count
          , in_stock_rate
          , least(in_stock_rate * 1.1, 1) as adjusted_rate
          , tm_dim_key_day
        from
            (
                select
                    item_dim_key
                  , fips_cluster_id
                  , retailer_dim_key
                  , in_stock_count
                  , in_stock_and_oos_count
                  , tm_dim_key_day
                  , case
                        when (
                                in_stock_count is not null
                                and in_stock_count       > 0
                            )
                            and (
                                in_stock_and_oos_count is not null
                                and in_stock_and_oos_count       > 0
                            )
                            then in_stock_count / in_stock_and_oos_count
                            else 0
                    end as in_stock_rate
                from
                    (
                        select
                            fact.item_dim_key
                          , vnuc.fips_cluster_id
                          , fact.tm_dim_key_day
                          , fact.retailer_dim_key
                          , sum
                                (
                                    case
                                        when oos_status_dim_key = 1
                                            then 1
                                            else 0
                                    end
                                )
                            as in_stock_count
                          , sum
                                (
                                    case
                                        when oos_status_dim_key in (1
                                                                  , 2)
                                            then 1
                                            else 0
                                    end
                                )
                            as in_stock_and_oos_count
                        from
                            supply_chain_oos.oos_fact_tsv34 fact
                            inner join
                                supply_chain_oos.it_dim_tsvsummt_71783 itmd
                                on
                                    fact.item_dim_key = itmd.item_dim_key
                            inner join
                                supply_chain_oos.vn_dim_iri_8135 vnud
                                on
                                    fact.venue_dim_key = vnud.venue_dim_key
                            inner join
                                supply_chain_oos.vn_dim_iri_fips_attr vnua
                                on
                                    vnud.s_49076_key = vnua.fips_key
                            inner join
                                supply_chain_oos.vn_dim_iri_fips_cluster vnuc
                                on
                                    vnua.fips_code = vnuc.fips_code
                        where
                            tm_dim_key_day between {argument_parser.ws_exec_tdk_start} and {argument_parser.ws_exec_tdk_end}
                            and oos_status_dim_key in (1
                                                     , 2)
                            and retailer_dim_key in ({argument_parser.ws_retailer_id_list})
                        group by
                            fact.item_dim_key
                          , vnuc.fips_cluster_id
                          , fact.tm_dim_key_day
                          , fact.retailer_dim_key
                    )
                    initial_sum
            )
            stock_rate
    )
    diff_rate
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
