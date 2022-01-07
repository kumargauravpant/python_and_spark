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
        .config("spark.app.name", "oos_fact_adjustment_tsv34")
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
    venue_dim_key
  , item_dim_key
  , 1 as oos_status_dim_key
  , tm_dim_key_day
  , retailer_dim_key
from
    (
        select
            fact.venue_dim_key
          , fact.item_dim_key
          , fact.tm_dim_key_day
          , fact.retailer_dim_key
          , clst.running_total_adjust_count
          , clst.total_adjust_count
          , clst.adjust_count
          , row_number() over ( partition by fact.tm_dim_key_day, fact.retailer_dim_key, fact.item_dim_key, clst.fips_cluster_id order by
                               sales_gap - oos_gap_threshold, sales_gap, tiebreaker) rnk
        from
            (
                select
                    venue_dim_key
                  , item_dim_key
                  , retailer_dim_key
                  , sales_gap
                  , oos_gap_threshold
                  , rand(20200501) as tiebreaker
                  , tm_dim_key_day
                from
                    supply_chain_oos.oos_fact_tsv34
                where
                    tm_dim_key_day between {argument_parser.ws_exec_tdk_start} and {argument_parser.ws_exec_tdk_end}
                    and oos_status_dim_key = 2
                    and retailer_dim_key in ({argument_parser.ws_retailer_id_list})
            )
            fact
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
            inner join
                (
                    select
                        c.item_dim_key
                      , c.fips_cluster_id
                      , c.retailer_dim_key
                      , least(ceiling((adjust_weight / total_adjust_weight) * total_adjust_count), in_stock_and_oos_count - in_stock_count) as adjust_count
                      , sum(least(ceiling((adjust_weight / total_adjust_weight) * total_adjust_count), in_stock_and_oos_count - in_stock_count)) over ( partition by c.tm_dim_key_day, c.item_dim_key, c.retailer_dim_key order by
                      adjust_weight desc, rand(20200501)) running_total_adjust_count
                      , t.total_adjust_count
                      , c.tm_dim_key_day
                    from
                        supply_chain_oos.oos_fact_retailer_item_fips_cluster_tsv34 c
                        inner join
                            supply_chain_oos.oos_fact_adjustment_total_tsv34 t
                            on
                                c.item_dim_key         = t.item_dim_key
                                and c.retailer_dim_key = t.retailer_dim_key
                                and c.tm_dim_key_day   = t.tm_dim_key_day
                    where
                        c.tm_dim_key_day between {argument_parser.ws_exec_tdk_start} and {argument_parser.ws_exec_tdk_end}
                        and c.adjust_weight > 0.0
                        and c.retailer_dim_key in ({argument_parser.ws_retailer_id_list})
                        and t.tm_dim_key_day between {argument_parser.ws_exec_tdk_start} and {argument_parser.ws_exec_tdk_end}
                        and t.total_adjust_count > 0
                        and t.retailer_dim_key in ({argument_parser.ws_retailer_id_list})
                )
                clst
                on
                    vnuc.fips_cluster_id      = clst.fips_cluster_id
                    and fact.item_dim_key     = clst.item_dim_key
                    and fact.retailer_dim_key = clst.retailer_dim_key
                    and fact.tm_dim_key_day   = clst.tm_dim_key_day
    )
    adjustment_candidates
where
    rnk <= adjust_count
    and
    (
        running_total_adjust_count - adjust_count
    )
    < total_adjust_count
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