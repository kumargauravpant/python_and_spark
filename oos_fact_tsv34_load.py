import argparse
import sys
import logging

logging.basicConfig(level=logging.ERROR)


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
        .config("spark.app.name", "oos_fact_tsv34")
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
        usage="""oos_hist_load_pyspark.py <ws_exec_tdk> <ws_gap_threshold_tdk> <ws_min_gap_length>
        <ws_venue_item_history_limit> <ws_base_velocity_tdk> <ws_curr_velocity_tdk> <ws_reset_exec_tdk>
        <ws_spike_duration_after_reset> <ws_spike_exec_tdk> <ws_thld_adj> <ws_retailer_id_list> <hdfs_path>""",
        description="Utility to load oos history data",
    )
    parser.add_argument("-ws_exec_tdk", "--ws_exec_tdk", help="exec_tdk", required=True)
    parser.add_argument(
        "-ws_gap_threshold_tdk",
        "--ws_gap_threshold_tdk",
        help="sales_gap_threshold_end_tdk",
        required=True,
    )
    parser.add_argument(
        "-ws_min_gap_length",
        "--ws_min_gap_length",
        help="min_gap_length",
        required=True,
    )
    parser.add_argument(
        "-ws_venue_item_history_limit",
        "--ws_venue_item_history_limit",
        help="fact_venue_item_history_limit",
        required=True,
    )
    parser.add_argument(
        "-ws_base_velocity_tdk",
        "--ws_base_velocity_tdk",
        help="velocity_base_tdk",
        required=True,
    )
    parser.add_argument(
        "-ws_curr_velocity_tdk",
        "--ws_curr_velocity_tdk",
        help="velocity_curr_tdk",
        required=True,
    )
    parser.add_argument(
        "-ws_reset_exec_tdk",
        "--ws_reset_exec_tdk",
        help="reset_curr_exec_tdk",
        required=True,
    )
    parser.add_argument(
        "-ws_spike_duration_after_reset",
        "--ws_spike_duration_after_reset",
        help="spike_duration_after_reset",
        required=True,
    )
    parser.add_argument(
        "-ws_spike_exec_tdk",
        "--ws_spike_exec_tdk",
        help="spike_curr_exec_tdk",
        required=True,
    )
    parser.add_argument(
        "-ws_thld_adj", "--ws_thld_adj", help="threshold_adj", required=True
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
    gaps.pre_synd_venue_dim_key as venue_dim_key
  , gaps.pre_synd_item_dim_key  as item_dim_key
  , gaps.days_not_sold          as sales_gap
  , gaps.item_quantity
  , gaps.item_net_amount
  , case
        when oos_gap_threshold is null
            then 3
        when days_not_sold < {argument_parser.ws_min_gap_length}
            then 1
        when days_not_sold >= oos_gap_threshold * {argument_parser.ws_thld_adj}
            then 2
        when days_not_sold >= item_spike_gap_threshold
            then 2
        when days_not_sold >= subcat_spike_gap_threshold
            then 2
            else 1
    end as oos_status_dim_key
  , oos_gap_threshold
  , unmodified_oos_gap_threshold
  , unit_sales_curr_base_ratio as unit_sales_cy_ya_ratio
  , item_spike_tm_dim_key_day
  , item_reset_tm_dim_key_day
  , subcat_spike_tm_dim_key_day
  , subcat_reset_tm_dim_key_day
  , gaps.tm_dim_key_day
  , gaps.retailer_id as retailer_dim_key
from
    (
        select
            pre_synd_venue_dim_key
          , pre_synd_item_dim_key
          , retailer_id
          , days_not_sold
          , item_quantity
          , item_net_amount
          , tm_dim_key_day
        from
            supply_chain_oos.oos_venue_item_last_sold
        where
            tm_dim_key_day             = {argument_parser.ws_exec_tdk}
            and pre_synd_venue_dim_key > 0
            and pre_synd_item_dim_key  > 0
            and days_not_sold         <= {argument_parser.ws_venue_item_history_limit}
            and retailer_id in ({argument_parser.ws_retailer_id_list})
    )
    gaps
    left outer join
        (
            select
                ghst.pre_synd_item_dim_key
              , ghst.retailer_id
              , case
                    when curr_base_ratio is null
                        then gap_length_95p
                    when curr_base_ratio <= 1.0
                        then gap_length_95p
                        else gap_length_95p / curr_base_ratio
                end                       as oos_gap_threshold
              , gap_length_95p            as unmodified_oos_gap_threshold
              , spke.spike_tm_dim_key_day as item_spike_tm_dim_key_day
              , spke.reset_tm_dim_key_day as item_reset_tm_dim_key_day
              , spke.spike_gap_threshold  as item_spike_gap_threshold
              , sspk.spike_tm_dim_key_day as subcat_spike_tm_dim_key_day
              , sspk.reset_tm_dim_key_day as subcat_reset_tm_dim_key_day
              , sspk.spike_gap_threshold  as subcat_spike_gap_threshold
              , curr_base_ratio           as unit_sales_curr_base_ratio
            from
                supply_chain_oos.it_dim_tsvsummt_71783 itmd
                inner join
                    (
                        select
                            pre_synd_item_dim_key
                          , retailer_id
                          , gap_length_95p
                        from
                            supply_chain_oos.oos_retailer_item_sales_gap_threshold
                        where
                            end_tm_dim_key_day = {argument_parser.ws_gap_threshold_tdk}
                            and retailer_id in ({argument_parser.ws_retailer_id_list})
                    )
                    ghst
                    on
                        itmd.item_dim_key = ghst.pre_synd_item_dim_key
                left outer join
                    (
                        select
                            pre_synd_item_dim_key
                          , retailer_id
                          , curr_base_ratio
                        from
                            supply_chain_oos.oos_retailer_item_velocity_ratio
                        where
                            base_tm_dim_key_day     = {argument_parser.ws_base_velocity_tdk}
                            and curr_tm_dim_key_day = {argument_parser.ws_curr_velocity_tdk}
                            and retailer_id in ({argument_parser.ws_retailer_id_list})
                    )
                    velo
                    on
                        ghst.pre_synd_item_dim_key = velo.pre_synd_item_dim_key
                        and ghst.retailer_id       = velo.retailer_id
                left outer join
                    (
                        select
                            si.pre_synd_item_dim_key
                          , si.retailer_id
                          , si.spike_tm_dim_key_day
                          , ri.reset_tm_dim_key_day
                          , case
                                when {argument_parser.ws_exec_tdk} - nvl(ri.reset_tm_dim_key_day, {argument_parser.ws_exec_tdk}) < {argument_parser.ws_spike_duration_after_reset}
                                    then -1
                                    else ({argument_parser.ws_exec_tdk} - nvl(ri.reset_tm_dim_key_day, {argument_parser.ws_exec_tdk}) - {argument_parser.ws_spike_duration_after_reset}) + {argument_parser.ws_min_gap_length}
                            end as spike_gap_threshold
                        from
                            (
                                select
                                    pre_synd_item_dim_key
                                  , retailer_id
                                  , spike_tm_dim_key_day
                                from
                                    supply_chain_oos.oos_retailer_item_spike
                                where
                                    exec_tm_dim_key_day = {argument_parser.ws_spike_exec_tdk}
                                    and retailer_id in ({argument_parser.ws_retailer_id_list})
                            )
                            si
                            left outer join
                                (
                                    select
                                        pre_synd_item_dim_key
                                      , retailer_id
                                      , reset_tm_dim_key_day
                                    from
                                        supply_chain_oos.oos_retailer_item_reset
                                    where
                                        exec_tm_dim_key_day = {argument_parser.ws_reset_exec_tdk}
                                        and retailer_id in ({argument_parser.ws_retailer_id_list})
                                )
                                ri
                                on
                                    si.pre_synd_item_dim_key = ri.pre_synd_item_dim_key
                                    and si.retailer_id       = ri.retailer_id
                    )
                    spke
                    on
                        ghst.pre_synd_item_dim_key = spke.pre_synd_item_dim_key
                        and ghst.retailer_id       = spke.retailer_id
                left outer join
                    (
                        select
                            ss.subcategory_key
                          , ss.retailer_id
                          , ss.spike_tm_dim_key_day
                          , rs.reset_tm_dim_key_day
                          , case
                                when {argument_parser.ws_exec_tdk} - nvl(rs.reset_tm_dim_key_day, {argument_parser.ws_exec_tdk}) < {argument_parser.ws_spike_duration_after_reset}
                                    then -1
                                    else ({argument_parser.ws_exec_tdk} - nvl(rs.reset_tm_dim_key_day, {argument_parser.ws_exec_tdk}) - {argument_parser.ws_spike_duration_after_reset}) + {argument_parser.ws_min_gap_length}
                            end as spike_gap_threshold
                        from
                            (
                                select
                                    subcategory_key
                                  , retailer_id
                                  , spike_tm_dim_key_day
                                from
                                    supply_chain_oos.oos_retailer_subcategory_spike
                                where
                                    exec_tm_dim_key_day = {argument_parser.ws_spike_exec_tdk}
                                    and retailer_id in ({argument_parser.ws_retailer_id_list})
                            )
                            ss
                            left outer join
                                (
                                    select
                                        subcategory_key
                                      , retailer_id
                                      , reset_tm_dim_key_day
                                    from
                                        supply_chain_oos.oos_retailer_subcategory_reset
                                    where
                                        exec_tm_dim_key_day = {argument_parser.ws_reset_exec_tdk}
                                        and retailer_id in ({argument_parser.ws_retailer_id_list})
                                )
                                rs
                                on
                                    ss.subcategory_key = rs.subcategory_key
                                    and ss.retailer_id = rs.retailer_id
                    )
                    sspk
                    on
                        itmd.s_21860_key     = sspk.subcategory_key
                        and ghst.retailer_id = sspk.retailer_id
        )
        thld
        on
            gaps.pre_synd_item_dim_key = thld.pre_synd_item_dim_key
            and gaps.retailer_id       = thld.retailer_id

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