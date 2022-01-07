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

spark = get_spark_session()
query = """select
    item_dim_key
  , venue_dim_key
  , vendor_dim_key
  , retailer_sku
  , store_num
  , dc_num
  , dc_venue_dim_key
  , transaction_date
  , upc
  , onsite_onhand_qty
  , offsite_onhand_qty
  , claims_onhand_qty
  , on_order_qty
  , unit_cost
  , unit_sell
  , status string
  , inv_status_dim_key
  , vendor_pack_cost
  , item_on_shelf_date
  , item_off_shelf_dt
  , markdown_status_cd
  , vendor_num
  , vendor_num_raw
  , mds_fam_id
  , onsite_onhand_ext_cost
  , offsite_onhand_ext_cost
  , on_order_ext_cost
  , vendor_nbr_long
  , item_loc_nbr
  , avg_wk_tot_onhand_qty
  , item_status_dim_key
  , item_status
  , item_effective_date_dim_key
  , item_effective_date
  , item_no_of_eff_days
  , item_out_of_stock_date_dim_key
  , item_out_of_stock_date
  , item_no_of_oos_days
  , phase_in_out_qty
  , phase_in_out_retailer_sku
  , phase_in_out_type_dim_key
  , phase_in_out_type
  , unit_qty
  , file_date
  , file_id
  , tm_dim_key
from
    (
        select *
          , row_number() over (partition by retailer_sku, store_num, tm_dim_key order by
                               file_id) as rnm
        from
            sams_supply_chain.sams_inv_sales_fact
    )
    x
where
    x.rnm = 1"""
print(query)
print("Creating DF")
df = spark.sql(query)
print("writing data into hdfs")
df.write.mode('append').parquet("/sams_supply_chain/data/db/sams_supply_chain.db/managed/sams_inv_sales_fact_uniq_temp/")
print("Completed")

spark.stop()