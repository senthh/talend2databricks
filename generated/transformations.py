"""PySpark transformations for the migrated Talend job."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


def run_transformations(spark: SparkSession, config: dict):
    """Execute all transformation steps."""

    # fact_le_order: Read from Redshift via Spark SQL
    tRedshiftInput_2_query = """
    SELECT order_stat,
           sales_doc_item,
           sales_org,
           dist_channel,
           sales_doc,
           sales_doc_prod_cd,
           order_dt,
           order_tm,
           billing_doc_item,
           billing_doc,
           billing_doc_prod_cd,
           billing_dt,
           ship_dt,
           route_cd,
           doc_type,
           sold_to_party,
           sold_to_party_name,
           gln,
           end_cust_num,
           order_category,
           order_category_name,
           route_name,
           account_name,
           business_name,
           country,
           segment,
           entity_type_1,
           account_manager,
           geo,
           entity_type_2,
           prod_cd,
           prod_desc,
           prod_family,
           prod_group,
           prod_class,
           sys_acc_flag,
           qty,
           rev_local,
           rev_usd,
           cost_local,
           cost_usd,
           margin_local,
           margin_usd,
           tax,
           freight,
           commissionable_cost,
           ship_day_of_week,
           ship_week,
           ship_fiscal_month,
           ship_fiscal_quarter,
           ship_quarter_month,
           ship_quarter_month_week,
           ship_quarter_week,
           ship_quarter_week_dayofweek,
           order_day_of_week,
           order_week,
           order_fiscal_month,
           order_fiscal_quarter,
           order_quarter_month,
           order_quarter_month_week,
           order_quarter_week,
           order_quarter_week_dayofweek,
           create_id,
           create_dttm,
           b2b,
    	   theoldkey1,
           attach_status,
           order_status,
           touchless_orders,card_type,b.prod_offer_type,
    created_by,
    Business,
    ZCONTRACT,
    us_state,
    ZMCN
    FROM fact_le_order_shipment a left join  product_final_master b on ltrim(a.prod_cd,'0')=b.prod_code ;
    
    
    """
    df_tRedshiftInput_2 = spark.sql(tRedshiftInput_2_query)
    

    # tMap_1: Multi-input transformation (tMap)
    from pyspark.sql import functions as F
    
    # Main input: row2
    df_joined = df_row2
    
    df_joined = df_joined.join(df_geo, F.col("row2__Country") == F.col("geo__Country"), "left")
    df_joined = df_joined.join(df_row3, F.col("row2__sold_to_party") == F.col("row3__sold_to_party"), "left")
    df_joined = df_joined.join(df_row4, F.col("row2__prod_code") == F.col("row4__prod_code"), "left")
    df_joined = df_joined.join(df_row5, F.col("row2__subsegment") == F.col("row5__subsegment"), "left")
    df_joined = df_joined.join(df_doc, F.col("row2__DOCTYPE") == F.col("doc__DOCTYPE"), "left")
    df_joined = df_joined.join(df_account_map, F.col("row2__sold_to_party_key") == F.col("account_map__sold_to_party_key"), "left")
    df_joined = df_joined.join(df_route, F.col("row2__business") == F.col("route__business") & F.col("row2__route_cd") == F.col("route__route_cd") & F.col("row2__doc_type") == F.col("route__doc_type") & F.col("row2__created_by") == F.col("route__created_by"), "left")
    
    # Output: tofile
    df_filtered = df_joined.filter((F.col("row2__sold_to_party") != "row3.sold_to_party"))
    
    df_tofile = df_filtered.select(
        F.col("row2__order_stat").alias("order_stat"),
        F.col("row2__sales_doc_item").alias("sales_doc_item"),
        F.col("row2__sales_org").alias("sales_org"),
        F.col("row2__dist_channel").alias("dist_channel"),
        F.col("row2__sales_doc").alias("sales_doc"),
        F.col("row2__sales_doc_prod_cd").alias("sales_doc_prod_cd"),
        F.col("row2__order_dt").alias("order_dt"),
        F.col("row2__order_tm").alias("order_tm"),
        F.col("row2__billing_doc_item").alias("billing_doc_item"),
        F.col("row2__billing_doc").alias("billing_doc"),
        F.col("row2__billing_doc_prod_cd").alias("billing_doc_prod_cd"),
        F.col("row2__billing_dt").alias("billing_dt"),
        F.col("row2__ship_dt").alias("ship_dt"),
        F.col("row2__route_cd").alias("route_cd"),
        F.col("row2__doc_type").alias("doc_type"),
        F.col("row2__sold_to_party").alias("sold_to_party"),
        F.col("row2__sold_to_party_name").alias("sold_to_party_name"),
        F.col("row2__gln").alias("gln"),
        F.col("row2__end_cust_num").alias("end_cust_num"),
        F.col("row2__order_category").alias("order_category"),
        F.col("row2__order_category_name").alias("order_category_name"),
        F.col("route__final_route").alias("route_name"),
        F.col("row2__account_name").alias("account_name"),
        F.col("row2__business_name").alias("business_name"),
        F.when(F.length(F.col("geo__country_name")) > F.lit(1), F.col("geo__country_name")).otherwise(F.col("row2__country")).alias("country"),
        F.col("account_map__segment").alias("segment"),
        F.col("account_map__sub_segment").alias("sub_segment"),
        F.col("account_map__sub_sub_segment").alias("sub_sub_segment"),
        F.col("row2__entity_type_1").alias("entity_type_1"),
        F.col("row2__account_manager").alias("account_manager"),
        F.when(F.length(F.col("geo__Geo")) > F.lit(1), F.col("geo__Geo")).otherwise(F.col("row2__geo")).alias("geo"),
        F.col("row2__entity_type_2").alias("entity_type_2"),
        F.when(F.length(F.trim(F.regexp_replace(F.col("row2__prod_cd"), "-", ""))) > F.lit(7), F.ltrim(F.trim(F.regexp_replace(F.col("row2__prod_cd"), "-", "")), "0")).otherwise(F.trim(F.regexp_replace(F.col("row2__prod_cd"), "-", ""))).alias("prod_cd"),
        F.col("row4__prod_description").alias("prod_desc"),
        F.col("row4__prod_type").alias("prod_family"),
        F.col("row4__prod_group_name").alias("prod_group"),
        F.col("row4__prod_type").alias("prod_class"),
        F.col("row2__sys_acc_flag").alias("sys_acc_flag"),
        F.col("row2__qty").alias("qty"),
        F.col("row2__rev_local").alias("rev_local"),
        F.col("row2__rev_usd").alias("rev_usd"),
        F.col("row2__cost_local").alias("cost_local"),
        F.col("row2__cost_usd").alias("cost_usd"),
        F.col("row2__margin_local").alias("margin_local"),
        F.col("row2__margin_usd").alias("margin_usd"),
        F.col("row2__tax").alias("tax"),
        F.col("row2__freight").alias("freight"),
        F.col("row2__commissionable_cost").alias("commissionable_cost"),
        F.col("row2__ship_day_of_week").alias("ship_day_of_week"),
        F.col("row2__ship_week").alias("ship_week"),
        F.col("row2__ship_fiscal_month").alias("ship_fiscal_month"),
        F.col("row2__ship_fiscal_quarter").alias("ship_fiscal_quarter"),
        F.col("row2__ship_quarter_month").alias("ship_quarter_month"),
        F.col("row2__ship_quarter_month_week").alias("ship_quarter_month_week"),
        F.col("row2__ship_quarter_week").alias("ship_quarter_week"),
        F.col("row2__ship_quarter_week_dayofweek").alias("ship_quarter_week_dayofweek"),
        F.col("row2__order_day_of_week").alias("order_day_of_week"),
        F.col("row2__order_week").alias("order_week"),
        F.col("row2__order_fiscal_month").alias("order_fiscal_month"),
        F.col("row2__order_fiscal_quarter").alias("order_fiscal_quarter"),
        F.col("row2__order_quarter_month").alias("order_quarter_month"),
        F.col("row2__order_quarter_month_week").alias("order_quarter_month_week"),
        F.col("row2__order_quarter_week").alias("order_quarter_week"),
        F.col("row2__order_quarter_week_dayofweek").alias("order_quarter_week_dayofweek"),
        F.col("row2__create_id").alias("create_id"),
        F.col("row2__create_dttm").alias("create_dttm"),
        F.col("row2__b2b").alias("b2b_1"),
        F.col("account_map__theoldkey1").alias("theoldkey1"),
        F.col("account_map__theoldkey2").alias("theoldkey2"),
        F.col("account_map__theoldkey3").alias("theoldkey3"),
        F.col("row2__attach_status").alias("attach_status"),
        F.when(((F.col("row2__order_status") == "Electronic")) | ((F.col("row2__order_status") == "ELECTRONIC")), F.lit("Electronic")).otherwise(F.lit("")).alias("order_status"),
        F.col("row2__touchless_orders").alias("touchless_orders"),
        F.col("row2__card_type").alias("card_type"),
        F.col("row2__prod_offer_type").alias("prod_offer_type"),
        F.col("row2__created_by").alias("created_by"),
        F.col("route__Source_Description").alias("created_by_description"),
        F.col("row2__Business").alias("Business"),
        F.col("row2__ZCONTRACT").alias("ZCONTRACT"),
        F.when(F.length(F.col("row2__us_state")) > F.lit(1), F.col("row2__us_state")).otherwise(F.col("account_map__us_state")).alias("us_state"),
        F.col("account_map__new_structure").alias("new_structure"),
        F.col("account_map__eproc").alias("eproc"),
        F.col("row2__ZMCN").alias("ZMCN"),
        F.col("account_map__epro_support_level").alias("epro_support_level"),
        F.col("route__Source_Description").alias("Source_Description"),
        F.col("route__final_route").alias("final_route"),
    )
    

    # __TABLE__: Read from MySQL (migrated to Spark SQL)
    tMysqlInput_1_query = """
    select prod_code,prod_group_name,prod_cust_segment ,prod_cat,prod_description,prod_type
    from Product_final_master 
    
    """
    df_tMysqlInput_1 = spark.sql(tMysqlInput_1_query)
    

    # d_account_map: Read from Redshift via Spark SQL
    tDBInput_1_query = """
    select distinct SOLD_TO_PARTY_KEY,theoldkey1,theoldkey2,theoldkey3,segment,entity_type  as sub_segment,
    entity_type_ii as sub_sub_segment, us_state,new_structure,eproc,epro_support_level
    from d_account_map;
    """
    df_tDBInput_1 = spark.sql(tDBInput_1_query)
    

    # ── File I/O ──
    # fact_le_order: Write delimited file
    df_output.coalesce(1).write.mode("overwrite").option(
        "header", true
    ).option("delimiter", "\t").csv("/Volumes/catalog/schema/volume/data/ecomm/Large_Enterprise/outgoing/LARGE_ENTERPRISE_fact_le_order_shipment.csv")
    

    # Country_Geo_Mapping: Read Excel file
    df_tFileInputExcel_1 = (
        spark.read.format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("dataAddress", "'Sheet1'!A1")
        .load("/Volumes/catalog/schema/volume/data/ecomm/Large_Enterprise/incoming/Country_Geo_Mapping.xlsx")
    )
    

    # Sole_to_party_remove: Read Excel file
    df_tFileInputExcel_2 = (
        spark.read.format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("dataAddress", "'Sheet1'!A1")
        .load("/Volumes/catalog/schema/volume/data/ecomm/Large_Enterprise/incoming/Sold tos to Remove.xlsx")
    )
    

    # Segment - Sub segment mapping file.xlsx: Read Excel file
    df_tFileInputExcel_3 = (
        spark.read.format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("dataAddress", "'Sheet1'!A1")
        .load("/Volumes/catalog/schema/volume/data/ecomm/Large_Enterprise/incoming/Segment - Sub segment mapping file.xlsx")
    )
    

    # LE_Doctype_dataSMC: Read Excel file
    df_tFileInputExcel_4 = (
        spark.read.format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("dataAddress", "'Sheet1'!A1")
        .load("/Volumes/catalog/schema/volume/data/ecomm/Large_Enterprise/incoming/LE_Doctype_data.xlsx")
    )
    

    # Route: Read Excel file
    df_tFileInputExcel_5 = (
        spark.read.format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("dataAddress", "'Sheet1'!A1")
        .load("/Volumes/catalog/schema/volume/data/ecomm/Large_Enterprise/incoming/Route Code mapping with doc type.xlsx")
    )
    

    # tS3Put_1: Upload to S3 (→ Unity Catalog Volume / cloud storage)
    # Original: s3:///
    source_path = "/Volumes/catalog/schema/volume/data"
    dest_path = "/Volumes/catalog/schema/volume/output"
    dbutils.fs.cp(source_path, dest_path)
    

