"""Spark SQL statements for the migrated Talend job."""

from pyspark.sql import SparkSession


SQL_STATEMENTS = [
    """DELETE 
FROM fact_le_order_shipment  using
           tmp_le_order_shipment 
WHERE 
fact_le_order_shipment.sales_doc_item = tmp_le_order_shipment.customer_so_header___item 
 AND  fact_le_order_shipment.billing_doc_item = tmp_le_order_shipment.customer_billing""",
    """DROP TABLE IF EXISTS fact_le_order_shipment_temp CASCADE""",
    """CREATE TABLE fact_le_order_shipment_temp
(
   order_stat                    varchar(200),
   sales_doc_item                varchar(555),
   sales_org                     varchar(555),
   dist_channel                  varchar(555),
   sales_doc                     varchar(555),
   sales_doc_prod_cd             varchar(500),
   order_dt                      date,
   order_tm                      varchar(6),
   billing_doc_item              varchar(555),
   billing_doc                   varchar(555),
   billing_doc_prod_cd           varchar(50),
   billing_dt                    date,
   ship_dt                       date,
   route_cd                      varchar(30),
   doc_type                      varchar(40),
   sold_to_party                 varchar(250),
   sold_to_party_name            varchar(555),
   gln                           varchar(220),
   end_cust_num                  varchar(220),
   order_category                varchar(250),
   order_category_name           varchar(250),
   route_name                    varchar(1000),
   account_name                  varchar(555),
   business_name                 varchar(555),
   country                       varchar(500),
   segment                       varchar(220),
   sub_segment                   varchar(220),
   sub_sub_segment                   varchar(220),
   entity_type_1                 varchar(520),
   account_manager               varchar(1000),
   geo                           varchar(1000),
   entity_type_2                 varchar(500),
   prod_cd                       varchar(200),
   prod_desc                     varchar(1000),
   prod_family                   varchar(1000),
   prod_group                    varchar(1000),
   prod_class                    varchar(200),
   sys_acc_flag                  varchar(1),
   qty                           integer,
   rev_local                     float8,
   rev_usd                       float8,
   cost_local                    float8,
   cost_usd                      float8,
   margin_local                  float8,
   margin_usd                    float8,
   tax                           float8,
   freight                       float8,
   commissionable_cost           float8,
   ship_day_of_week              varchar(220),
   ship_week                     integer,
   ship_fiscal_month             varchar(220),
   ship_fiscal_quarter           varchar(100),
   ship_quarter_month            varchar(20),
   ship_quarter_month_week       varchar(50),
   ship_quarter_week             varchar(20),
   ship_quarter_week_dayofweek   varchar(50),
   order_day_of_week             varchar(20),
   order_week                    integer,
   order_fiscal_month            varchar(20),
   order_fiscal_quarter          varchar(10),
   order_quarter_month           varchar(20),
   order_quarter_month_week      varchar(50),
   order_quarter_week            varchar(20),
   order_quarter_week_dayofweek  varchar(50),
   create_id                     varchar(20),
   create_dttm                   varchar(100),
   b2b                           varchar(1000),
   theoldkey1                    varchar(555),
 theoldkey2                   varchar(555),
 theoldkey3                    varchar(555),
   attach_status                 varchar(555),
   order_status                  varchar(555),
   touchless_orders              varchar(555),
   card_type                     varchar(555),
   prod_offer_type               varchar(500),
   created_by               varchar(500),
   created_by_description              varchar(500),
Business varchar(500),
ZCONTRACT varchar(999),
us_state varchar(500),
new_structure varchar(500),
eproc varchar(255),
ZMCN varchar(999),
epro_support_level  varchar(255),
Source_Description varchar(555),
final_route varchar(555)
)""",
    """copy fact_le_order_shipment_temp
(
       order_stat,
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
       sub_segment,
       sub_sub_segment,
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
	    theoldkey2,
	    theoldkey3,
       attach_status,
      order_status,
     touchless_orders,
card_type,
prod_offer_type,
created_by,
created_by_description,
Business,
ZCONTRACT,
us_state,
new_structure,
eproc,
ZMCN,
epro_support_level,
Source_Description ,
final_route
)
from 's3://' + config['s3_bucket_luci_redshift'] + '/LARGE_ENTERPRISE_fact_le_order_shipment.csv' 
access_key_id '' + config['s3_access_key_luci_redshift'] + '' secret_access_key '' + config['s3_secret_key_luci_redshift'] + ''
DELIMITER '\\t' csv IGNOREHEADER 1  BLANKSASNULL EMPTYASNULL IGNOREBLANKLINES MAXERROR 1""",
    """UPDATE fact_le_order_shipment
SET ACCOUNT_NAME  = DAM.ACCOUNT_NAME 
, SOLD_TO_PARTY_NAME = DAM.ACCOUNT_NAME
, BUSINESS_NAME = DAM.BUSINESS_NAME
, SEGMENT= DAM.SEGMENT
, COUNTRY = DAM.COUNTRY
, ENTITY_TYPE_1 = DAM.ENTITY_TYPE 
, GEO = DAM.GEO
, B2B = DAM.B2B
, ACCOUNT_MANAGER = DAM.ACCOUNT_MANAGER
, THEOLDKEY1 = DAM.THEOLDKEY1
,ORDER_STATUS = DAM.ORDER_STATUS
, US_STATE = DAM.US_STATE
FROM D_ACCOUNT_MAP DAM WHERE UPPER(DAM.sold_to_party_key) = UPPER(sold_to_party)""",
    """UPDATE fact_le_order_shipment
SET ROUTE_NAME = D_ROUTE.ROUTE_NAME
FROM fact_le_order_shipment, D_ROUTE
WHERE fact_le_order_shipment.ROUTE_CD = D_ROUTE.REF_SAP_ORDER_CATEGORY
AND fact_le_order_shipment.DOC_TYPE = D_ROUTE.REF_SAP_ORDER_TYPE
AND (B2B_FLAG is null or B2B_FLAG = '' or B2B_FLAG = 'NO')""",
    """UPDATE fact_le_order_shipment
SET ROUTE_NAME = D_ROUTE.ROUTE_NAME
FROM fact_le_order_shipment, D_ACCOUNT_MAP, D_ROUTE
WHERE fact_le_order_shipment.ROUTE_CD = D_ROUTE.REF_SAP_ORDER_CATEGORY
AND fact_le_order_shipment.DOC_TYPE = D_ROUTE.REF_SAP_ORDER_TYPE
AND UPPER(D_ACCOUNT_MAP.sold_to_party_key) = UPPER(fact_le_order_shipment.sold_to_party)
AND D_ROUTE.B2B_FLAG = D_ACCOUNT_MAP.B2B AND B2B_FLAG != ''""",
    """update fact_le_order_shipment
set sold_to_party_name = account_name
where sold_to_party_name ~ '[0-9]' and sold_to_party_name !~ ' '""",
    """update fact_le_order_shipment
set entity_type_2 = ''""",
    """update fact_le_order_shipment
set entity_type_2 = 'NON WEB'
where entity_type_1 = 'NON WEB'""",
    """update fact_le_order_shipment
set entity_type_2 = 'Manual'
where route_name in ('Manual','Mass Upload')
and entity_type_1 not in ('NON WEB')""",
    """update fact_le_order_shipment
set entity_type_2 = 'Web'
where entity_type_2 = '' or entity_type_2 is null""",
    """update fact_le_order_shipment
set cost_usd = 0,
margin_usd = 0
where rev_usd = 0 and cost_usd > 0
and sales_doc in (
select distinct sales_doc from fact_le_order_shipment where prod_cd like 'XXXX%' )""",
    """update fact_le_order_shipment
set sold_to_party = a.zsoldtopt
from (select zsale_itm, salesorg, zsoldtopt from le_ludp_row_mds38) a
where fact_le_order_shipment.sales_doc_item = a.zsale_itm
and fact_le_order_shipment.sales_org = a.salesorg
and fact_le_order_shipment.sales_org in ('US10','CA10','CA20','US31')""",
    """UPDATE fact_le_order_shipment
SET ACCOUNT_NAME  = DAM.ACCOUNT_NAME 
, SOLD_TO_PARTY_NAME = DAM.ACCOUNT_NAME
, BUSINESS_NAME = DAM.BUSINESS_NAME
, SEGMENT= DAM.SEGMENT
, COUNTRY = DAM.COUNTRY
, ENTITY_TYPE_1 = DAM.ENTITY_TYPE 
, GEO = DAM.GEO
, B2B = DAM.B2B
, ACCOUNT_MANAGER = DAM.ACCOUNT_MANAGER
, THEOLDKEY1 = DAM.THEOLDKEY1
,ORDER_STATUS = DAM.ORDER_STATUS
, US_STATE = DAM.US_STATE
FROM D_ACCOUNT_MAP DAM WHERE UPPER(DAM.sold_to_party_key) = UPPER(sold_to_party)""",
    """insert into fact_le_order_shipment(
order_stat,
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
order_status,
touchless_orders,
card_type,
created_by,
Business,
ZCONTRACT,
ZMCN
)
SELECT DISTINCT (CASE WHEN DAM.B2B = 'YES' THEN 'WEB ASSIST' ELSE (CASE
                     WHEN UPPER(CLS.CUSTOMER_SO_H___I_SALES_DOCUMENT__KEY_) = UPPER(OOM.ORDER_NUMBER) THEN 'ONLINE ORDER'
                     ELSE 'OFFLINE ORDER'
                 END) END) AS ORDER_STAT ,
                CLS.CUSTOMER_SO_HEADER___ITEM AS SALES_DOC_ITEM ,
                CLS.CUSTOMER_SO_H___I_SALES_ORGANIZATION__KEY_ AS SALES_ORG ,
                CLS.CUSTOMER_SO_H___I_DISTRIBUTION_CHANNEL__KEY_ AS DIST_CHANNEL ,
                UPPER(CLS.CUSTOMER_SO_H___I_SALES_DOCUMENT__KEY_) AS SALES_DOC ,
                CONCAT(CLS.CUSTOMER_SO_H___I_SALES_DOCUMENT__KEY_ ,MATERIAL_MASTER) AS SALES_DOC_PROD_CD ,
                CLS.CUSTOMER_SO_H___I_CREATE_DATE_LINE__KEY_ AS ORDER_DT ,
                CLS.CUSTOMER_SO_H___I_CREATE_TIME_LINE__KEY_ AS ORDER_TM ,
                CLS.CUSTOMER_BILLING AS BILLING_DOC_ITEM ,
                CLS.CUSTOMER_BILLING_BILLING_DOCUMENT__KEY_ AS BILLING_DOC ,
                CONCAT(CLS.CUSTOMER_BILLING_BILLING_DOCUMENT__KEY_,MATERIAL_MASTER) AS BILLING_DOC_PROD_CD ,
                CLS.CUSTOMER_BILLING_BILLING_DOC__DATE__KEY_ AS BILLING_DT ,
                CLS.SHIP_DATE ,
                CLS.CUSTOMER_SO_H___I_CUSTOMER_ORDER_METHOD__KEY_ AS ROUTE_CD ,
                CLS.CUSTOMER_SO_H___I_DOC_TYPE_FOR_CUSTOME__KEY_ AS DOC_TYPE ,
                UPPER(CLS.SOLD_TO_SOLD_TO_PARTY__KEY_) AS SOLD_TO_PARTY ,
                DAM.ACCOUNT_NAME AS SOLD_TO_PARTY_NAME ,
                CLS.GLN ,
                CLS.END_CUSTOMER_NUMBER_ILN__KEY_ AS END_CUST_NUM ,
                NULL AS ORDER_CATEGORY ,
                NULL AS ORDER_CATEGORY_NAME ,
                NULL AS ROUTE_NAME ,
                DAM.ACCOUNT_NAME ,
                DAM.BUSINESS_NAME ,
                DAM.COUNTRY ,
                (CASE WHEN (DAM.SEGMENT = '' or DAM.SEGMENT is null) THEN 'Unclassified' ELSE DAM.SEGMENT END)  AS SEGMENT ,
                (CASE WHEN DAM.ENTITY_TYPE = '' THEN 'Unclassified' ELSE DAM.ENTITY_TYPE END)  AS ENTITY_TYPE_1 ,
                                DAM.ACCOUNT_MANAGER ,
                (CASE WHEN (DAM.GEO = '' or DAM.GEO is null ) THEN 'UNCLASSIFIED' ELSE DAM.GEO END)  AS GEO ,
                 (CASE WHEN DAM.ENTITY_TYPE_II = '' THEN 'Unclassified' ELSE DAM.ENTITY_TYPE_II END)  AS ENTITY_TYPE_2 ,
                CLS.MATERIAL_MASTER AS PROD_CD ,
                CLS.MATERIAL_MASTER_MEDIUM_NAME AS PROD_DESC ,
                CLS.MATERIAL_MASTER_SUB_SERIES__NAME_ AS PROD_FAMILY ,
                CLS.MATERIAL_MASTER_PRODUCT_GROUP__NAME_ AS PROD_GROUP ,
                DPD.PROD_CLASS ,
                ATTCH.SYS_ACC_FLAG ,
                (CLS.INVOICE_QUANTITY_ON_CUSTOMER) AS QTY ,
                (CLS.net_value_on_customer * CLS.EXCHANGE_RATE ) AS REV_LOCAL ,
                (CLS.net_value_on_customer ) AS REV_USD ,
                (CLS.CUSTOMER_BILLING_COST_IN_DOC_CURRENCY) AS COST_LOCAL ,
                (CLS.CUSTOMER_BILLING_COST_IN_DOC_CURRENCY * CLS.EXCHANGE_RATE) AS COST_USD ,
                (CLS.net_value_on_customer * CLS.EXCHANGE_RATE  - CLS.CUSTOMER_BILLING_COST_IN_DOC_CURRENCY) AS MARGIN_LOCAL ,
                ((CLS.net_value_on_customer) - (CLS.CUSTOMER_BILLING_COST_IN_DOC_CURRENCY * CLS.EXCHANGE_RATE)) AS MARGIN_USD ,
                CLS.TAX_AMOUNT AS TAX ,
                CLS.FREIGHT_AMOUNT AS FREIGHT ,
                CLS.COMMISSIONAL_COST AS COMMISSIONABLE_COST ,
                DFC_SHP.DAY_OF_WEEK AS SHIP_DAY_OF_WEEK ,
                DFC_SHP.WEEK AS SHIP_WEEK ,
                DFC_SHP.FISCAL_MONTH AS SHIP_FISCAL_MONTH ,
                DFC_SHP.FISCAL_QUARTER AS SHIP_FISCAL_QUARTER ,
                DFC_SHP.QUARTER_MONTH AS SHIP_QUARTER_MONTH ,
                DFC_SHP.QUARTER_MONTH_WEEK AS SHIP_QUARTER_MONTH_WEEK ,
                DFC_SHP.QUARTER_WEEK AS SHIP_QUARTER_WEEK ,
                DFC_SHP.QUARTER_WEEK_DAYOFWEEK AS SHIP_QUARTER_WEEK_DAYOFWEEK ,
                DFC_ORD.DAY_OF_WEEK AS ORDER_DAY_OF_WEEK ,
                DFC_ORD.WEEK AS ORDER_WEEK ,
                DFC_ORD.FISCAL_MONTH AS ORDER_FISCAL_MONTH ,
                DFC_ORD.FISCAL_QUARTER AS ORDER_FISCAL_QUARTER ,
                DFC_ORD.QUARTER_MONTH AS ORDER_QUARTER_MONTH ,
                DFC_ORD.QUARTER_MONTH_WEEK AS ORDER_QUARTER_MONTH_WEEK ,
                DFC_ORD.QUARTER_WEEK AS ORDER_QUARTER_WEEK ,
				                DFC_ORD.QUARTER_WEEK_DAYOFWEEK AS ORDER_QUARTER_WEEK_DAYOFWEEK,
								'dm_load' as create_id,
				getdate() as create_dttm,
				DAM.B2B ,
				DAM.THEOLDKEY1,
				DAM.ORDER_STATUS,
				(case when (CLS.CUSTOMER_SO_H___I_CUSTOMER_ORDER_METHOD__KEY_='' or CLS.CUSTOMER_SO_H___I_CUSTOMER_ORDER_METHOD__KEY_ is null) then null
				when CLS.CUSTOMER_SO_H___I_CUSTOMER_ORDER_METHOD__KEY_ in('ZI5','ZIA','ZO1','ZO2','ZO3','ZO4','ZO5') then 'Touchless Orders' else 
				'Non Touchless Orders' end) as touchless_orders,
				CLS.customer_so_h___i_card_type__key_ as card_type,
				created_by,
                Business,
				ZCONTRACT,
                ZMCN
				
FROM CLS_LE_ORDER_SHIPMENT CLS
LEFT JOIN STG_LE_ONLINE_ORDER_MAPPING OOM ON CLS.CUSTOMER_SO_H___I_SALES_DOCUMENT__KEY_ = OOM.ORDER_NUMBER
LEFT JOIN D_ACCOUNT_MAP DAM ON CLS.SOLD_TO_SOLD_TO_PARTY__KEY_ = DAM.SOLD_TO_PARTY_KEY
LEFT JOIN (select distinct ref_sap_order_category, ref_sap_order_type, distribution_channel from d_route)  DRT ON CLS.CUSTOMER_SO_H___I_CUSTOMER_ORDER_METHOD__KEY_ = DRT.REF_SAP_ORDER_CATEGORY
AND CLS.CUSTOMER_SO_H___I_DOC_TYPE_FOR_CUSTOME__KEY_ = DRT.REF_SAP_ORDER_TYPE
LEFT JOIN D_FISCAL_CALENDAR DFC_SHP ON CLS.SHIP_DATE = DFC_SHP.DATE_KEY
LEFT JOIN D_FISCAL_CALENDAR DFC_ORD ON CLS.CUSTOMER_SO_H___I_CREATE_DATE_LINE__KEY_ = DFC_ORD.DATE_KEY
LEFT JOIN D_PRODUCT DPD ON CLS.MATERIAL_MASTER = DPD.PROD_CD AND DPD.ACTIVEFLAG = 'ACTIVE'
LEFT JOIN D_GEO GEO ON DAM.COUNTRY = GEO.COUNTRY
INNER JOIN TMP_LE_ORDER_SHIPMENT TMPOS 
ON CLS.customer_so_header___item = TMPOS.customer_so_header___item
AND cls.customer_billing = TMPOS.customer_billing
LEFT JOIN
  ( SELECT CUSTOMER_SO_H___I_SALES_DOCUMENT__KEY_ ORDER_NUMBER, SUM((CASE WHEN DPD.PROD_CLASS = 'System' THEN INVOICE_QUANTITY_ON_CUSTOMER ELSE 0 END)) SYS_QTY, SUM((CASE WHEN DPD.PROD_CLASS = 'Accessory' THEN INVOICE_QUANTITY_ON_CUSTOMER ELSE 0 END)) ACC_QTY, (CASE WHEN SUM((CASE WHEN DPD.PROD_CLASS = 'System' THEN INVOICE_QUANTITY_ON_CUSTOMER ELSE 0 END)) > 0
AND SUM((CASE WHEN DPD.PROD_CLASS = 'Accessory' THEN INVOICE_QUANTITY_ON_CUSTOMER ELSE 0 END)) > 0 THEN 'Y' ELSE 'N' END) SYS_ACC_FLAG
   FROM CLS_LE_ORDER_SHIPMENT CLS
   LEFT JOIN D_PRODUCT DPD ON CLS.MATERIAL_MASTER = DPD.PROD_CD
WHERE CUSTOMER_SO_H___I_SALES_DOCUMENT__KEY_ IS NOT NULL
   GROUP BY CUSTOMER_SO_H___I_SALES_DOCUMENT__KEY_ ) ATTCH 
   ON CLS.CUSTOMER_SO_H___I_SALES_DOCUMENT__KEY_ = ATTCH.ORDER_NUMBER""",
    """update  fact_le_order_shipment
set cost_usd = (case when ((rev_usd-cost_usd)/rev_usd) < -0.2 then 0 else cost_usd end)
where ((rev_usd > 0 or rev_usd < 0) and rev_usd is not null) and ((cost_usd > 0 or cost_usd < 0) and cost_usd is not null)
and create_dttm = '"+TalendDate.formatDate("yyyy-MM-dd", TalendDate.addDate(TalendDate.getCurrentDate(), - 0, "dd"))+"'""",
    """update  fact_le_order_shipment SET business = 'IDG' where sales_doc = '4602959612'""",
    """update  fact_le_order_shipment SET business = 'IDG' where sales_doc = '4330968167'""",
    """update  fact_le_order_shipment SET business = 'IDG' where sales_doc = '4330707120'""",
    """update fact_le_order_shipment SET geo = 'Europe-META' where geo = 'EMEA'""",
    """DROP TABLE IF EXISTS fact_le_order_shipment_old""",
    """ALTER TABLE fact_le_order_shipment RENAME TO fact_le_order_shipment_old""",
    """ALTER TABLE fact_le_order_shipment_temp RENAME TO fact_le_order_shipment""",
    """GRANT SELECT ON  fact_le_order_shipment TO GROUP blueocean_readonly""",
    """GRANT SELECT ON  fact_le_order_shipment_old TO GROUP blueocean_readonly""",
    """grant select on fact_le_order_shipment  to metric1""",
    """INSERT INTO D_ACCOUNT_MAP
(sold_to_party_key, country, create_id,ship_date)
SELECT DISTINCT SOLD_TO_SOLD_TO_PARTY__KEY_,
customer_so_h___i_sales_organization__key_,'DM_LOAD',ship_date
FROM TMP_LE_ORDER_SHIPMENT LOS
WHERE SOLD_TO_SOLD_TO_PARTY__KEY_ NOT IN (SELECT SOLD_TO_PARTY_KEY FROM D_ACCOUNT_MAP)""",
    """update D_ACCOUNT_MAP
set country = g.country,
geo = g.region
from d_geo g
where left(d_account_map.country,2) = g.country_cd
and (account_name is null or account_name = '')""",
    """update D_ACCOUNT_MAP
set account_name = 'UNCLASSIFIED',
business_name = 'UNCLASSIFIED',
segment ='UNCLASSIFIED',
entity_type ='UNCLASSIFIED',
account_manager = 'UNCLASSIFIED',
entity_type_ii = 'UNCLASSIFIED',
b2b = 'NO', 
theoldkey1 = 'UNCLASSIFIED'
where (account_name is null or account_name = '')""",
    """UPDATE D_ACCOUNT_MAP
SET ENTITY_TYPE_II = 'Not Applicable'
WHERE SEGMENT = 'GLOBAL'
AND ENTITY_TYPE = 'FOCUS'""",
]


def run_sql_statements(spark: SparkSession, config: dict):
    """Execute all SQL statements in order."""
    for i, stmt in enumerate(SQL_STATEMENTS):
        # Substitute config variables
        resolved = stmt
        for key, val in config.items():
            resolved = resolved.replace(f"${{config.{key}}}", str(val))
        print(f"Executing SQL statement {i + 1}/{len(SQL_STATEMENTS)}")
        spark.sql(resolved)
        print(f"  ✓ Statement {i + 1} complete")
