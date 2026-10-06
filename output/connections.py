"""Connection and configuration management for Databricks."""


def get_config(spark=None) -> dict:
    """Load job configuration with secret scope references.

    Credentials are loaded from Databricks secret scopes.
    Config values come from job parameters or widgets.
    """
    config = {}

    # ── Secret scope credentials ──
    # Replace hardcoded credentials with Databricks secrets
    config["mailpassword"] = dbutils.secrets.get(scope="talend-migration", key="mailpassword")
    config["FTPPassword"] = dbutils.secrets.get(scope="talend-migration", key="FTPPassword")
    config["password"] = dbutils.secrets.get(scope="talend-migration", key="password")
    config["flexpcebg_password"] = dbutils.secrets.get(scope="talend-migration", key="flexpcebg_password")
    config["ftp_password_omniture"] = dbutils.secrets.get(scope="talend-migration", key="ftp_password_omniture")
    config["luciskydata_password"] = dbutils.secrets.get(scope="talend-migration", key="luciskydata_password")
    config["luciskydev_password"] = dbutils.secrets.get(scope="talend-migration", key="luciskydev_password")
    config["luciskyrds_password"] = dbutils.secrets.get(scope="talend-migration", key="luciskyrds_password")
    config["luciskystg_password"] = dbutils.secrets.get(scope="talend-migration", key="luciskystg_password")
    config["luciskystg_prod_password"] = dbutils.secrets.get(scope="talend-migration", key="luciskystg_prod_password")
    config["s3_access_key_luci_redshift"] = dbutils.secrets.get(scope="talend-migration", key="s3_access_key_luci_redshift")
    config["s3_secret_key_luci_redshift"] = dbutils.secrets.get(scope="talend-migration", key="s3_secret_key_luci_redshift")
    config["ssh_password"] = dbutils.secrets.get(scope="talend-migration", key="ssh_password")
    config["tsendemail_password"] = dbutils.secrets.get(scope="talend-migration", key="tsendemail_password")

    # ── Configuration parameters ──
    config["datecnt"] = 0
    config["CCList"] = "pkumar8@lenovo.com"
    config["FromList"] = "jitendra.d@buleoceanmi.com"
    config["mailusername"] = "jitendra.d@buleoceanmi.com"
    config["SMTPHost"] = "pod51012.outlook.com"
    config["SMTPPort"] = "587"
    config["ToList"] = "pkumar8@lenovo.com"
    config["csvTemplate"] = "/talend/data/large_enterprise/templates/"
    config["csvTemplateImp"] = "/talend/data/large_enterprise/templates/"
    config["days_minus"] = 00
    config["FileCount"] = 0
    config["message"] = ""
    config["rawSAPFileName"] = ""
    config["rawSAPFilePath"] = "/talend/data/large_enterprise/SAP/raw/"
    config["rawSCAccountMapFileName"] = ""
    config["rawSCFilePath"] = "/talend/data/large_enterprise/site_cat/raw/"
    config["rawSCOrderMapFileName"] = ""
    config["rawSCTrafficFileName"] = ""
    config["records"] = 0
    config["sap_days_minus"] = 00
    config["sourceSAPFilePath"] = "/talend/data/large_enterprise/SAP/incoming/"
    config["sourceSAPFilePathImp"] = "/talend/data/large_enterprise/SAP/incoming/"
    config["sourceSCFilePath"] = "/talend/data/large_enterprise/site_cat/incoming/"
    config["sourceSCFilePathImp"] = "/talend/data/large_enterprise/site_cat/incoming/"
    config["FTPHost"] = "ftp.gbi-lenovo.com"
    config["FTPPort"] = "21"
    config["FTPSAPDir"] = "/incoming/"
    config["FTPSCDir"] = "/incoming/LE/"
    config["FTPUsername"] = "omniture_dw"
    config["db"] = "lucicloud"
    config["host"] = "lucicloud.csgh3y4ndgau.us-east-1.redshift.amazonaws.com"
    config["port"] = "5439"
    config["username"] = "luci_master"
    config["jobMailContent"] = ""
    config["propertiesFile"] = "Large_Enterprise_Properties_file.json"
    config["propertiesFilePath"] = "/talend/data/ecomm/Large_Enterprise/Properties_File/"
    config["homedir"] = ""
    config["printOperations"] = True
    config["context_filepath"] = "/talend/data/ecomm/master/context_file/glcontext_dev.txt"
    config["ecomm_directory"] = "/talend/data/ecomm/"
    config["flexpcebg_database"] = ""
    config["flexpcebg_host"] = ""
    config["flexpcebg_port"] = ""
    config["flexpcebg_schema"] = ""
    config["flexpcebg_username"] = ""
    config["ftp_host_omniture"] = ""
    config["ftp_local_directory_omniture"] = ""
    config["ftp_port_omniture"] = 0
    config["ftp_remote_directory_omniture"] = ""
    config["ftp_username_omniture"] = ""
    config["logfile_path"] = ""
    config["luciskydata_additional_params"] = ""
    config["luciskydata_database"] = ""
    config["luciskydata_host"] = ""
    config["luciskydata_port"] = ""
    config["luciskydata_schema"] = ""
    config["luciskydata_username"] = ""
    config["luciskydev_additional_params"] = ""
    config["luciskydev_database"] = ""
    config["luciskydev_host"] = ""
    config["luciskydev_port"] = ""
    config["luciskydev_schema"] = ""
    config["luciskydev_username"] = ""
    config["luciskyrds_additional_params"] = ""
    config["luciskyrds_database"] = ""
    config["luciskyrds_host"] = ""
    config["luciskyrds_port"] = ""
    config["luciskyrds_schema"] = ""
    config["luciskyrds_username"] = ""
    config["luciskystg_additional_params"] = ""
    config["luciskystg_database"] = ""
    config["luciskystg_host"] = ""
    config["luciskystg_port"] = ""
    config["luciskystg_prod_additional_params"] = ""
    config["luciskystg_prod_database"] = ""
    config["luciskystg_prod_host"] = ""
    config["luciskystg_prod_port"] = ""
    config["luciskystg_prod_schema"] = ""
    config["luciskystg_prod_username"] = ""
    config["luciskystg_schema"] = ""
    config["luciskystg_username"] = ""
    config["s3_bucket_luci_redshift"] = ""
    config["ssh_host"] = ""
    config["ssh_port"] = ""
    config["ssh_user"] = ""
    config["tsendemail_bcc"] = ""
    config["tsendemail_cc"] = ""
    config["tsendemail_from"] = ""
    config["tsendemail_message"] = ""
    config["tsendemail_sendername"] = ""
    config["tsendemail_smtphost"] = ""
    config["tsendemail_smtpport"] = ""
    config["tsendemail_subject"] = ""
    config["tsendemail_to"] = ""
    config["tsendemail_username"] = ""
    config["luciskyrds_Login"] = "lucisky_etl"
    config["luciskyrds_Server"] = "luciskyrds.gbi-lenovo.com"
    config["luciskyrds_AdditionalParams"] = "noDatetimeStringSync=true&characterEncoding=UTF-8&allowMultiQueries=true"

    return config
