"""
Databricks job: LARGE_ENTERPRISE_RPT
Migrated from Talend job on 2026-10-07
Components: 41 nodes, 36 connections
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from connections import get_config
from transformations import run_transformations
from sql_statements import run_sql_statements


def main():
    """Main job entry point."""
    spark = SparkSession.builder.appName(
        "LARGE_ENTERPRISE_RPT"
    ).getOrCreate()

    config = get_config(spark)

    # ── Pre-job Phase ──
    # === PRE-JOB PHASE: tPrejob_1 ===

    # tJava_1: Custom Java code → Python equivalent
    # Original Java:
    # System.out.println("path :- " + context.context_filepath);
    
    # TODO: Manual translation required for custom Java logic
    print("tJava_1: Custom logic placeholder")
    

    # GetConnDetails: GetConnDetails (custom/generic component)
    # TODO: Manual translation needed

    try:
        # ── SQL Statements ──
        run_sql_statements(spark, config)

        # ── Transformations ──
        run_transformations(spark, config)

        # ── Child Jobs / Control ──
        # __PROCESS__: Run child job (tRunJob → Databricks task reference)
        # Original Talend job: ECOMM:_bWyPsKMWEemf4KS34WPbDA (version: Latest)
        # In Databricks, this becomes a task dependency in the workflow
        dbutils.notebook.run("ECOMM:_bWyPsKMWEemf4KS34WPbDA", timeout_seconds=3600, arguments={})
        

        # __PROCESS__: Run child job (tRunJob → Databricks task reference)
        # Original Talend job: ECOMM:_IjEH4IcOEeewRJv3HJpkUA (version: Latest)
        # In Databricks, this becomes a task dependency in the workflow
        dbutils.notebook.run("ECOMM:_IjEH4IcOEeewRJv3HJpkUA", timeout_seconds=3600, arguments={})
        

        # ── Notifications ──
        # SuccessMail: Send notification (tSendMail → webhook)
        # Original: to=context.tsendemail_to, subject=LE Global B2B Reporting Process Started
        # TODO: Replace with Databricks webhook or notification integration
        import requests
        webhook_url = dbutils.secrets.get(scope="notifications", key="webhook_url")
        to_addr = config.get("tsendemail_to", "config_tsendemail_to")
        subject_text = config.get("tsendemail_subject", "LE Global B2B Reporting Process Started")
        body_text = config.get("jobMailContent", "Job completed")
        payload = {
            "to": to_addr,
            "subject": subject_text,
            "body": body_text,
        }
        # requests.post(webhook_url, json=payload)
        print(f"NOTIFICATION: {payload['subject']}")
        

        # SuccessMail: Send notification (tSendMail → webhook)
        # Original: to=sgupta5@lenovo.com;kdmello@lenovo.com;asaavedra1@lenovo.com, subject=LE Global B2B Reporting Process Completed
        # TODO: Replace with Databricks webhook or notification integration
        import requests
        webhook_url = dbutils.secrets.get(scope="notifications", key="webhook_url")
        to_addr = config.get("tsendemail_to", "sgupta5@lenovo.com;kdmello@lenovo.com;asaavedra1@lenovo.com")
        subject_text = config.get("tsendemail_subject", "LE Global B2B Reporting Process Completed")
        body_text = config.get("jobMailContent", "Job completed")
        payload = {
            "to": to_addr,
            "subject": subject_text,
            "body": body_text,
        }
        # requests.post(webhook_url, json=payload)
        print(f"NOTIFICATION: {payload['subject']}")
        

    except Exception as e:
        print(f"Job failed: {e}")
        raise
    finally:
        # ── Post-job Phase ──
        print("Post-job cleanup")
        spark.stop()


if __name__ == "__main__":
    main()
