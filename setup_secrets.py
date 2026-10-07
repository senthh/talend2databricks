# Databricks notebook — run once to set up all secrets
# Scope: talend-migration
# Keys:  14 (deduplicated from 1 .item files)

# Step 1: Create scope (idempotent)
try:
    dbutils.secrets.createScope("talend-migration")
    print("Created scope: talend-migration")
except Exception as e:
    print(f"Scope may already exist: {e}")

# Step 2: Populate secrets — replace REPLACE_ME with actual values
secrets = {
    "FTPPassword": "REPLACE_ME",  # used by: LARGE_ENTERPRISE_RPT
    "flexpcebg_password": "REPLACE_ME",  # used by: LARGE_ENTERPRISE_RPT
    "ftp_password_omniture": "REPLACE_ME",  # used by: LARGE_ENTERPRISE_RPT
    "luciskydata_password": "REPLACE_ME",  # used by: LARGE_ENTERPRISE_RPT
    "luciskydev_password": "REPLACE_ME",  # used by: LARGE_ENTERPRISE_RPT
    "luciskyrds_password": "REPLACE_ME",  # used by: LARGE_ENTERPRISE_RPT
    "luciskystg_password": "REPLACE_ME",  # used by: LARGE_ENTERPRISE_RPT
    "luciskystg_prod_password": "REPLACE_ME",  # used by: LARGE_ENTERPRISE_RPT
    "mailpassword": "REPLACE_ME",  # used by: LARGE_ENTERPRISE_RPT
    "password": "REPLACE_ME",  # used by: LARGE_ENTERPRISE_RPT
    "s3_access_key_luci_redshift": "REPLACE_ME",  # used by: LARGE_ENTERPRISE_RPT
    "s3_secret_key_luci_redshift": "REPLACE_ME",  # used by: LARGE_ENTERPRISE_RPT
    "ssh_password": "REPLACE_ME",  # used by: LARGE_ENTERPRISE_RPT
    "tsendemail_password": "REPLACE_ME",  # used by: LARGE_ENTERPRISE_RPT
}

for key, value in secrets.items():
    if value == "REPLACE_ME":
        print(f"  ⏭  {key} — skipped (still REPLACE_ME)")
        continue
    dbutils.secrets.put(scope="talend-migration", key=key, string_value=value)
    print(f"  ✓ {key}")

print(f"\nDone — check with: dbutils.secrets.list('talend-migration')")