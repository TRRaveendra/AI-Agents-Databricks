# Unity Catalog Volumes and External Locations

## Overview
Unity Catalog provides managed storage for unstructured data (files) through **Volumes** and secure access to external cloud storage through **External Locations**.

---

## Unity Catalog Volumes

### What are Volumes?
Volumes are Unity Catalog objects that provide governed access to non-tabular data (files). They support versioning, access control, and lineage tracking.

### Volume Types

#### 1. Managed Volumes
Databricks fully manages the storage lifecycle. Data is stored in the metastore's root storage location.

```sql
-- Create managed volume
CREATE VOLUME IF NOT EXISTS main.sales.customer_documents;

-- Upload files via Databricks UI or dbutils
```

**When to use**:
- Internal project data
- Data fully owned by Databricks
- No need to access data outside Databricks
- Want automatic lifecycle management

#### 2. External Volumes
Points to existing cloud storage. Data remains in your cloud account.

```sql
-- Create external volume (requires external location)
CREATE EXTERNAL VOLUME IF NOT EXISTS main.sales.customer_uploads
LOCATION 's3://my-bucket/customer-data/';

-- Or using an external location
CREATE EXTERNAL VOLUME main.sales.archive_data
LOCATION 'external-location-name';
```

**When to use**:
- Data shared across multiple systems
- Existing data lakes
- Need direct cloud storage access
- Compliance requires data in specific locations

### Working with Volumes

#### Python Access
```python
from pyspark.sql.functions import col

# List files in volume
files = dbutils.fs.ls("/Volumes/main/sales/customer_documents")

# Read file from volume
df = spark.read.json("/Volumes/main/sales/customer_documents/data.json")

# Write file to volume
df.write.mode("overwrite").parquet("/Volumes/main/sales/customer_documents/output/")

# Read all JSON files in volume
df = spark.read.json("/Volumes/main/sales/customer_documents/*.json")
```

#### SQL Access
```sql
-- List files (requires supported function)
LIST '/Volumes/main/sales/customer_documents/';

-- Read from volume in query
SELECT * FROM json.`/Volumes/main/sales/customer_documents/data.json`;

-- Create table from volume data
CREATE TABLE main.sales.customer_info AS
SELECT * FROM parquet.`/Volumes/main/sales/customer_documents/customers/`;
```

#### File Operations
```python
# Copy file
dbutils.fs.cp(
    "/Volumes/main/sales/uploads/file.csv",
    "/Volumes/main/sales/archive/file.csv"
)

# Move file
dbutils.fs.mv(
    "/Volumes/main/sales/uploads/file.csv",
    "/Volumes/main/sales/processed/file.csv"
)

# Delete file
dbutils.fs.rm("/Volumes/main/sales/uploads/file.csv")

# Delete directory recursively
dbutils.fs.rm("/Volumes/main/sales/uploads/old/", recurse=True)
```

### Volume Permissions

```sql
-- Grant read access
GRANT READ VOLUME ON VOLUME main.sales.customer_documents TO `data_analysts`;

-- Grant write access
GRANT WRITE VOLUME ON VOLUME main.sales.customer_documents TO `data_engineers`;

-- Grant all permissions
GRANT ALL PRIVILEGES ON VOLUME main.sales.customer_documents TO `admin_group`;

-- Show permissions
SHOW GRANTS ON VOLUME main.sales.customer_documents;
```

---

## External Locations

### What are External Locations?
External Locations provide governed access to cloud storage paths (S3, ADLS, GCS). They enable secure, centralized management of external storage credentials.

### Creating External Locations

#### Prerequisites
1. Storage credential (cloud service principal or IAM role)
2. Cloud storage path
3. METASTORE ADMIN or CREATE EXTERNAL LOCATION privilege

#### AWS S3 Example
```sql
-- Create storage credential (admin only)
CREATE STORAGE CREDENTIAL IF NOT EXISTS aws_s3_cred
WITH (AWS_IAM_ROLE = 'arn:aws:iam::123456789:role/databricks-s3-role');

-- Create external location
CREATE EXTERNAL LOCATION IF NOT EXISTS s3_data_lake
URL 's3://my-company-data-lake/raw/'
WITH (STORAGE CREDENTIAL aws_s3_cred)
COMMENT 'Company data lake - raw zone';
```

#### Azure ADLS Gen2 Example
```sql
-- Create storage credential
CREATE STORAGE CREDENTIAL IF NOT EXISTS azure_adls_cred
WITH (
    AZURE_SERVICE_PRINCIPAL_CLIENT_ID = '<client-id>',
    AZURE_SERVICE_PRINCIPAL_CLIENT_SECRET = '<secret>',
    AZURE_TENANT_ID = '<tenant-id>'
);

-- Create external location
CREATE EXTERNAL LOCATION IF NOT EXISTS adls_data_lake
URL 'abfss://container@storageaccount.dfs.core.windows.net/data/'
WITH (STORAGE CREDENTIAL azure_adls_cred);
```

#### GCP GCS Example
```sql
-- Create storage credential
CREATE STORAGE CREDENTIAL IF NOT EXISTS gcp_gcs_cred
WITH (GCP_SERVICE_ACCOUNT_EMAIL = 'service-account@project.iam.gserviceaccount.com');

-- Create external location
CREATE EXTERNAL LOCATION IF NOT EXISTS gcs_data_lake
URL 'gs://my-company-bucket/data/'
WITH (STORAGE CREDENTIAL gcp_gcs_cred);
```

### Using External Locations

#### Create External Tables
```sql
-- Create external table pointing to external location
CREATE TABLE IF NOT EXISTS main.bronze.customer_data
LOCATION 's3://my-company-data-lake/raw/customers/'
AS SELECT * FROM source_data;

-- Or use external location name
CREATE EXTERNAL VOLUME main.raw.landing
LOCATION 'adls_data_lake';
```

#### Read Data from External Location
```python
# Read from S3 using external location
df = spark.read.parquet("s3://my-company-data-lake/raw/customers/")

# Write to external location
df.write.mode("overwrite").parquet("s3://my-company-data-lake/processed/customers/")
```

### External Location Permissions

```sql
-- Grant read access
GRANT READ FILES ON EXTERNAL LOCATION s3_data_lake TO `data_analysts`;

-- Grant write access
GRANT WRITE FILES ON EXTERNAL LOCATION s3_data_lake TO `data_engineers`;

-- Grant all permissions
GRANT ALL PRIVILEGES ON EXTERNAL LOCATION s3_data_lake TO `admin_group`;

-- Show grants
SHOW GRANTS ON EXTERNAL LOCATION s3_data_lake;
```

---

## Best Practices

### Volume Best Practices

1. **Use Managed Volumes for internal data** - Simpler governance, automatic lifecycle
2. **Use External Volumes for shared data** - When data is accessed by multiple systems
3. **Organize volumes by business domain** - `main.finance.invoices`, `main.hr.documents`
4. **Set appropriate permissions** - Least privilege access
5. **Document volume purpose** - Use COMMENT to describe contents
6. **Use consistent naming** - `{catalog}.{schema}.{volume_name}`
7. **Archive old data** - Move to separate archive volumes or delete

### External Location Best Practices

1. **Limit external locations** - Create only what's necessary for security
2. **Use descriptive names** - `s3_raw_data_lake`, not `external_loc_1`
3. **Document the purpose** - Add COMMENT explaining what storage is accessed
4. **Separate by environment** - Different locations for dev/test/prod
5. **Regular access reviews** - Audit who has access to each location
6. **Use storage credentials** - Don't embed access keys in code
7. **Apply cloud storage policies** - Lifecycle rules, versioning, encryption at rest

### Migration from Legacy DBFS Paths

```python
# OLD: Legacy DBFS path (not governed)
df = spark.read.parquet("/mnt/data-lake/customers/")

# NEW: Unity Catalog Volume (governed, lineage tracked)
df = spark.read.parquet("/Volumes/main/bronze/landing/customers/")
```

---

## Common Patterns

### Pattern 1: Landing Zone with Volumes
```python
# Bronze ingestion from volume landing zone
landing_path = "/Volumes/main/bronze/landing/orders/"
archive_path = "/Volumes/main/bronze/archive/orders/"

# Read new files
df = (spark.read
    .format("json")
    .option("inferSchema", "true")
    .load(landing_path)
    .withColumn("_ingested_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

# Write to Delta table
df.write.mode("append").saveAsTable("main.bronze.raw_orders")

# Archive processed files
for file_info in dbutils.fs.ls(landing_path):
    if file_info.name.endswith(".json"):
        dbutils.fs.mv(
            file_info.path,
            f"{archive_path}{file_info.name}"
        )
```

### Pattern 2: Model Artifacts in Volumes
```python
# Save ML model artifacts to volume
import mlflow
import pandas as pd

model_volume = "/Volumes/main/ml/model_artifacts/customer_churn_v1/"

# Save model
mlflow.sklearn.save_model(model, f"{model_volume}model")

# Save feature names
pd.DataFrame({"feature": feature_names}).to_parquet(f"{model_volume}features.parquet")

# Save model metadata
with open(f"{model_volume}metadata.json", "w") as f:
    json.dump({"version": "1.0", "trained_date": "2024-01-15"}, f)
```

### Pattern 3: External Location for Data Lake
```sql
-- Setup external location for multi-zone data lake
CREATE EXTERNAL LOCATION bronze_zone
URL 's3://company-lake/bronze/'
WITH (STORAGE CREDENTIAL data_lake_cred);

CREATE EXTERNAL LOCATION silver_zone
URL 's3://company-lake/silver/'
WITH (STORAGE CREDENTIAL data_lake_cred);

CREATE EXTERNAL LOCATION gold_zone
URL 's3://company-lake/gold/'
WITH (STORAGE CREDENTIAL data_lake_cred);

-- Create external tables in each zone
CREATE TABLE main.bronze.raw_sales
LOCATION 's3://company-lake/bronze/sales/';

CREATE TABLE main.silver.clean_sales
LOCATION 's3://company-lake/silver/sales/';

CREATE TABLE main.gold.sales_summary
LOCATION 's3://company-lake/gold/sales/';
```

---

## Troubleshooting

### Volume Access Issues
```python
# Check if volume exists
try:
    dbutils.fs.ls("/Volumes/main/sales/customer_documents/")
    print("✓ Volume accessible")
except Exception as e:
    print(f"✗ Volume access error: {e}")
```

```sql
-- Verify volume exists
SHOW VOLUMES IN main.sales;

-- Check permissions
SHOW GRANTS ON VOLUME main.sales.customer_documents;
```

### External Location Access Issues
```sql
-- Verify external location exists
SHOW EXTERNAL LOCATIONS;

-- Check external location details
DESCRIBE EXTERNAL LOCATION s3_data_lake;

-- Test access
LIST 's3://my-company-data-lake/raw/';
```

---

## Checklist

### Volume Setup
✓ Volume created in appropriate catalog/schema  
✓ Volume type (managed vs external) chosen appropriately  
✓ Permissions granted to correct groups  
✓ Volume purpose documented (COMMENT)  
✓ File organization structure defined  
✓ Archive/cleanup strategy in place

### External Location Setup
✓ Storage credential created and tested  
✓ External location created with correct URL  
✓ Permissions granted appropriately  
✓ External location documented (COMMENT)  
✓ Cloud storage policies configured (lifecycle, encryption)  
✓ Access regularly audited  
✓ Separate locations for dev/test/prod

---

**Remember**: Volumes provide governed, versioned access to files. External Locations enable secure access to cloud storage. Both are essential for modern data engineering on Databricks with Unity Catalog.
