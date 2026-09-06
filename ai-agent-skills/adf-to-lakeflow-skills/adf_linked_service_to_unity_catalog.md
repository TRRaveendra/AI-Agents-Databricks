# ADF Linked Service → Unity Catalog / Secrets

## 1. Overview

### What is an ADF Linked Service?
A Linked Service in Azure Data Factory defines the connection information needed to connect to external data sources and compute resources. It encapsulates authentication, connection strings, endpoints, and other configuration required for ADF to interact with external systems.

### Where is it used?
* Database connections (Azure SQL, PostgreSQL, Oracle, SQL Server)
* Cloud storage connections (Azure Blob, ADLS Gen2, S3)
* File system connections
* API endpoints (REST, SOAP)
* Compute resources (Databricks, HDInsight, Azure Batch)
* Authentication and credential management

## 2. Lakeflow Equivalent

### Concept Mapping
 ADF Linked Service | Databricks Equivalent |
--------------------|----------------------|
 Azure SQL Linked Service | JDBC connection with secrets |
 ADLS Gen2 Linked Service | Unity Catalog external location |
 Blob Storage Linked Service | Unity Catalog external location |
 Databricks Linked Service | N/A (native execution) |
 Key Vault | Databricks Secrets |
 Managed Identity | Service Principal / Unity Catalog credentials |
 Connection strings | Secret scopes |

### Key Differences
* **Credential Management**: ADF Linked Services vs Databricks Secrets + Unity Catalog
* **Access Model**: Linked Service references vs direct Unity Catalog access
* **Scope**: Pipeline-level vs workspace/catalog-level
* **Security**: Azure Key Vault integration vs Databricks Secret Scopes
* **Governance**: ADF managed vs Unity Catalog fine-grained ACLs

## 3. Migration Strategy (Step-by-Step)

### Step 1: Inventory Linked Services
```python
def inventory_linked_services(adf_export: dict) -> list:
    """Extract all linked services from ADF export"""
    
    linked_services = []
    
    for ls in adf_export.get('linkedServices', []):
        linked_services.append({
            'name': ls['name'],
            'type': ls['properties']['type'],
            'connection_info': extract_connection_info(ls),
            'authentication': ls['properties'].get('typeProperties', {}).get('authenticationType', 'Unknown')
        })
    
    return linked_services
```

### Step 2: Map to Databricks Construct
```python
def map_to_databricks_construct(linked_service: dict) -> str:
    """Determine appropriate Databricks migration target"""
    
    ls_type = linked_service['type']
    
    # Storage connections → Unity Catalog external locations
    if ls_type in ['AzureBlobStorage', 'AzureBlobFS', 'AzureDataLakeStore', 'AmazonS3']:
        return 'external_location'
    
    # Database connections → JDBC with secrets
    if ls_type in ['AzureSqlDatabase', 'SqlServer', 'PostgreSql', 'Oracle', 'MySql']:
        return 'jdbc_connection'
    
    # Databricks → Native (no migration needed)
    if ls_type == 'AzureDatabricks':
        return 'native'
    
    # REST/Web → Secrets + notebook code
    if ls_type in ['RestService', 'HttpServer']:
        return 'api_secrets'
    
    return 'generic_secrets'
```

### Step 3: Create Unity Catalog Objects
```python
def create_unity_catalog_objects(linked_service: dict, construct_type: str):
    """Generate Unity Catalog SQL to replace linked service"""
    
    if construct_type == 'external_location':
        return generate_external_location_sql(linked_service)
    
    elif construct_type == 'jdbc_connection':
        return generate_jdbc_secrets(linked_service)
    
    elif construct_type == 'api_secrets':
        return generate_api_secrets(linked_service)
```

## 4. Code Conversion

### ADF Example (JSON)

#### Example 1: Azure SQL Linked Service
```json
{
  "name": "AzureSqlLinkedService",
  "type": "Microsoft.DataFactory/factories/linkedservices",
  "properties": {
    "type": "AzureSqlDatabase",
    "typeProperties": {
      "connectionString": "Server=tcp:myserver.database.windows.net,1433;Database=mydb;",
      "authenticationType": "ServicePrincipal",
      "servicePrincipalId": "app-client-id",
      "servicePrincipalKey": {
        "type": "AzureKeyVaultSecret",
        "store": {
          "referenceName": "AzureKeyVault",
          "type": "LinkedServiceReference"
        },
        "secretName": "sql-sp-secret"
      },
      "tenant": "tenant-id"
    }
  }
}
```

#### Example 2: ADLS Gen2 Linked Service
```json
{
  "name": "ADLSGen2LinkedService",
  "type": "Microsoft.DataFactory/factories/linkedservices",
  "properties": {
    "type": "AzureBlobFS",
    "typeProperties": {
      "url": "https://mydatalake.dfs.core.windows.net/",
      "authenticationType": "ServicePrincipal",
      "servicePrincipalId": "app-client-id",
      "servicePrincipalKey": {
        "type": "AzureKeyVaultSecret",
        "store": {
          "referenceName": "AzureKeyVault",
          "type": "LinkedServiceReference"
        },
        "secretName": "adls-sp-secret"
      },
      "tenant": "tenant-id"
    }
  }
}
```

#### Example 3: REST API Linked Service
```json
{
  "name": "RestAPILinkedService",
  "type": "Microsoft.DataFactory/factories/linkedservices",
  "properties": {
    "type": "RestService",
    "typeProperties": {
      "url": "https://api.example.com/v1",
      "authenticationType": "Basic",
      "userName": "api-user",
      "password": {
        "type": "AzureKeyVaultSecret",
        "store": {
          "referenceName": "AzureKeyVault",
          "type": "LinkedServiceReference"
        },
        "secretName": "api-password"
      }
    }
  }
}
```

### Databricks Equivalent

#### SQL (Azure SQL → Unity Catalog + Secrets)
```sql
-- Step 1: Create secret scope (one-time setup)
-- Run in Databricks CLI or via API
-- databricks secrets create-scope --scope azure-sql-prod

-- Step 2: Store secrets (one-time setup)
-- databricks secrets put --scope azure-sql-prod --key sp-client-id
-- databricks secrets put --scope azure-sql-prod --key sp-client-secret
-- databricks secrets put --scope azure-sql-prod --key tenant-id

-- Step 3: Create external data source (optional, for frequent use)
CREATE EXTERNAL LOCATION IF NOT EXISTS azure_sql_prod
URL 'jdbc:sqlserver://myserver.database.windows.net:1433;database=mydb'
WITH (CREDENTIAL azure_sql_credential);

-- Or define connection in notebook
```

#### PySpark (Using Secrets for JDBC)
```python
# Databricks notebook: JDBC connection using secrets
# COMMAND ----------
# Retrieve secrets (replaces ADF Linked Service)
server = "myserver.database.windows.net"
database = "mydb"
sp_client_id = dbutils.secrets.get(scope="azure-sql-prod", key="sp-client-id")
sp_client_secret = dbutils.secrets.get(scope="azure-sql-prod", key="sp-client-secret")
tenant_id = dbutils.secrets.get(scope="azure-sql-prod", key="tenant-id")

# COMMAND ----------
# Build JDBC URL with authentication
jdbc_url = f"jdbc:sqlserver://{server}:1433;database={database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;Authentication=ActiveDirectoryServicePrincipal"

connection_properties = {
    "user": sp_client_id,
    "password": sp_client_secret,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "AADSecurePrincipalId": sp_client_id,
    "AADSecurePrincipalSecret": sp_client_secret
}

# COMMAND ----------
# Read from SQL Server (replaces ADF dataset with linked service)
df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "dbo.customers")
    .options(**connection_properties)
    .load()
)

display(df)
```

#### SQL (ADLS Gen2 → Unity Catalog External Location)
```sql
-- Step 1: Create storage credential (one-time, requires account admin)
CREATE STORAGE CREDENTIAL IF NOT EXISTS adls_prod_credential
WITH (
  AZURE_SERVICE_PRINCIPAL
  CLIENT_ID 'app-client-id',
  CLIENT_SECRET 'client-secret-value',  -- Or reference from secret
  DIRECTORY_ID 'tenant-id'
);

-- Step 2: Create external location
CREATE EXTERNAL LOCATION IF NOT EXISTS adls_raw_data
URL 'abfss://raw@mydatalake.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL adls_prod_credential)
COMMENT 'Raw data landing zone from ADLS Gen2';

-- Step 3: Grant permissions
GRANT READ FILES ON EXTERNAL LOCATION adls_raw_data TO `data-engineers`;
GRANT WRITE FILES ON EXTERNAL LOCATION adls_raw_data TO `etl-service-account`;

-- Step 4: Use in queries
CREATE TABLE IF NOT EXISTS production_catalog.bronze.customers
LOCATION 'abfss://raw@mydatalake.dfs.core.windows.net/customers/';

SELECT * FROM production_catalog.bronze.customers;
```

#### PySpark (Direct ADLS Access with Secrets)
```python
# Databricks notebook: ADLS Gen2 access
# COMMAND ----------
# Option 1: Account key authentication
storage_account_name = "mydatalake"
storage_account_key = dbutils.secrets.get(scope="azure-storage", key="account-key")

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# COMMAND ----------
# Option 2: Service Principal authentication (recommended)
storage_account_name = "mydatalake"
client_id = dbutils.secrets.get(scope="azure-storage", key="sp-client-id")
client_secret = dbutils.secrets.get(scope="azure-storage", key="sp-secret")
tenant_id = dbutils.secrets.get(scope="azure-storage", key="tenant-id")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------
# Read from ADLS (replaces ADF dataset with linked service)
df = spark.read.format("parquet").load(f"abfss://raw@{storage_account_name}.dfs.core.windows.net/customers/")

display(df)
```

#### PySpark (REST API with Secrets)
```python
# Databricks notebook: REST API calls
# COMMAND ----------
import requests
from requests.auth import HTTPBasicAuth

# COMMAND ----------
# Retrieve API credentials (replaces REST Linked Service)
api_base_url = "https://api.example.com/v1"
api_user = dbutils.secrets.get(scope="external-apis", key="api-user")
api_password = dbutils.secrets.get(scope="external-apis", key="api-password")

# COMMAND ----------
# Make API call
response = requests.get(
    f"{api_base_url}/customers",
    auth=HTTPBasicAuth(api_user, api_password),
    headers={"Accept": "application/json"}
)

# COMMAND ----------
# Convert to DataFrame
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data = response.json()

# Define schema
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data['customers'], schema)

# Write to Delta
df.write.mode("overwrite").saveAsTable("production_catalog.bronze.api_customers")
```

## 5. Design Patterns

### Pattern 1: Centralized Secret Management
```python
# Create reusable connection utility
class ConnectionManager:
    """Centralized connection management"""
    
    def __init__(self, secret_scope: str):
        self.secret_scope = secret_scope
    
    def get_jdbc_connection(self, connection_name: str):
        """Get JDBC connection properties"""
        
        return {
            "url": dbutils.secrets.get(self.secret_scope, f"{connection_name}-url"),
            "user": dbutils.secrets.get(self.secret_scope, f"{connection_name}-user"),
            "password": dbutils.secrets.get(self.secret_scope, f"{connection_name}-password"),
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
    
    def get_storage_config(self, storage_name: str):
        """Get storage account configuration"""
        
        account_name = dbutils.secrets.get(self.secret_scope, f"{storage_name}-account")
        account_key = dbutils.secrets.get(self.secret_scope, f"{storage_name}-key")
        
        spark.conf.set(
            f"fs.azure.account.key.{account_name}.dfs.core.windows.net",
            account_key
        )

# Usage
conn_mgr = ConnectionManager("production-secrets")
jdbc_props = conn_mgr.get_jdbc_connection("azure-sql-prod")
df = spark.read.jdbc(**jdbc_props, table="dbo.customers")
```

### Pattern 2: Environment-Specific Connections
```python
# Different secret scopes per environment
def get_connection_config(environment: str):
    """Get connection config based on environment"""
    
    scope_mapping = {
        "dev": "dev-secrets",
        "staging": "staging-secrets",
        "production": "prod-secrets"
    }
    
    scope = scope_mapping[environment]
    
    return {
        "jdbc_url": dbutils.secrets.get(scope, "sql-server-url"),
        "storage_account": dbutils.secrets.get(scope, "storage-account"),
        "api_key": dbutils.secrets.get(scope, "external-api-key")
    }

# Usage
env = dbutils.widgets.get("environment")
config = get_connection_config(env)
```

### Pattern 3: Unity Catalog External Location Abstraction
```sql
-- Define external locations per data layer
CREATE EXTERNAL LOCATION bronze_data
URL 'abfss://bronze@datalake.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL prod_credential);

CREATE EXTERNAL LOCATION silver_data
URL 'abfss://silver@datalake.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL prod_credential);

CREATE EXTERNAL LOCATION gold_data
URL 'abfss://gold@datalake.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL prod_credential);

-- Use consistent naming in table definitions
CREATE TABLE production_catalog.bronze.customers
LOCATION 'abfss://bronze@datalake.dfs.core.windows.net/customers/';
```

## 6. Anti-Patterns

### ❌ Anti-Pattern 1: Hardcoding Credentials
**Problem**: Embedding credentials in notebooks.
```python
# WRONG: Hardcoded credentials
jdbc_url = "jdbc:sqlserver://server.database.windows.net;user=admin;password=MyPassword123"
```

**Solution**: Always use secrets.
```python
# CORRECT: Use secrets
user = dbutils.secrets.get("prod-secrets", "sql-user")
password = dbutils.secrets.get("prod-secrets", "sql-password")
jdbc_url = f"jdbc:sqlserver://server.database.windows.net;user={user};password={password}"
```

### ❌ Anti-Pattern 2: Not Using Unity Catalog for Storage
**Problem**: Direct storage access instead of Unity Catalog.
```python
# WRONG: Direct access configuration in every notebook
spark.conf.set("fs.azure.account.key.storage.dfs.core.windows.net", "key")
df = spark.read.parquet("abfss://...")
```

**Solution**: Define external locations once, reuse everywhere.
```sql
-- CORRECT: Define once
CREATE EXTERNAL LOCATION data_lake
URL 'abfss://data@storage.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL prod_cred);

-- Use in tables
CREATE TABLE catalog.schema.table
LOCATION 'abfss://data@storage.dfs.core.windows.net/table/';
```

### ❌ Anti-Pattern 3: Separate Secrets for Each Pipeline
**Problem**: Creating new secret scope for every job.
```python
# WRONG: Proliferation of secret scopes
dbutils.secrets.get("pipeline1-secrets", "db-password")
dbutils.secrets.get("pipeline2-secrets", "db-password")
dbutils.secrets.get("pipeline3-secrets", "db-password")
```

**Solution**: Organize by system/environment, not pipeline.
```python
# CORRECT: Logical grouping
dbutils.secrets.get("azure-sql-prod", "customer-db-password")
dbutils.secrets.get("azure-storage-prod", "account-key")
```

## 7. Performance Considerations

### Connection Pooling
```python
# Enable JDBC connection pooling
jdbc_options = {
    "url": jdbc_url,
    "dbtable": table,
    "numPartitions": "10",
    "fetchsize": "10000",
    # Connection pooling
    "connectionProvider": "hikari",
    "hikari.maximumPoolSize": "20",
    "hikari.connectionTimeout": "30000"
}

df = spark.read.format("jdbc").options(**jdbc_options).load()
```

### Credential Caching
```python
# Cache credentials for repeated use
class CachedSecretManager:
    _cache = {}
    
    @classmethod
    def get_secret(cls, scope: str, key: str) -> str:
        cache_key = f"{scope}:{key}"
        
        if cache_key not in cls._cache:
            cls._cache[cache_key] = dbutils.secrets.get(scope, key)
        
        return cls._cache[cache_key]
```

## 8. Governance & Security

### Principle of Least Privilege
```sql
-- Grant minimal necessary permissions
GRANT READ FILES ON EXTERNAL LOCATION bronze_data TO `data-engineers`;
GRANT WRITE FILES ON EXTERNAL LOCATION bronze_data TO `etl-service-principal`;

-- Don't grant broad permissions
-- WRONG: GRANT ALL PRIVILEGES ON CATALOG production TO `everyone`;
```

### Audit Secret Access
```sql
-- Query secret access audit logs
SELECT 
    event_time,
    user_identity.email,
    request_params.scope_name,
    request_params.key_name,
    response.status_code
FROM system.access.audit
WHERE action_name = 'getSecret'
    AND event_date >= CURRENT_DATE - INTERVAL 7 DAYS
ORDER BY event_time DESC;
```

### Rotate Credentials Regularly
```python
# Automated secret rotation check
def check_secret_age(scope: str, key: str) -> bool:
    """Check if secret needs rotation"""
    
    # Query audit logs for secret creation/update
    audit_df = spark.sql(f"""
        SELECT MAX(event_time) as last_updated
        FROM system.access.audit
        WHERE action_name = 'putSecret'
            AND request_params.scope_name = '{scope}'
            AND request_params.key_name = '{key}'
    """)
    
    last_updated = audit_df.first()['last_updated']
    
    # Check if older than 90 days
    from datetime import datetime, timedelta
    rotation_threshold = datetime.now() - timedelta(days=90)
    
    if last_updated < rotation_threshold:
        print(f"⚠️  Secret {scope}/{key} needs rotation!")
        return False
    
    return True
```

## 9. Automation Hints (IMPORTANT)

### AI Agent Transformation Logic
```python
class LinkedServiceTransformer:
    """Transform ADF Linked Services to Databricks equivalents"""
    
    def transform(self, linked_service: dict) -> dict:
        """Main transformation method"""
        
        ls_type = linked_service['properties']['type']
        
        if ls_type in ['AzureBlobStorage', 'AzureBlobFS']:
            return self.generate_external_location(linked_service)
        
        elif ls_type in ['AzureSqlDatabase', 'SqlServer']:
            return self.generate_jdbc_config(linked_service)
        
        elif ls_type == 'RestService':
            return self.generate_api_config(linked_service)
        
        else:
            return self.generate_generic_secrets(linked_service)
    
    def generate_external_location(self, ls: dict) -> dict:
        """Generate Unity Catalog external location SQL"""
        
        type_props = ls['properties']['typeProperties']
        url = type_props['url']
        
        # Extract credential info
        sp_id = type_props.get('servicePrincipalId', '')
        tenant = type_props.get('tenant', '')
        
        return {
            'type': 'external_location',
            'sql': f"""
CREATE STORAGE CREDENTIAL IF NOT EXISTS {ls['name']}_credential
WITH (
  AZURE_SERVICE_PRINCIPAL
  CLIENT_ID '{sp_id}',
  CLIENT_SECRET '{{{{secrets/{ls['name']}/sp-secret}}}}',
  DIRECTORY_ID '{tenant}'
);

CREATE EXTERNAL LOCATION IF NOT EXISTS {ls['name']}_location
URL '{url}'
WITH (STORAGE CREDENTIAL {ls['name']}_credential);
            """,
            'secrets_needed': [
                {'scope': ls['name'], 'key': 'sp-secret', 'description': 'Service Principal secret'}
            ]
        }
    
    def generate_jdbc_config(self, ls: dict) -> dict:
        """Generate JDBC connection configuration"""
        
        type_props = ls['properties']['typeProperties']
        conn_string = type_props['connectionString']
        
        # Parse connection string
        server, database = self._parse_connection_string(conn_string)
        
        return {
            'type': 'jdbc_connection',
            'python_code': f'''
# JDBC connection for {ls['name']}
jdbc_url = "jdbc:sqlserver://{server};database={database}"
jdbc_user = dbutils.secrets.get("{ls['name']}", "username")
jdbc_password = dbutils.secrets.get("{ls['name']}", "password")

connection_properties = {{
    "user": jdbc_user,
    "password": jdbc_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}}

df = spark.read.jdbc(url=jdbc_url, table="your_table", properties=connection_properties)
            ''',
            'secrets_needed': [
                {'scope': ls['name'], 'key': 'username', 'description': 'Database username'},
                {'scope': ls['name'], 'key': 'password', 'description': 'Database password'}
            ]
        }
```

## 10. Validation Checklist

### Pre-Migration Validation
- [ ] All linked services cataloged
- [ ] Connection strings extracted
- [ ] Authentication methods documented
- [ ] Secrets identified and backed up
- [ ] Target Databricks constructs determined
- [ ] Permission requirements identified

### Post-Migration Validation
```python
def validate_connection_migration(linked_service_name: str):
    """Validate migrated connection works"""
    
    try:
        # Test JDBC connection
        if is_jdbc_connection(linked_service_name):
            test_df = spark.read.jdbc(
                url=get_jdbc_url(linked_service_name),
                table="(SELECT 1 AS test) AS t",
                properties=get_jdbc_properties(linked_service_name)
            )
            assert test_df.count() == 1, "JDBC connection test failed"
        
        # Test storage access
        elif is_storage_connection(linked_service_name):
            test_path = get_storage_test_path(linked_service_name)
            files = dbutils.fs.ls(test_path)
            assert len(files) >= 0, "Storage access test failed"
        
        print(f"✅ {linked_service_name} migration validated")
        
    except Exception as e:
        print(f"❌ {linked_service_name} validation failed: {e}")
        raise
```

---

**Related Skills:**
* [adf_dataset_to_delta_table.md](adf_dataset_to_delta_table.md) - Dataset migration using connections
* [adf_copy_activity_to_spark.md](adf_copy_activity_to_spark.md) - Using connections in data movement
* [migration_index.md](migration_index.md) - Complete migration guide
