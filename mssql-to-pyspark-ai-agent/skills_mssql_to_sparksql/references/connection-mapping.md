# SSIS → Databricks Connection Mapping

## Connection Manager Types

| SSIS Connection Manager | Databricks Connector | Configuration |
|---|---|---|
| **OLE DB (SQL Server)** | JDBC via Spark | `spark.read.format("jdbc")` |
| **ADO.NET (SQL Server)** | JDBC via Spark | Same as OLE DB |
| **ODBC** | JDBC or native connector | Depends on source system |
| **Flat File (CSV/TXT)** | `spark.read.csv()` | Cloud storage path (ADLS, S3) |
| **Excel** | `spark.read.format("com.crealytics.spark.excel")` | Requires library |
| **FTP/SFTP** | `dbutils.fs` + cloud storage | Stage files first, then read |
| **HTTP** | Python `requests` + Spark | API calls in Python cells |
| **SMTP (Email)** | Python `smtplib` or workflow notification | Alerting only |

## SQL Server (OLE DB / ADO.NET) → Databricks JDBC

### SSIS Connection String
```
Data Source=SQLPROD01;
Initial Catalog=AdventureWorks;
Provider=SQLNCLI11.1;
Integrated Security=SSPI;
```

### Databricks Equivalent
```python
jdbc_url = "jdbc:sqlserver://SQLPROD01:1433;databaseName=AdventureWorks"

df = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "dbo.Customer")
    .option("user", dbutils.secrets.get("kv-scope", "sql-user"))
    .option("password", dbutils.secrets.get("kv-scope", "sql-password"))
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("fetchsize", "10000")
    .load()
)
```

### With Query Pushdown
```python
query = """
(SELECT CustomerID, CustomerName, Email, ModifiedDate
 FROM dbo.Customer
 WHERE ModifiedDate > '2024-01-01') AS subquery
"""

df = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", query)
    .option("user", dbutils.secrets.get("kv-scope", "sql-user"))
    .option("password", dbutils.secrets.get("kv-scope", "sql-password"))
    .load()
)
```

### Parallel Read (Large Tables)
```python
df = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "dbo.Orders")
    .option("user", dbutils.secrets.get("kv-scope", "sql-user"))
    .option("password", dbutils.secrets.get("kv-scope", "sql-password"))
    .option("partitionColumn", "OrderID")
    .option("lowerBound", "1")
    .option("upperBound", "10000000")
    .option("numPartitions", "10")
    .load()
)
```

## Flat File → Cloud Storage

### SSIS Flat File Connection
```
\\FileServer\ETL\Data\customers.csv
Format: Delimited (comma)
Header: Yes
Text Qualifier: double-quote
```

### Databricks Equivalent
```python
# Files should be staged in cloud storage first
# ADLS Gen2 example:
df = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", "true")
    .load("abfss://raw@storageaccount.dfs.core.windows.net/etl/data/customers.csv")
)

# With explicit schema (recommended for production):
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

schema = StructType([
    StructField("CustomerID", IntegerType(), False),
    StructField("CustomerName", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("ModifiedDate", TimestampType(), True)
])

df = spark.read.csv(path, header=True, schema=schema)
```

## Secrets Management

### SSIS: Package/Project Parameters + SQL Server Agent
```
Connection string stored in SSIS Catalog environment variables
or SQL Server Agent job step configuration.
```

### Databricks: Secret Scopes (Azure Key Vault backed)
```python
# Setup (one-time, via CLI or API):
# databricks secrets create-scope --scope kv-scope --scope-backend-type AZURE_KEYVAULT \
#   --resource-id /subscriptions/.../Microsoft.KeyVault/vaults/my-kv

# Usage in notebooks:
username = dbutils.secrets.get(scope="kv-scope", key="sql-username")
password = dbutils.secrets.get(scope="kv-scope", key="sql-password")
```

## Unity Catalog External Connections

For Unity Catalog, prefer using managed connections:

```sql
-- Create connection (admin, one-time)
CREATE CONNECTION sql_server_prod
TYPE SQLSERVER
OPTIONS (
    host 'SQLPROD01',
    port '1433',
    user secret('kv-scope', 'sql-user'),
    password secret('kv-scope', 'sql-password')
);

-- Use in notebook
CREATE TABLE bronze.raw_customers
USING FOREIGN TABLE
OPTIONS (
    connection 'sql_server_prod',
    dbtable 'AdventureWorks.dbo.Customer'
);
```
