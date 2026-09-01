# Security and Governance

## Unity Catalog

Unity Catalog provides centralized governance for data, ML models, and notebooks.

### Three-level namespace
```
catalog.schema.table
main.sales.customers
dev.staging.orders
```

### Create catalogs and schemas
```sql
-- Create catalog
CREATE CATALOG IF NOT EXISTS production;

-- Create schema
CREATE SCHEMA IF NOT EXISTS production.sales;

-- Create table
CREATE TABLE production.sales.customers (...);
```

## Least Privilege Access

### Grant minimum necessary permissions
```sql
-- Grant read access to specific schema
GRANT SELECT ON SCHEMA production.sales TO `data_analysts`;

-- Grant write access to specific table
GRANT SELECT, INSERT, UPDATE ON TABLE production.sales.orders TO `data_engineers`;

-- Grant execute permission on function
GRANT EXECUTE ON FUNCTION production.sales.calculate_revenue TO `analysts`;
```

### Check permissions
```sql
-- Show grants for a user
SHOW GRANTS ON CATALOG production FOR `user@company.com`;

-- Show grants for a table
SHOW GRANTS ON TABLE production.sales.customers;
```

## Secrets Management

### Never hardcode credentials
```python
# BAD: Hardcoded credentials
jdbc_url = "jdbc:mysql://server:3306/db"
username = "admin"
password = "password123"  # Never do this!

# GOOD: Use Databricks secrets
username = dbutils.secrets.get(scope="db-credentials", key="username")
password = dbutils.secrets.get(scope="db-credentials", key="password")
```

### Create and use secret scopes
```bash
# Create secret scope (using Databricks CLI)
databricks secrets create-scope --scope db-credentials

# Add secret
databricks secrets put-secret --scope db-credentials --key username --string-value "admin"
databricks secrets put-secret --scope db-credentials --key password --string-value "secure_password"
```

## Service Principals

Use service principals for production jobs instead of user accounts.

```python
# Configure service principal authentication
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", dbutils.secrets.get(scope="azure", key="sp-client-id"))
spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(scope="azure", key="sp-client-secret"))
```

## PII Handling

### Identify and protect PII
```sql
-- Tag PII columns
ALTER TABLE production.sales.customers 
ALTER COLUMN email SET TAGS ('pii' = 'email');

ALTER TABLE production.sales.customers 
ALTER COLUMN ssn SET TAGS ('pii' = 'ssn', 'sensitivity' = 'high');
```

### Mask PII in non-production
```sql
-- Create view with masked PII for dev/test
CREATE VIEW dev.sales.customers_masked AS
SELECT
    customer_id,
    CONCAT(LEFT(email, 3), '***@***.com') as email,
    '***-**-****' as ssn,
    customer_name,
    region
FROM production.sales.customers;
```

## Row-Level and Column-Level Security

### Row-level security (row filters)
```sql
-- Only show rows for user's region
CREATE FUNCTION production.sales.region_filter()
RETURNS STRING
RETURN SELECT region FROM production.sales.user_regions WHERE user = current_user();

ALTER TABLE production.sales.orders SET ROW FILTER 
    region_filter ON (region);
```

### Column-level security (column masks)
```sql
-- Mask SSN for non-privileged users
CREATE FUNCTION production.sales.mask_ssn(ssn STRING)
RETURNS STRING
RETURN IF(is_member('sensitive_data_access'), ssn, '***-**-****');

ALTER TABLE production.sales.customers ALTER COLUMN ssn SET MASK mask_ssn;
```

## Data Lineage

Unity Catalog automatically captures lineage.

```sql
-- View lineage
DESCRIBE EXTENDED production.gold.customer_summary;

-- Lineage shows:
-- - Source tables used
-- - Transformation notebooks
-- - Downstream dependencies
```

## Best Practices

1. **Use Unity Catalog** - Centralized governance for all data assets
2. **Implement least privilege** - Grant minimum necessary permissions
3. **Never hardcode secrets** - Always use secret scopes
4. **Use service principals for jobs** - Not personal user accounts
5. **Tag PII columns** - Required for compliance (GDPR, CCPA)
6. **Mask PII in non-production** - Dev/test should not have real PII
7. **Audit access regularly** - Review who has access to sensitive data
8. **Enable audit logging** - Track all data access
9. **Implement row/column security** - When appropriate for sensitive data
10. **Document data classification** - What data is sensitive, why, and how to handle it

## Compliance Checklist

✓ All PII columns tagged  
✓ Access controls in place  
✓ No hardcoded credentials  
✓ Service principals used for production  
✓ Audit logging enabled  
✓ Data lineage tracked  
✓ Sensitive data masked in non-production  
✓ Regular access reviews conducted

---

**Remember**: Security and governance are not optional. Build them into your pipelines from the start.
