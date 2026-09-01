# Gold Layer - Business-Ready Analytics

## Purpose
The Gold layer provides business-level aggregates, denormalized tables, and analytics-ready datasets. It implements business logic, creates fact/dimension tables, and optimizes for query performance.

## Core Principles

### 1. Business Logic
- Implement complex business rules
- Multi-table joins for complete context
- KPI calculations
- Domain-specific transformations

### 2. Optimized for Consumption
- Pre-aggregated when appropriate
- Denormalized for performance
- Liquid clustering on query patterns
- Dimension and fact tables (star schema)

### 3. Production-Ready
- SCD (Slowly Changing Dimensions) where needed
- Point-in-time accuracy
- Well-documented business definitions

## Gold Table Design

### Fact Table (Transactions/Events)
```sql
CREATE TABLE IF NOT EXISTS gold.fact_sales (
    sale_id BIGINT NOT NULL,
    order_date DATE NOT NULL,
    customer_key BIGINT NOT NULL,          -- Surrogate key to dim_customer
    product_key BIGINT NOT NULL,           -- Surrogate key to dim_product
    store_key BIGINT NOT NULL,             -- Surrogate key to dim_store
    
    -- Measures/Metrics
    quantity INT,
    unit_price DECIMAL(10, 2),
    discount_amount DECIMAL(10, 2),
    tax_amount DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    
    -- Audit
    _created_timestamp TIMESTAMP
)
USING DELTA
CLUSTER BY (order_date, customer_key)
COMMENT 'Sales fact table - grain: one row per line item';
```

### Dimension Table (SCD Type 2)
```sql
CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_key BIGINT GENERATED ALWAYS AS IDENTITY,  -- Surrogate key
    customer_id STRING NOT NULL,                       -- Business key
    
    -- Attributes
    customer_name STRING,
    email STRING,
    phone STRING,
    customer_segment STRING,
    loyalty_tier STRING,
    
    -- SCD Type 2 tracking
    effective_start_date DATE NOT NULL,
    effective_end_date DATE,
    is_current BOOLEAN NOT NULL,
    
    -- Audit
    _created_timestamp TIMESTAMP,
    _updated_timestamp TIMESTAMP
)
USING DELTA
CLUSTER BY (customer_id, is_current)
COMMENT 'Customer dimension - SCD Type 2 for historical tracking';
```

### Aggregate Table (Pre-computed Metrics)
```sql
CREATE TABLE IF NOT EXISTS gold.daily_sales_summary (
    report_date DATE NOT NULL,
    customer_segment STRING,
    product_category STRING,
    region STRING,
    
    -- Aggregated metrics
    total_orders BIGINT,
    total_revenue DECIMAL(18, 2),
    total_units_sold BIGINT,
    unique_customers BIGINT,
    avg_order_value DECIMAL(10, 2),
    
    -- Audit
    _computed_timestamp TIMESTAMP
)
USING DELTA
CLUSTER BY (report_date, customer_segment)
COMMENT 'Daily sales rollup for dashboards and reporting';
```

## Silver to Gold Transformation Patterns

### Pattern 1: Fact Table Creation
```python
from pyspark.sql.functions import col, current_timestamp

def create_fact_sales():
    """
    Create fact_sales by joining Silver tables.
    Uses surrogate keys from dimension tables.
    """
    # Read from Silver
    orders = spark.read.table("silver.orders")
    order_items = spark.read.table("silver.order_items")
    
    # Read dimension tables to get surrogate keys
    dim_customer = spark.read.table("gold.dim_customer").filter(col("is_current") == True)
    dim_product = spark.read.table("gold.dim_product").filter(col("is_current") == True)
    dim_store = spark.read.table("gold.dim_store")
    
    # Join to build fact table
    fact_df = (order_items
        .join(orders, "order_id")
        .join(dim_customer, orders["customer_id"] == dim_customer["customer_id"])
        .join(dim_product, order_items["product_id"] == dim_product["product_id"])
        .join(dim_store, orders["store_id"] == dim_store["store_id"])
        .select(
            order_items["order_item_id"].alias("sale_id"),
            orders["order_date"],
            dim_customer["customer_key"],
            dim_product["product_key"],
            dim_store["store_key"],
            order_items["quantity"],
            order_items["unit_price"],
            order_items["discount_amount"],
            order_items["tax_amount"],
            (order_items["quantity"] * order_items["unit_price"] - 
             order_items["discount_amount"] + order_items["tax_amount"]).alias("total_amount"),
            current_timestamp().alias("_created_timestamp")
        )
    )
    
    # Write to Gold fact table
    fact_df.write.mode("append").saveAsTable("gold.fact_sales")
    print(f"✓ Created {fact_df.count()} fact records")
    
    return fact_df

create_fact_sales()
```

### Pattern 2: Daily Aggregate Creation
```python
from pyspark.sql.functions import count, sum as spark_sum, avg, countDistinct

def create_daily_sales_summary(report_date=None):
    """
    Create pre-aggregated daily summary for dashboards.
    """
    if report_date is None:
        report_date = spark.sql("SELECT CURRENT_DATE()").collect()[0][0]
    
    # Read from Silver (or fact table if already exists)
    sales_df = (spark.read.table("silver.orders")
        .filter(col("order_date") == report_date)
    )
    
    customers_df = spark.read.table("silver.customers")
    products_df = spark.read.table("silver.products")
    
    # Join and aggregate
    summary_df = (sales_df
        .join(customers_df, "customer_id")
        .join(products_df, "product_id")
        .groupBy(
            col("order_date").alias("report_date"),
            col("customer_segment"),
            col("product_category"),
            col("region")
        )
        .agg(
            count("order_id").alias("total_orders"),
            spark_sum("order_amount").alias("total_revenue"),
            spark_sum("quantity").alias("total_units_sold"),
            countDistinct("customer_id").alias("unique_customers"),
            avg("order_amount").alias("avg_order_value")
        )
        .withColumn("_computed_timestamp", current_timestamp())
    )
    
    # Write to Gold
    summary_df.write.mode("append").saveAsTable("gold.daily_sales_summary")
    print(f"✓ Created daily summary for {report_date}")
    
    return summary_df

create_daily_sales_summary()
```

### Pattern 3: Customer 360 View
```python
def create_customer_360():
    """
    Create comprehensive customer view with aggregated metrics.
    """
    customers = spark.read.table("silver.customers")
    orders = spark.read.table("silver.orders")
    support_tickets = spark.read.table("silver.support_tickets")
    
    # Order metrics
    order_metrics = (orders
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),
            spark_sum("order_amount").alias("lifetime_value"),
            max("order_date").alias("last_order_date"),
            min("order_date").alias("first_order_date"),
            avg("order_amount").alias("avg_order_value")
        )
    )
    
    # Support metrics
    support_metrics = (support_tickets
        .groupBy("customer_id")
        .agg(
            count("ticket_id").alias("total_tickets"),
            spark_sum(when(col("status") == "resolved", 1).otherwise(0)).alias("resolved_tickets")
        )
    )
    
    # Combine into 360 view
    customer_360 = (customers
        .join(order_metrics, "customer_id", "left")
        .join(support_metrics, "customer_id", "left")
        .select(
            "customer_id",
            "customer_name",
            "email",
            "customer_segment",
            "loyalty_tier",
            coalesce("total_orders", lit(0)).alias("total_orders"),
            coalesce("lifetime_value", lit(0.0)).alias("lifetime_value"),
            coalesce("total_tickets", lit(0)).alias("total_tickets"),
            "last_order_date",
            "first_order_date",
            "avg_order_value",
            current_timestamp().alias("_created_timestamp")
        )
    )
    
    customer_360.write.mode("overwrite").saveAsTable("gold.customer_360")
    print(f"✓ Created Customer 360 view with {customer_360.count()} customers")
    
    return customer_360

create_customer_360()
```

### Pattern 4: Incremental Gold Updates (MERGE)
```python
from delta.tables import DeltaTable

def update_daily_summary_incremental(target_date):
    """
    Update or insert daily summary for specific date.
    Uses MERGE for efficient incremental updates.
    """
    # Calculate metrics for target date
    new_summary = calculate_daily_metrics(target_date)
    
    # MERGE into existing Gold table
    target_table = DeltaTable.forName(spark, "gold.daily_sales_summary")
    
    (target_table.alias("target")
        .merge(
            new_summary.alias("source"),
            """target.report_date = source.report_date AND 
               target.customer_segment = source.customer_segment AND
               target.product_category = source.product_category"""
        )
        .whenMatchedUpdate(set={
            "total_orders": "source.total_orders",
            "total_revenue": "source.total_revenue",
            "unique_customers": "source.unique_customers",
            "_computed_timestamp": "current_timestamp()"
        })
        .whenNotMatchedInsert(values={
            "report_date": "source.report_date",
            "customer_segment": "source.customer_segment",
            "product_category": "source.product_category",
            "total_orders": "source.total_orders",
            "total_revenue": "source.total_revenue",
            "_computed_timestamp": "current_timestamp()"
        })
        .execute()
    )
    
    print(f"✓ Updated summary for {target_date}")

update_daily_summary_incremental("2024-01-15")
```

## Star Schema Best Practices

### Dimension Table Guidelines
✓ Use surrogate keys (auto-incrementing)  
✓ Keep business keys for joins from Silver  
✓ Implement SCD Type 2 for historical tracking  
✓ Use `is_current` flag for easy current-state queries  
✓ Denormalize when it improves query performance  

### Fact Table Guidelines
✓ Reference dimensions via surrogate keys  
✓ Grain must be clearly defined (one row per...)  
✓ Only store numeric measures/metrics  
✓ Use CLUSTER BY on common filter/join columns  
✓ Partition by date for very large tables (> 1TB)  

## Gold Best Practices

✓ **Pre-aggregate for dashboards** - Faster queries  
✓ **Use star schema** - Fact + dimensions for analytics  
✓ **Implement SCD Type 2** - When history matters  
✓ **Optimize with liquid clustering** - On query patterns  
✓ **Denormalize appropriately** - Balance normalization vs performance  
✓ **Document business logic** - Complex calculations need clear docs  
✓ **Incremental updates** - Use MERGE for efficiency  

## Gold Anti-patterns

❌ **Overly normalized** - Too many joins hurt performance  
❌ **No pre-aggregation** - Dashboards query raw data  
❌ **Missing SCD** - Can't answer historical questions  
❌ **Using business keys in facts** - Use surrogate keys  
❌ **No clustering/partitioning** - Large table query performance suffers  
❌ **Undocumented metrics** - Business definitions unclear  

## See Also
- [Silver Layer](silver.md) - Input from Silver
- [04-scd.md](04-scd.md) - Slowly Changing Dimensions details
- [08-delta-table-design.md](08-delta-table-design.md) - Table optimization
- [Code Examples](../code/examples/gold_star_schema.py) - Full examples

---

**Remember**: Gold is optimized for consumption. Think about how analysts and dashboards will query the data.
