# Testing Data Engineering Pipelines

## Types of Tests

### 1. Unit Tests (Transformation Logic)
Test individual transformation functions in isolation.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def test_calculate_total_revenue():
    spark = SparkSession.builder.getOrCreate()
    
    # Create test data
    test_data = [
        (1, "order1", 100.0),
        (1, "order2", 200.0),
        (2, "order3", 150.0)
    ]
    test_df = spark.createDataFrame(test_data, ["customer_id", "order_id", "amount"])
    
    # Apply transformation
    result_df = test_df.groupBy("customer_id").agg({"amount": "sum"})
    
    # Assert expected results
    results = result_df.collect()
    assert len(results) == 2
    assert results[0]["sum(amount)"] == 300.0
```

### 2. Data Quality Tests
Validate data quality rules are enforced.

```python
def test_null_validation():
    # Given: DataFrame with some null customer_ids
    test_data = [
        (1, "customer1", "email1@test.com"),
        (None, "customer2", "email2@test.com"),
        (3, "customer3", None)
    ]
    test_df = spark.createDataFrame(test_data, ["customer_id", "name", "email"])
    
    # When: Apply validation logic
    validated_df = test_df.withColumn(
        "_is_valid",
        col("customer_id").isNotNull() & col("email").isNotNull()
    )
    
    # Then: Assert validation flags set correctly
    invalid_count = validated_df.filter(col("_is_valid") == False).count()
    assert invalid_count == 2  # 2 records have nulls
```

### 3. Integration Tests (Full Pipeline)
Test end-to-end pipeline with sample data.

```python
def test_bronze_to_silver_pipeline():
    # Given: Bronze table with raw data
    bronze_data = [...]
    bronze_df = spark.createDataFrame(bronze_data, bronze_schema)
    bronze_df.write.mode("overwrite").saveAsTable("test_bronze.orders")
    
    # When: Run silver transformation
    run_silver_pipeline("test_bronze.orders", "test_silver.orders")
    
    # Then: Verify silver table contents
    silver_df = spark.table("test_silver.orders")
    assert silver_df.count() == expected_count
    assert silver_df.filter(col("_is_valid") == False).count() == 0
```

### 4. Reconciliation Tests
Verify row counts and sums match between source and target.

```python
def test_row_count_reconciliation():
    source_count = spark.table("bronze.orders").count()
    target_count = spark.table("silver.orders").count()
    quarantine_count = spark.table("silver.orders_quarantine").count()
    
    assert source_count == (target_count + quarantine_count)

def test_sum_reconciliation():
    source_sum = spark.sql("SELECT SUM(amount) FROM bronze.orders").collect()[0][0]
    target_sum = spark.sql("SELECT SUM(amount) FROM silver.orders").collect()[0][0]
    
    # Allow small floating point tolerance
    assert abs(source_sum - target_sum) < 0.01
```

## Testing Patterns

### Full Load Testing
```python
def test_full_load():
    # Clear target table
    spark.sql("TRUNCATE TABLE test_silver.customers")
    
    # Load test data
    test_df = create_test_customers(100)
    test_df.write.mode("overwrite").saveAsTable("test_bronze.customers")
    
    # Run full load
    run_full_load_pipeline()
    
    # Verify
    result = spark.table("test_silver.customers")
    assert result.count() == 100
```

### Incremental Load Testing
```python
def test_incremental_load():
    # Initial load
    initial_df = create_test_customers(50)
    initial_df.write.mode("overwrite").saveAsTable("test_bronze.customers")
    run_incremental_load()
    
    # Add new records
    new_df = create_test_customers(20, start_id=51)
    new_df.write.mode("append").saveAsTable("test_bronze.customers")
    run_incremental_load()
    
    # Verify only new records processed
    result = spark.table("test_silver.customers")
    assert result.count() == 70
```

### CDC Testing
```python
def test_cdc_operations():
    # Test INSERT
    insert_df = create_test_records(operation="INSERT", count=10)
    process_cdc(insert_df)
    assert spark.table("test_silver.customers").count() == 10
    
    # Test UPDATE
    update_df = create_test_records(operation="UPDATE", count=5)
    process_cdc(update_df)
    
    # Test DELETE
    delete_df = create_test_records(operation="DELETE", count=2)
    process_cdc(delete_df)
    assert spark.table("test_silver.customers").count() == 8
```

### SCD Type 2 Testing
```python
def test_scd_type2():
    # Initial load
    initial_df = create_customer_records(customer_id=1, email="old@test.com")
    process_scd2(initial_df)
    
    # Update customer email
    update_df = create_customer_records(customer_id=1, email="new@test.com")
    process_scd2(update_df)
    
    # Verify history preserved
    history = spark.table("test_silver.dim_customer_scd2") \
        .filter(col("customer_id") == 1) \
        .orderBy("effective_start_date")
    
    assert history.count() == 2  # 2 versions
    assert history.filter(col("is_current") == True).count() == 1  # 1 current
    assert history.filter(col("email") == "old@test.com").first()["is_current"] == False
    assert history.filter(col("email") == "new@test.com").first()["is_current"] == True
```

## Best Practices

1. **Use separate test schemas** - Don't test against production tables
2. **Create reusable test data generators** - Parameterized test data creation functions
3. **Test edge cases** - Nulls, duplicates, empty datasets, schema changes
4. **Always clean up test data** - Truncate/drop test tables after tests
5. **Test idempotency** - Running pipeline twice should produce same result
6. **Automate tests in CI/CD** - Run tests on every code change
7. **Test with realistic data volumes** - Small samples may miss performance issues
8. **Document test assumptions** - What each test validates and why

## Test Organization

```
project/
├── src/
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
├── tests/
│   ├── unit/
│   │   ├── test_transformations.py
│   │   ├── test_validations.py
│   ├── integration/
│   │   ├── test_bronze_silver_pipeline.py
│   │   ├── test_silver_gold_pipeline.py
│   ├── data_quality/
│   │   ├── test_reconciliation.py
│   │   ├── test_null_handling.py
│   ├── fixtures/
│   │   ├── test_data.py
│   │   ├── test_schemas.py
```

## Regression Testing

After making changes, always verify:

✓ All existing tests still pass  
✓ Row counts match expected  
✓ Key metrics (sums, counts) unchanged  
✓ Data quality checks still pass  
✓ Performance hasn't degraded  

---

**Remember**: Test early, test often. Automated tests catch issues before they reach production.
