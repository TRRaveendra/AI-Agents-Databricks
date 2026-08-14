# SQL Server → Spark/Delta Data Type Mapping

## Numeric Types

| SQL Server | SSIS Data Type | Spark SQL | Delta Lake | Notes |
|---|---|---|---|---|
| `bit` | `DT_BOOL` | `BooleanType` | `BOOLEAN` | |
| `tinyint` | `DT_UI1` | `ShortType` | `SMALLINT` | Spark has no unsigned byte |
| `smallint` | `DT_I2` | `ShortType` | `SMALLINT` | |
| `int` | `DT_I4` | `IntegerType` | `INT` | |
| `bigint` | `DT_I8` | `LongType` | `BIGINT` | |
| `float` | `DT_R4` | `FloatType` | `FLOAT` | |
| `real` | `DT_R4` | `FloatType` | `FLOAT` | |
| `double` | `DT_R8` | `DoubleType` | `DOUBLE` | |
| `decimal(p,s)` | `DT_NUMERIC` | `DecimalType(p,s)` | `DECIMAL(p,s)` | Preserve precision/scale exactly |
| `numeric(p,s)` | `DT_NUMERIC` | `DecimalType(p,s)` | `DECIMAL(p,s)` | Same as decimal |
| `money` | `DT_CY` | `DecimalType(19,4)` | `DECIMAL(19,4)` | Fixed mapping |
| `smallmoney` | `DT_CY` | `DecimalType(10,4)` | `DECIMAL(10,4)` | Fixed mapping |

## String Types

| SQL Server | SSIS Data Type | Spark SQL | Delta Lake | Notes |
|---|---|---|---|---|
| `char(n)` | `DT_STR` | `StringType` | `STRING` | Spark doesn't enforce length |
| `varchar(n)` | `DT_STR` | `StringType` | `STRING` | Add CHECK constraint if needed |
| `varchar(max)` | `DT_TEXT` | `StringType` | `STRING` | |
| `nchar(n)` | `DT_WSTR` | `StringType` | `STRING` | Spark is always UTF-8 |
| `nvarchar(n)` | `DT_WSTR` | `StringType` | `STRING` | |
| `nvarchar(max)` | `DT_NTEXT` | `StringType` | `STRING` | |
| `text` | `DT_TEXT` | `StringType` | `STRING` | Deprecated in SQL Server |
| `ntext` | `DT_NTEXT` | `StringType` | `STRING` | Deprecated in SQL Server |

## Date/Time Types

| SQL Server | SSIS Data Type | Spark SQL | Delta Lake | Notes |
|---|---|---|---|---|
| `date` | `DT_DBDATE` | `DateType` | `DATE` | |
| `time` | `DT_DBTIME2` | `StringType` | `STRING` | No native time-only type in Spark |
| `datetime` | `DT_DBTIMESTAMP` | `TimestampType` | `TIMESTAMP` | Precision: milliseconds |
| `datetime2(p)` | `DT_DBTIMESTAMP2` | `TimestampType` | `TIMESTAMP` | Spark default precision: microseconds |
| `smalldatetime` | `DT_DBTIMESTAMP` | `TimestampType` | `TIMESTAMP` | Lower precision source |
| `datetimeoffset` | `DT_DBTIMESTAMPOFFSET` | `TimestampType` | `TIMESTAMP` | Timezone info may be lost — normalize to UTC |

## Binary Types

| SQL Server | SSIS Data Type | Spark SQL | Delta Lake | Notes |
|---|---|---|---|---|
| `binary(n)` | `DT_BYTES` | `BinaryType` | `BINARY` | |
| `varbinary(n)` | `DT_BYTES` | `BinaryType` | `BINARY` | |
| `varbinary(max)` | `DT_IMAGE` | `BinaryType` | `BINARY` | |
| `image` | `DT_IMAGE` | `BinaryType` | `BINARY` | Deprecated |

## Special Types

| SQL Server | SSIS Data Type | Spark SQL | Delta Lake | Notes |
|---|---|---|---|---|
| `uniqueidentifier` | `DT_GUID` | `StringType` | `STRING` | Store as string, format: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |
| `xml` | `DT_NTEXT` | `StringType` | `STRING` | Parse with `from_xml()` if needed |
| `json` | `DT_NTEXT` | `StringType` | `STRING` | Parse with `from_json()` + schema |
| `sql_variant` | `DT_WSTR` | `StringType` | `STRING` | Cast to string before migration |
| `hierarchyid` | `DT_BYTES` | `StringType` | `STRING` | Convert to path string (`/1/2/3/`) |
| `geography` | N/A | `StringType` | `STRING` | Convert to WKT or GeoJSON |
| `geometry` | N/A | `StringType` | `STRING` | Convert to WKT or GeoJSON |
| `rowversion` | `DT_BYTES` | `BinaryType` / `LongType` | `BIGINT` | Use as change tracking indicator |
| `timestamp` | `DT_BYTES` | `BinaryType` / `LongType` | `BIGINT` | Same as rowversion |

## SSIS Expression → PySpark Function Mapping

| SSIS Expression | PySpark Equivalent |
|---|---|
| `GETDATE()` | `F.current_timestamp()` |
| `YEAR(col)` | `F.year("col")` |
| `MONTH(col)` | `F.month("col")` |
| `DAY(col)` | `F.dayofmonth("col")` |
| `UPPER(col)` | `F.upper("col")` |
| `LOWER(col)` | `F.lower("col")` |
| `TRIM(col)` | `F.trim("col")` |
| `LEN(col)` | `F.length("col")` |
| `SUBSTRING(col, start, len)` | `F.substring("col", start, len)` |
| `REPLACE(col, old, new)` | `F.regexp_replace("col", old, new)` |
| `ISNULL(col)` | `F.isnull("col")` or `F.col("col").isNull()` |
| `(condition) ? true : false` | `F.when(condition, true_val).otherwise(false_val)` |
| `DATEADD(day, n, col)` | `F.date_add("col", n)` |
| `DATEDIFF(day, col1, col2)` | `F.datediff("col2", "col1")` |
| `CAST(col AS type)` | `F.col("col").cast("type")` |
| `CONCATENATE(a, "|", b)` | `F.concat("a", F.lit("\|"), "b")` |
| `HASHBYTES('SHA2_256', col)` | `F.sha2("col", 256)` |
