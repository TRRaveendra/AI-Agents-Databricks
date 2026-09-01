---
name: databricks-data-engineering
description: Comprehensive guidance for Databricks data engineering projects including medallion architecture (bronze/silver/gold layers), ingestion patterns, incremental/CDC processing, SCD, data quality, PySpark/SQL standards, Delta Lake features (time travel, MERGE, CDF, OPTIMIZE, VACUUM), Unity Catalog (volumes, external locations), streaming patterns (Auto Loader, structured streaming), performance optimization (AQE, Photon, liquid clustering), testing, security, and production readiness. Read this skill when the user asks to build data pipelines, implement bronze/silver/gold layers, handle CDC or SCD, work with Unity Catalog volumes, use Auto Loader, optimize data engineering workflows, or wants data engineering best practices on Databricks.
---

# Databricks Data Engineering Skill

**When to use this skill**: Load when the user is building data engineering pipelines, implementing medallion architecture, handling data ingestion/transformation, or needs guidance on production-grade data engineering patterns on Databricks.

## How to use this skill

This skill contains focused reference files covering common data engineering patterns. **Read the relevant reference(s) before writing code or providing guidance.**

### Quick reference map

**Medallion Architecture (Bronze/Silver/Gold)**
- Bronze layer (raw ingestion)? → Read [`references/bronze.md`](references/bronze.md)
- Silver layer (cleansing/validation)? → Read [`references/silver.md`](references/silver.md)
- Gold layer (business logic/aggregates)? → Read [`references/gold.md`](references/gold.md)
- Architecture overview? → Read [`references/01-architecture.md`](references/01-architecture.md)

**Data Ingestion & Processing**
- Loading data (full/incremental)? → Read [`references/02-ingestion.md`](references/02-ingestion.md)
- CDC, watermarks, late data? → Read [`references/03-incremental-cdc.md`](references/03-incremental-cdc.md)
- Slowly changing dimensions? → Read [`references/04-scd.md`](references/04-scd.md)

**Code Quality**
- Writing PySpark? → Read [`references/06-pyspark-standards.md`](references/06-pyspark-standards.md)
- Writing SQL? → Read [`references/07-sql-standards.md`](references/07-sql-standards.md)
- Data validation? → Read [`references/05-data-quality.md`](references/05-data-quality.md)

**Table Design & Optimization**
- Delta table decisions? → Read [`references/08-delta-table-design.md`](references/08-delta-table-design.md)
- Performance issues? → Read [`references/09-performance.md`](references/09-performance.md)

**Production Readiness**
- Creating tests? → Read [`references/10-testing.md`](references/10-testing.md)
- Security/governance? → Read [`references/11-security-governance.md`](references/11-security-governance.md)
- **Before completing work** → Read [`references/12-production-checklist.md`](references/12-production-checklist.md)

**Code Examples**
- Clustered tables? → See [`code/examples/clustered_tables.py`](code/examples/clustered_tables.py)
- Partitioned tables? → See [`code/examples/partitioned_tables.py`](code/examples/partitioned_tables.py)
- SCD Type 2? → See [`code/examples/scd_type2_tables.py`](code/examples/scd_type2_tables.py)

## Core principles

1. **Always check the production checklist** before saying work is complete
2. **Apply medallion patterns** consistently (bronze → silver → gold)
3. **Design for idempotency** from the start
4. **Include audit columns** in every table
5. **Implement data quality checks** at appropriate layers
6. **Follow PySpark/SQL standards** in all code
7. **Optimize Delta tables** with liquid clustering, not legacy partitioning
8. **Test thoroughly** before production deployment
9. **Never hardcode secrets** - use Databricks secrets or service principals
10. **Document assumptions** and business logic clearly

## Agent guidance

When the user requests data engineering work:

1. **Understand the requirement** - full load vs incremental, batch vs streaming, SCD requirements
2. **Load relevant references** - Don't guess; read the appropriate reference files first
3. **Apply patterns consistently** - Follow the standards in the reference files
4. **Generate production-ready code** - Include error handling, logging, and data quality checks
5. **Verify against checklist** - Review production checklist before completion
6. **Explain key decisions** - Help the user understand why patterns were chosen

## Common workflows

### Building a new pipeline
1. Read medallion layer references (`bronze.md`, `silver.md`, `gold.md`) - Understand responsibilities
2. Read `02-ingestion.md` - Choose ingestion pattern
3. Read appropriate pattern file (`03-incremental-cdc.md` or `04-scd.md`)
4. Read `05-data-quality.md` - Add validation
5. Read code standards (`06-pyspark-standards.md` or `07-sql-standards.md`)
6. Read `08-delta-table-design.md` - Design tables (clustering, properties)
7. Read `09-performance.md` - Optimize
8. Read `10-testing.md` - Create tests
9. Review `12-production-checklist.md` - Final verification

### Implementing CDC/SCD
1. Read `03-incremental-cdc.md` or `04-scd.md` based on requirement
2. See `code/examples/scd_type2_tables.py` for working examples
3. Read `02-ingestion.md` for ingestion strategy
4. Read `05-data-quality.md` for validation patterns
5. Read relevant code standards
6. Review `12-production-checklist.md`

### Optimizing table performance
1. Read `09-performance.md` - Identify common issues
2. Read `08-delta-table-design.md` - Check table configuration
3. See `code/examples/clustered_tables.py` for clustering patterns
4. Apply optimizations, measure results

### Building streaming pipelines
1. Read `14-streaming-patterns.md` - Auto Loader and Structured Streaming
2. Read medallion layer references (`bronze.md`, `silver.md`, `gold.md`) - Layer design
3. Read `08-delta-table-design.md` - Enable CDF for incremental processing
4. Read `09-performance.md` - Optimize streaming performance
5. Review `12-production-checklist.md` - Production readiness

### Working with Unity Catalog volumes and external locations
1. Read `13-unity-catalog-storage.md` - Volumes and external locations
2. Read `11-security-governance.md` - Access control and permissions
3. Read `02-ingestion.md` - Ingestion patterns from volumes
4. Review `12-production-checklist.md` - Security and governance checks

---

**Remember**: These references represent battle-tested patterns. Follow them closely for production-grade results. Always check code examples for complete working implementations.
