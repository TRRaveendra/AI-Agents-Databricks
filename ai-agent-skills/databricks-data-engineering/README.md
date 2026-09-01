# Databricks Data Engineering Skill

A comprehensive, production-grade skill for Databricks Genie Code covering all aspects of data engineering on Databricks.

## 📋 What This Skill Covers

This skill provides battle-tested patterns and best practices for:

* **Medallion Architecture** - Bronze, Silver, Gold layer design and implementation
* **Data Ingestion** - Full load, incremental, Auto Loader, CDC, API ingestion
* **Streaming Patterns** - Auto Loader, Structured Streaming, watermarks, triggers
* **Delta Lake Features** - Time Travel, MERGE, CDF, OPTIMIZE, VACUUM, constraints
* **Unity Catalog** - Volumes (managed/external), external locations, governance
* **Data Quality** - Validation, quarantine, reconciliation patterns
* **Table Design** - Liquid clustering, Delta tables, SCD implementations
* **Performance** - AQE, Photon, optimization techniques, small files, data skew handling
* **Testing** - Unit, integration, data quality tests
* **Security** - Unity Catalog, secrets management, PII handling, row/column security
* **Production Readiness** - Complete checklist before deployment

## 📁 Skill Structure

```
databricks-data-engineering/
├── SKILL.md                          # Main skill orchestrator
├── README.md                         # This file
├── references/                       # Reference documentation (15 files)
│   ├── bronze.md                     # Bronze layer deep-dive
│   ├── silver.md                     # Silver layer deep-dive
│   ├── gold.md                       # Gold layer deep-dive
│   ├── 01-architecture.md            # Medallion architecture overview
│   ├── 02-ingestion.md              # Data ingestion patterns
│   ├── 03-incremental-cdc.md        # CDC and incremental processing
│   ├── 04-scd.md                    # Slowly Changing Dimensions
│   ├── 05-data-quality.md           # Data quality patterns
│   ├── 06-pyspark-standards.md      # PySpark coding standards
│   ├── 07-sql-standards.md          # SQL best practices
│   ├── 08-delta-table-design.md     # Delta table design
│   ├── 09-performance.md            # Performance optimization
│   ├── 10-testing.md                # Testing patterns
│   ├── 11-security-governance.md    # Security & governance
│   └── 12-production-checklist.md   # Production readiness gate
└── code/                            # Working code examples
    └── examples/
        ├── clustered_tables.py      # Liquid clustering examples
        ├── partitioned_tables.py    # Legacy partitioning (when needed)
        └── scd_type2_tables.py      # Complete SCD Type 2 implementation

Total: 1 main file + 15 references + 3 code examples = 19 files
```

## 🎯 When to Use This Skill

Genie Code automatically loads this skill when you ask to:

* Build data pipelines or ETL/ELT workflows
* Implement bronze/silver/gold medallion architecture
* Handle CDC (Change Data Capture) or incremental processing
* Implement SCD (Slowly Changing Dimensions)
* Optimize data engineering workflows
* Need data engineering best practices on Databricks

## 🚀 How Genie Code Uses This Skill

### Example 1: Building a Pipeline
**User asks**: *"Build an incremental data pipeline with CDC"*

**Genie Code workflow**:
1. Reads `bronze.md` → Understands raw data ingestion
2. Reads `02-ingestion.md` → Chooses Auto Loader pattern
3. Reads `03-incremental-cdc.md` → Implements CDC with MERGE
4. Reads `silver.md` → Applies cleansing and validation
5. Reads `05-data-quality.md` → Adds quarantine logic
6. Reads `12-production-checklist.md` → Verifies completeness

**Result**: Production-ready pipeline with proper architecture, error handling, and data quality checks.

### Example 2: Performance Optimization
**User asks**: *"Why is my Delta table query slow?"*

**Genie Code workflow**:
1. Reads `09-performance.md` → Identifies common issues (small files, no clustering, data skew)
2. Reads `08-delta-table-design.md` → Checks table configuration
3. Sees `code/examples/clustered_tables.py` → Applies liquid clustering pattern
4. Generates optimization commands with explanations

**Result**: Optimized table with liquid clustering and auto-compaction enabled.

### Example 3: SCD Implementation
**User asks**: *"Implement SCD Type 2 for customer dimension"*

**Genie Code workflow**:
1. Reads `04-scd.md` → Understands SCD Type 2 concepts
2. Reads `gold.md` → Understands dimension table design
3. Sees `code/examples/scd_type2_tables.py` → Uses complete working example
4. Applies pattern with surrogate keys, effective dates, is_current flag

**Result**: Fully functional SCD Type 2 dimension with history tracking.

## 🏆 Key Features

### 1. Modular Design
- Small, focused reference files (each < 10KB)
- Easy to navigate and maintain
- Agent reads only relevant files

### 2. Production-Ready Patterns
- Real PySpark/SQL code examples
- Error handling included
- Performance optimizations built-in
- Security best practices enforced

### 3. Complete Coverage
Every aspect of data engineering:
- ✅ Architecture design
- ✅ Data ingestion
- ✅ Data quality
- ✅ Performance
- ✅ Testing
- ✅ Security
- ✅ Production checklist

### 4. Working Code Examples
Not just theory - actual runnable code:
- Liquid clustering implementation
- SCD Type 2 complete example
- Incremental CDC patterns
- Data quality validation

### 5. Production Checklist
Before any work is marked complete, the agent reviews:
- Architecture correctness
- Idempotency
- Data quality checks
- Error handling
- Security (no hardcoded secrets)
- Performance optimization
- Testing
- Documentation

## 📚 Learning Path

### For Beginners
1. Start with `01-architecture.md` - Understand medallion layers
2. Read layer-specific guides: `bronze.md`, `silver.md`, `gold.md`
3. See `02-ingestion.md` - Learn ingestion patterns
4. Review `12-production-checklist.md` - Understand requirements

### For Intermediate Engineers
1. Deep-dive into `03-incremental-cdc.md` and `04-scd.md`
2. Study `08-delta-table-design.md` for table optimization
3. Master `09-performance.md` for performance tuning
4. Review `10-testing.md` for comprehensive testing

### For Advanced Users
1. Customize patterns for your organization
2. Add domain-specific reference files
3. Extend code examples with your use cases
4. Integrate with CI/CD pipelines

## 🔧 Customization

### Adding Your Own Patterns
1. Create new `.md` file in `references/`
2. Follow existing file format (clear sections, code examples)
3. Update `SKILL.md` quick reference map
4. Add working code examples in `code/examples/`

### Organization-Specific Standards
Extend references with your standards:
- Naming conventions
- Approval workflows
- Required metadata
- Compliance requirements

## ✅ Production Checklist Highlights

Before deployment, verify:

**Architecture**
- ✓ Correct layer placement (Bronze/Silver/Gold)
- ✓ Idempotent ingestion
- ✓ All audit columns included

**Data Quality**
- ✓ Null validation implemented
- ✓ Duplicate handling
- ✓ Referential integrity checks
- ✓ Quarantine table created

**Performance**
- ✓ Liquid clustering enabled
- ✓ Small files addressed
- ✓ Auto-optimization enabled

**Security**
- ✓ No hardcoded credentials
- ✓ Unity Catalog used
- ✓ PII properly handled

**Testing**
- ✓ Unit tests created
- ✓ Data quality tests
- ✓ Reconciliation verified

## 🌟 Best Practices Enforced

1. **Bronze = Raw** - No transformations, preserve everything
2. **Silver = Clean** - Validate, standardize, quarantine bad data
3. **Gold = Business** - Aggregates, star schema, optimized for queries
4. **Liquid Clustering > Partitioning** - Default to clustering for optimization
5. **Quarantine, Don't Drop** - Always preserve invalid records for investigation
6. **MERGE for Upserts** - More efficient than delete + insert
7. **Test Before Production** - Unit, integration, and data quality tests
8. **Document Business Logic** - Complex transformations need clear explanations

## 📊 Metrics & Validation

Every pattern includes:
- Row count reconciliation
- Data quality metrics (% valid records)
- Performance benchmarks
- Monitoring queries

## 🔗 See Also

- [Databricks Documentation](https://docs.databricks.com/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)

## 📝 Version History

- **v1.1** - Enhanced with comprehensive Databricks best practices (17 references + 3 code examples)
  - ✨ NEW: Unity Catalog Volumes and External Locations (`13-unity-catalog-storage.md`)
  - ✨ NEW: Streaming Patterns with Auto Loader and Structured Streaming (`14-streaming-patterns.md`)
  - 🚀 ENHANCED: Delta Lake features - Time Travel, MERGE, CDF, OPTIMIZE, VACUUM, constraints (`08-delta-table-design.md`)
  - 🚀 ENHANCED: Performance optimization - AQE, Photon, liquid clustering, query tuning (`09-performance.md`)
  - 🚀 ENHANCED: Unity Catalog security - row/column security, data lineage, tagging (`11-security-governance.md`)
  - Complete Delta Lake feature coverage
  - Production-grade streaming patterns
  - Cost optimization strategies

- **v1.0** - Initial release with 15 references + 3 code examples
  - Medallion architecture (Bronze/Silver/Gold)
  - Complete data engineering lifecycle
  - Production checklist
  - Working code examples

---

**Maintained by**: Databricks Data Engineering Team  
**Last Updated**: 2024  
**Status**: Production Ready ✅

*This skill represents battle-tested patterns from real-world Databricks data engineering projects.*
