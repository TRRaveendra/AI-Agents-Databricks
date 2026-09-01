# Production Readiness Checklist

This checklist must be reviewed and completed before any data engineering pipeline is considered production-ready.

## Architecture

✓ **Correct layer placement**  
- Bronze: Raw ingestion only, no transformations  
- Silver: Cleansing, validation, standardization  
- Gold: Business logic, aggregations, analytics-ready  

✓ **Appropriate batch vs streaming choice**  
- Batch: Scheduled/triggered, acceptable latency  
- Streaming: Continuous, low-latency requirements  

✓ **Table names follow conventions**  
- Bronze: `raw_<source>_<entity>`  
- Silver: `<entity>`  
- Gold: `<business_context>`  

## Data Ingestion

✓ **Correct ingestion strategy implemented**  
- Full load: Small tables, no reliable timestamp  
- Incremental: Large tables, reliable timestamp/CDC  
- Auto Loader: File-based ingestion from cloud storage  

✓ **Watermark tracking in place (for incremental)**  
- Last processed timestamp tracked  
- Buffer included for clock skew  

✓ **Idempotent ingestion**  
- Re-running doesn't create duplicates  
- Checkpointing in place for streaming  

✓ **All audit columns included**  
- `_ingested_timestamp`  
- `_processed_timestamp`  
- `_source_file` (if file-based)  
- `_source_system`  

## Change Data Capture / SCD

✓ **CDC handled correctly (if applicable)**  
- INSERT/UPDATE/DELETE operations processed  
- MERGE statement used for upserts  
- Change Data Feed enabled if needed downstream  

✓ **SCD implemented correctly (if applicable)**  
- Type 1: Overwrite logic confirmed  
- Type 2: Surrogate key, effective dates, is_current flag  
- Business key correctly identified  

✓ **Late-arriving data handled**  
- Grace period defined  
- Merge logic handles out-of-order records  

## Data Quality

✓ **Duplicate handling implemented**  
- Deduplication logic in place  
- Most recent record kept by timestamp  

✓ **Null validation implemented**  
- Required columns validated  
- Null checks flag invalid records  

✓ **Data type validation**  
- Types cast correctly (dates, decimals, integers)  
- Invalid formats handled gracefully  

✓ **Business rule validation**  
- Range checks (amounts > 0, dates not future)  
- Status values in allowed list  
- Cross-field validation rules applied  

✓ **Referential integrity checked**  
- Foreign key relationships validated  
- Orphaned records handled (quarantined or rejected)  

✓ **Quarantine table created**  
- Invalid records stored in `<table>_quarantine`  
- Validation errors captured  

✓ **Reconciliation implemented**  
- Row count reconciliation  
- Sum/aggregate reconciliation  
- Alerts on threshold violations  

## Code Quality

✓ **PySpark/SQL standards followed**  
- Naming conventions used  
- Code readable and maintainable  
- Transformation functions modular and reusable  

✓ **No hardcoded values**  
- Credentials in secret scopes  
- Configuration in parameters/tables  
- File paths parameterized  

✓ **Error handling included**  
- Try-except blocks around risky operations  
- Errors logged with context  
- Graceful failure (don't lose partial progress)  

✓ **Logging implemented**  
- Start/end timestamps  
- Row counts processed  
- Errors and warnings logged  

## Performance

✓ **Table design optimized**  
- Liquid clustering on large tables (> 1GB)  
- Clustering columns match common filters  
- Legacy partitioning avoided (unless specific need)  

✓ **Auto-optimization enabled**  
- `delta.autoOptimize.optimizeWrite = true`  
- `delta.autoOptimize.autoCompact = true`  

✓ **Small files problem addressed**  
- OPTIMIZE scheduled or auto-enabled  
- Coalesce/repartition before writes  
- File sizes 100MB - 1GB  

✓ **Joins optimized**  
- Small tables broadcasted  
- Data skew handled (salting if needed)  
- Join order optimized (small first)  

✓ **Caching used appropriately**  
- DataFrames used multiple times cached  
- Unpersist when done  

✓ **Unnecessary shuffles avoided**  
- Filter before group/join  
- Pre-partition when beneficial  

## Testing

✓ **Unit tests created**  
- Transformation functions tested in isolation  
- Edge cases covered (nulls, duplicates, empty)  

✓ **Integration tests created**  
- Full pipeline tested end-to-end  
- Test data generated programmatically  

✓ **Data quality tests created**  
- Validation rules tested  
- Quarantine logic tested  

✓ **Reconciliation tests created**  
- Row count and sum reconciliation verified  

✓ **Regression tests run**  
- Existing tests still pass  
- Metrics haven't changed unexpectedly  

## Security & Governance

✓ **Unity Catalog used**  
- Tables created in proper catalog.schema  
- Three-level namespace followed  

✓ **Permissions configured**  
- Least privilege applied  
- Service principal used (not personal account)  

✓ **No hardcoded credentials**  
- Secrets in secret scopes  
- Service principal for cloud access  

✓ **PII identified and protected**  
- PII columns tagged  
- Masked in non-production environments  

✓ **Audit logging enabled**  
- Access tracked  
- Changes logged  

✓ **Data lineage documented**  
- Source systems documented  
- Transformation logic described  
- Downstream dependencies known  

## Monitoring & Alerting

✓ **Job monitoring configured**  
- Scheduled job exists  
- Retry logic in place  
- Timeout configured appropriately  

✓ **Alerts configured**  
- Job failure alerts  
- Data quality threshold alerts (> 5% quarantine rate)  
- SLA breach alerts (freshness)  

✓ **Metrics tracked**  
- Row counts ingested/processed  
- Pipeline duration  
- Error rates  

## Documentation

✓ **README created**  
- Purpose of pipeline  
- Data sources and targets  
- Schedule and frequency  
- Key transformations and business logic  
- Dependencies  

✓ **Runbook created**  
- How to run pipeline manually  
- How to troubleshoot common issues  
- Escalation contacts  

✓ **Business logic documented**  
- Complex transformations explained  
- Assumptions documented  
- Edge cases described  

## Final Verification

✓ **End-to-end test completed successfully**  
- Full pipeline run from start to finish  
- All tables populated correctly  
- Data quality checks pass  
- Reconciliation successful  

✓ **Performance acceptable**  
- Pipeline completes within SLA  
- No OOM errors or timeouts  
- Resource usage reasonable  

✓ **Code reviewed**  
- Peer review completed  
- Feedback addressed  
- Approved by team lead  

✓ **Production deployment plan**  
- Deployment steps documented  
- Rollback plan in place  
- Stakeholders notified  

---

## Sign-Off

Before marking work as complete, confirm:

1. All items above checked ✓  
2. All tests passing  
3. Code reviewed and approved  
4. Documentation complete  
5. Ready for production deployment  

**If any item is NOT checked, the pipeline is NOT production-ready.**

---

**Remember**: This checklist exists to prevent production issues. Don't skip items or rush through. Quality over speed.
