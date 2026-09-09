# Validation and Cutover

A converted mapping is not done when it runs. It is done when someone can prove
it produces the same output as the mapping it replaced, and can say what happens
if it does not. This file is the proof procedure.

## Contents

1. [The four gates](#1-the-four-gates)
2. [Gate 1 — static review](#2-gate-1--static-review)
3. [Gate 2 — synthetic data equivalence](#3-gate-2--synthetic-data-equivalence)
4. [Gate 3 — differential run on production data](#4-gate-3--differential-run-on-production-data)
5. [The reconciliation harness](#5-the-reconciliation-harness)
6. [Gate 4 — parallel run](#6-gate-4--parallel-run)
7. [Known-acceptable differences](#7-known-acceptable-differences)
8. [Cutover](#8-cutover)
9. [Sign-off checklist](#9-sign-off-checklist)

---

## 1. The four gates

| Gate | What it proves | Typical duration |
|---|---|---|
| 1. Static review | The conversion is faithful *by inspection* | hours |
| 2. Synthetic equivalence | Edge cases (nulls, ties, duplicates) behave identically | hours |
| 3. Differential run | Real production data produces identical output | days |
| 4. Parallel run | It stays identical across the business cycle | 2–6 weeks |

Do not skip Gate 2. It is the only gate that reliably catches null-handling and
tie-breaking differences, because production data rarely contains the awkward
cases on the day you test — and always contains them the week after cutover.

## 2. Gate 1 — static review

Check each item against the original mapping, not against the generated code's
own comments:

- [ ] Every source in the mapping appears in the function, with the same filter/SQL override.
- [ ] Every target is written, with the same load type (append / overwrite / merge).
- [ ] Row-count-changing transformations are all present: Filter, Router, Joiner, Aggregator, Rank, Normalizer, Union, Lookup with "Use All Values".
- [ ] Join types match the master/detail direction (see transformation-patterns §6).
- [ ] Lookup multiple-match policies are reproduced.
- [ ] Update Strategy flags map to the right `MERGE` clauses.
- [ ] Decimal vs Double matches the session's high-precision setting.
- [ ] Date format strings were converted, not copied.
- [ ] `DECODE`, `||`/`CONCAT`, `DATE_DIFF`, `GREATEST`/`LEAST` were converted with their null semantics.
- [ ] Reject handling exists wherever the mapping used `ERROR()` or could produce reject rows.
- [ ] Every `# RISK:` comment has been read and accepted by a named person.
- [ ] No credentials, connection strings or physical table names are hard-coded.
- [ ] `dry_run=True` genuinely writes nothing (grep the function for `.write`, `.save`, `MERGE`, `DELETE`, `spark.sql` DDL outside the guard).

That last one matters more than it looks. The whole validation approach depends
on `dry_run` being trustworthy.

## 3. Gate 2 — synthetic data equivalence

Build a small dataset — tens of rows, not millions — that deliberately contains
the cases the mapping's semantics turn on:

| Case | Why |
|---|---|
| NULL in every join key | Normal joins drop these; a NULL-safe join would not |
| NULL in every expression input | `DECODE`, `\|\|`, `GREATEST`, arithmetic |
| Duplicate keys in the lookup source | exercises the multiple-match policy |
| Duplicate keys in the merge source | Delta `MERGE` errors where PowerCenter did not |
| Ties on the Rank port | non-deterministic in both engines — establishes what you accept |
| Empty string vs NULL vs spaces | Informatica CHAR padding |
| Values at the decimal precision boundary | high-precision flag |
| Dates at month/quarter/year boundaries | `DATE_DIFF` fractions, `LAST_DAY`, `TRUNC` |
| Out-of-range and unparseable values | Informatica rejects, Spark nulls |
| Zero rows from a source | aggregates over empty groups |

Run the original mapping against this data in a PowerCenter dev environment if
one still exists. If it does not — common, and the reason many migrations skip
this gate — derive the expected output from the mapping logic by hand and encode
it as assertions. That is slower, but it is still far cheaper than discovering
the difference in production.

```python
def test_customer_dim_nulls(spark):
    src = spark.createDataFrame(
        [("C1", None, "US"), ("C2", "ACME", None), (None, "BETA", "GB")],
        "cust_id string, cust_name string, country_code string")
    out = run_m_customer_dim_load(
        spark, params={"load_date": "2024-01-15"},
        table_map={"src_customer": _temp_view(src), ...}, dry_run=True)
    # Informatica: NULL country_code survives the left join with NULL attributes
    assert out["row_counts"]["target"] == 3
```

## 4. Gate 3 — differential run on production data

Run both systems over the same input snapshot and compare outputs row by row.
The comparison must be on **content**, not on order — neither system guarantees
row order, and reconciling on order produces false failures that erode trust in
the harness.

Sequence:

1. Freeze a source snapshot (a Delta clone, or a dated extract).
2. Run the PowerCenter workflow against it, landing to a `_legacy` target.
3. Run the converted job with `dry_run=True`, materialising to a `_new` target.
4. Run the reconciliation harness below.
5. Triage every difference into: real bug, known-acceptable difference (§7), or
   legacy bug you are deliberately fixing. Every difference must land in one of
   those three buckets with a name attached — "probably fine" is not a bucket.

## 5. The reconciliation harness

```python
from pyspark.sql import DataFrame, functions as F

def reconcile(spark, legacy: DataFrame, new: DataFrame, keys: list,
              tolerance: float = 0.0, ignore_cols: list = None) -> dict:
    """Row-level comparison of two DataFrames. Returns a report dict; writes nothing."""
    ignore = set(ignore_cols or [])
    cols = [c for c in legacy.columns if c not in ignore]
    assert set(cols) == set(c for c in new.columns if c not in ignore), \
        f"schema mismatch: {set(cols) ^ set(new.columns)}"

    l = legacy.select(*cols)
    n = new.select(*cols)

    report = {
        "legacy_rows": l.count(),
        "new_rows":    n.count(),
        "dup_keys_legacy": l.groupBy(*keys).count().filter("count > 1").count(),
        "dup_keys_new":    n.groupBy(*keys).count().filter("count > 1").count(),
    }

    j = l.alias("l").join(n.alias("n"), keys, "full_outer")
    report["missing_in_new"]    = j.filter(F.col(f"l.{cols[-1]}").isNull() &
                                           F.col(f"n.{cols[-1]}").isNotNull()).count()
    report["missing_in_legacy"] = j.filter(F.col(f"n.{cols[-1]}").isNull() &
                                           F.col(f"l.{cols[-1]}").isNotNull()).count()

    # Column-level differences, NULL-safe, with numeric tolerance
    diffs = {}
    for c in cols:
        if c in keys:
            continue
        lc, nc = F.col(f"l.{c}"), F.col(f"n.{c}")
        if dict(l.dtypes)[c] in ("double", "float") or dict(l.dtypes)[c].startswith("decimal"):
            differs = ~(F.abs(lc.cast("double") - nc.cast("double")) <= F.lit(tolerance)) \
                      | (lc.isNull() != nc.isNull())
        else:
            differs = ~lc.eqNullSafe(nc)
        diffs[c] = j.filter(differs).count()
    report["column_diffs"] = {k: v for k, v in diffs.items() if v > 0}
    report["matched"] = all(v == 0 for v in diffs.values()) and \
                        report["missing_in_new"] == 0 and report["missing_in_legacy"] == 0
    return report
```

Points that make the difference between a harness people trust and one they mute:

- **`eqNullSafe`, always.** A plain `!=` on two NULLs yields NULL, which filters
  the row out, so a harness using `!=` reports "no differences" on columns that
  are entirely NULL in one system. This is the most common way a reconciliation
  gives a false pass.
- **Tolerance only on floats**, and set it from the business, not from
  convenience. If the high-precision flag was off in the session, expect
  differences in the 15th significant digit and set tolerance accordingly — but
  record that you did.
- **Check duplicate keys on both sides** before comparing. A full outer join on
  non-unique keys fans out and produces nonsense difference counts.
- **Exclude genuinely non-deterministic columns** (`load_ts`, surrogate keys
  from a Sequence Generator, `RAND` output) via `ignore_cols` — and list them in
  the sign-off, so nobody later assumes surrogate keys were verified.
- **Sample the differences**, don't just count them. Add
  `j.filter(differs).limit(20)` output to the report for triage.

Also reconcile the **aggregates that the business actually reports on** — sum of
amounts by month and region, distinct customer counts — because those catch
compensating errors that row-level matching can miss when the ignore list grows.

## 6. Gate 4 — parallel run

Run both systems on the real daily schedule, comparing outputs each cycle, for
long enough to cover the business cycle: at minimum through a month-end close,
and through whatever periodic process the warehouse has (quarter-end, a reload,
a late-arriving-data scenario).

Track a simple daily record: date, mapping, legacy rows, new rows, differing
rows, unresolved differences. A run of clean days is the argument for cutover;
one bad day resets the clock and tells you what to fix.

Watch specifically for what only appears over time:

- Late-arriving data and restatements.
- The first retry after a failure (idempotency — see orchestration §10).
- The first run where a persistent variable / watermark matters.
- Month-end volume spikes against cluster sizing.
- Daylight-saving transitions, if the schedule is timezone-sensitive.

## 7. Known-acceptable differences

Document these up front so triage is fast, and get them accepted in writing:

| Difference | Cause | Usual disposition |
|---|---|---|
| Surrogate key values differ | Sequence Generator → IDENTITY | accept if keys are opaque; reconcile on business keys |
| Row order in the target | neither engine guarantees order | accept |
| Trailing-space handling | CHAR padding | decide and apply consistently; usually trim |
| 15th-digit numeric differences | high-precision flag off in the legacy session | accept with a stated tolerance |
| Tied rows chosen by Rank | non-deterministic in both | accept, or add a tie-breaker to both sides |
| Reject-row counts | Spark nulls where Informatica rejected | fix — this one usually indicates a real gap |
| Load timestamps | different clocks | exclude from comparison |

Anything not on this list is a bug until proven otherwise.

## 8. Cutover

1. Freeze changes to the PowerCenter mapping. Concurrent changes to both sides
   during parallel run is the single most reliable way to lose weeks.
2. Confirm the clean-run streak and get sign-off.
3. Switch the enterprise scheduler (or the job schedule) to the Databricks job;
   leave the PowerCenter workflow scheduled but disabled rather than deleted.
4. Keep the legacy targets readable for the agreed rollback window — a Delta
   `CLONE` of the pre-cutover state costs almost nothing and makes rollback a
   metadata operation rather than a restore.
5. Run with heightened monitoring for one full cycle: row-count alerts against
   historical ranges, reject-count alerts, runtime alerts.
6. Decommission only after the rollback window closes. Archive the mapping XML
   with the converted code in the same repository — it is the only remaining
   specification of what the pipeline is supposed to do.

## 9. Sign-off checklist

- [ ] All four gates passed, with results recorded.
- [ ] Every `# RISK:` flag has a named owner and a decision.
- [ ] Known-acceptable differences accepted in writing by the data owner.
- [ ] Audit/control table populated on every run (mapping, run id, counts, timestamps).
- [ ] Reject handling verified end-to-end, including that someone monitors the reject table.
- [ ] Idempotency verified: a task killed mid-run and retried produces the same result.
- [ ] Watermarks/persistent variables verified across a failed-then-retried run.
- [ ] Unity Catalog grants match or tighten the legacy access model — never loosen it silently.
- [ ] Runbook updated: how to rerun a single mapping, how to backfill a date range, who to call.
- [ ] Rollback procedure written and, ideally, rehearsed once.
