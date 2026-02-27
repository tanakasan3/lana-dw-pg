# Known Issues - BigQuery to PostgreSQL Migration

The dbt models were originally written for BigQuery. While many have been converted, some BigQuery-specific patterns remain.

## Remaining BigQuery-Specific SQL

### 1. `* EXCEPT(...)` syntax (23 files)
BigQuery allows `SELECT * EXCEPT(col1, col2)`. PostgreSQL doesn't support this.

**Files affected:**
```
models/intermediate/rollups/*.sql (most files)
models/intermediate/int_*.sql (several files)
```

**Fix:** Manually list all columns instead of using `* EXCEPT`.

### 2. `safe_divide()` function (2 files with multi-line calls)
- `models/intermediate/int_approved_credit_facility_loan_cash_flows.sql`
- `models/intermediate/int_approved_credit_facility_loans.sql`

**Fix:** Replace with `(numerator)::numeric / nullif((denominator)::numeric, 0)`

### 3. `timestamp_diff()` function (3 files)
BigQuery: `timestamp_diff(ts1, ts2, unit)`
PostgreSQL: `EXTRACT(EPOCH FROM ts1 - ts2)` or date arithmetic

### 4. `ends_with()` function (1 file)
BigQuery: `ends_with(str, suffix)`
PostgreSQL: `str LIKE '%' || suffix` or `right(str, length(suffix)) = suffix`

### 5. `last_day()` function (2 files)
BigQuery: `last_day(date, YEAR)`
PostgreSQL: `(date_trunc('year', date) + interval '1 year' - interval '1 day')::date`

## Workaround

For initial testing, you may want to exclude the problematic models:

```bash
# Run seeds and simpler models only
dbt run --exclude tag:complex_bq_syntax
```

Or selectively materialize working assets via the Dagster UI.

## Contributing Fixes

PRs welcome! When fixing a file:
1. Replace BigQuery syntax with PostgreSQL equivalent
2. Test with `dbt compile` and `dbt run`
3. Update this file to remove from the list
