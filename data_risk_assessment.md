# Data Risk Assessment Report
## NHS Claims Migration: Oracle → Apache Spark (Microsoft Fabric)

> **Disclaimer:** This report is based on a mocked dataset and fictional system architecture, 
> inspired by real-world experience in NHS-adjacent healthcare data migrations. 
> No real patient data, system schemas, or organisational details are used.

---

**Report Date:** 2024-03-15  
**Dataset:** `nhs_healthcare_claims` (5,000 mock records)  
**Migration Path:** Oracle DB → Apache Spark / Microsoft Fabric Lakehouse  
**Author:** Data Solutions Engineer  
**Classification:** Internal / Portfolio

---

## 1. Executive Summary

This risk assessment evaluates the key data risks identified during the proof-of-concept migration 
of an NHS-style healthcare claims dataset from a legacy Oracle relational database to 
an Apache Spark environment hosted on Microsoft Fabric.

Three critical risk areas were identified, alongside several medium and low-severity concerns. 
The migration was validated using a custom Python-based framework. Overall data fidelity 
was confirmed at **99.96%** with controlled remediation steps applied.

---

## 2. Dataset Overview

| Attribute               | Value                           |
|-------------------------|---------------------------------|
| Table Name              | `nhs_claims`                    |
| Row Count (Source)      | 5,000                           |
| Row Count (Target)      | 5,000                           |
| Key Columns             | claim_id, nhs_trust_id, provider_id, claimed_amount, approved_amount |
| Date Range              | 2022-01-01 to 2024-12-31        |
| Financial Years         | FY2022-23, FY2023-24, FY2024-25 |
| Claim Statuses          | APPROVED, PENDING, REJECTED, UNDER_REVIEW |

---

## 3. Risk Register

### 🔴 HIGH Risk

#### R-001: NULL Handling Differences (NVL vs COALESCE)

| Field           | Detail                                       |
|-----------------|----------------------------------------------|
| **Risk**        | Oracle `NVL()` behaves differently from Spark `COALESCE()` for certain edge cases involving implicit type casting. |
| **Impact**      | Financial aggregation queries could silently produce incorrect totals if `NVL` patterns are not fully replaced. |
| **Affected Columns** | `approved_amount`, `claimed_amount`      |
| **Mitigation**  | All `NVL` usages replaced with `COALESCE`. Post-migration aggregate comparison confirmed within 0.01% tolerance. |
| **Status**      | ✅ Resolved                                  |

---

#### R-002: Date Format Mask Case Sensitivity

| Field           | Detail                                       |
|-----------------|----------------------------------------------|
| **Risk**        | Oracle `TO_CHAR(date, 'YYYY-MM')` uses uppercase format masks. Spark's `DATE_FORMAT` requires lowercase (`'yyyy-MM'`). If not updated, date strings silently return wrong or null values. |
| **Impact**      | Monthly reporting aggregations produce incorrect groupings. |
| **Affected Queries** | Q4 (Monthly Volumes), any BI layer date dimensions |
| **Mitigation**  | SQL audit completed. All date format strings updated to Spark-compatible lowercase masks. |
| **Status**      | ✅ Resolved                                  |

---

#### R-003: DECODE() Not Supported in Spark SQL

| Field           | Detail                                       |
|-----------------|----------------------------------------------|
| **Risk**        | Oracle `DECODE()` is not a supported function in Spark SQL. Queries containing `DECODE` will fail at runtime rather than throwing a parse error in some environments. |
| **Impact**      | Potential silent failures in status-labelling queries if not caught in testing. |
| **Mitigation**  | All `DECODE` expressions replaced with equivalent `CASE WHEN ... THEN ... END` blocks. |
| **Status**      | ✅ Resolved                                  |

---

### 🟡 MEDIUM Risk

#### R-004: LISTAGG Replacement Behaviour

| Field           | Detail                                       |
|-----------------|----------------------------------------------|
| **Risk**        | Oracle `LISTAGG(DISTINCT ...) WITHIN GROUP (ORDER BY ...)` provides deterministic distinct-ordered string aggregation. The Spark equivalent (`ARRAY_JOIN(SORT_ARRAY(COLLECT_SET(...)))`) produces the same result but has different performance characteristics at scale. |
| **Impact**      | Non-functional at small scale. At large data volumes (>1M rows), COLLECT_SET can be memory-intensive. |
| **Mitigation**  | Accepted for PoC. Flagged for performance review at production scale. |
| **Status**      | ⚠️ Accepted (low data volume)               |

---

#### R-005: Approved Amount Nulls in APPROVED Records

| Field           | Detail                                       |
|-----------------|----------------------------------------------|
| **Risk**        | During data quality analysis, **47 records** with `claim_status = 'APPROVED'` were found to have a NULL `approved_amount`. This is a data quality issue in the **source system**, not introduced by migration. |
| **Impact**      | Financial reporting will undercount total approved values. |
| **Mitigation**  | Flagged to source system owners. Validation check added to detect this pattern in future loads. Downstream reports use `COALESCE(approved_amount, 0)` as a defensive measure. |
| **Status**      | ⚠️ Known source defect — mitigated in reports |

---

#### R-006: Diagnosis Code Nulls

| Field           | Detail                                       |
|-----------------|----------------------------------------------|
| **Risk**        | Approximately 2% of records have a NULL `diagnosis_code`. |
| **Impact**      | Clinical reporting by diagnosis category will undercount. |
| **Mitigation**  | Null records retained; dimension queries use `COALESCE(diagnosis_code, 'UNKNOWN')` for grouping. |
| **Status**      | ⚠️ Accepted — tracked in data quality log    |

---

### 🟢 LOW Risk

#### R-007: Financial Year Derivation Logic

| Field           | Detail                                       |
|-----------------|----------------------------------------------|
| **Risk**        | `financial_year` was pre-calculated in Oracle using a stored procedure. In Spark, this must be derived inline using `CASE WHEN MONTH(date) >= 4 THEN ...`. Logic must be validated to ensure UK fiscal year boundary (April 1) is handled correctly. |
| **Mitigation**  | Logic validated against Oracle output. No discrepancies found. |
| **Status**      | ✅ Resolved                                  |

---

#### R-008: Row-level Duplicate Detection

| Field           | Detail                                       |
|-----------------|----------------------------------------------|
| **Risk**        | Initial Spark ingestion using `COPY INTO` without deduplication can introduce duplicates if source files are re-processed. |
| **Mitigation**  | Idempotent load pattern implemented using `MERGE INTO` (Delta Lake). `claim_id` used as surrogate key for deduplication. |
| **Status**      | ✅ Resolved                                  |

---

## 4. Validation Summary

The following checks were run using the custom `MigrationValidator` framework:

| Check                        | Result  | Detail                                               |
|------------------------------|---------|------------------------------------------------------|
| Row Count                    | ✅ PASS | 5,000 rows in both source and target                 |
| Schema Check                 | ✅ PASS | All 16 columns present and correctly typed           |
| PK Completeness              | ✅ PASS | All `claim_id` values accounted for                  |
| Null Check: approved_amount  | ⚠️ WARN | 47 nulls in approved status — source defect          |
| Null Check: diagnosis_code   | ⚠️ WARN | ~2% null rate — expected and documented              |
| Aggregate: claimed_amount    | ✅ PASS | Sum delta < 0.001%                                   |
| Aggregate: approved_amount   | ✅ PASS | Sum delta < 0.001%                                   |
| Duplicate Rows               | ✅ PASS | No duplicates in source or target                    |
| Distribution: claim_status   | ✅ PASS | All 4 status values present in both                  |
| Distribution: patient_gender | ✅ PASS | M / F / U categories match                           |

---

## 5. Data Flow Diagram

```
┌───────────────────────────────────────────────────────────┐
│                    SOURCE SYSTEM                           │
│    Oracle Database (Legacy On-Premise)                    │
│    Tables: nhs_claims, providers, nhs_trusts              │
└──────────────────────┬────────────────────────────────────┘
                       │  Extract (CSV / ADF pipeline)
                       ▼
┌───────────────────────────────────────────────────────────┐
│                 STAGING LAYER                             │
│    Microsoft Fabric — OneLake (Bronze / Raw Zone)        │
│    Format: Parquet / Delta Lake                           │
└──────────────────────┬────────────────────────────────────┘
                       │  Spark transformations
                       ▼
┌───────────────────────────────────────────────────────────┐
│              VALIDATION FRAMEWORK (Python)                │
│    MigrationValidator.run_all_checks()                   │
│    • Row counts  • Nulls  • Aggregates                   │
│    • PK checks   • Distributions  • Duplicates           │
└──────────────────────┬────────────────────────────────────┘
                       │  If PASS → promote
                       ▼
┌───────────────────────────────────────────────────────────┐
│               TARGET SYSTEM                               │
│    Microsoft Fabric — Lakehouse (Silver / Cleansed Zone) │
│    Spark SQL accessible, Delta format                     │
└───────────────────────────────────────────────────────────┘
```

---

## 6. Recommendations

1. **Automate validation runs** on every incremental load using the `MigrationValidator` class — not just during initial migration.
2. **Alert on CRITICAL failures** — integrate with Azure Monitor or Fabric alerting to notify the data team if row count checks fail.
3. **Resolve R-005** (approved amount nulls in source) — raise with upstream data owner; this is a business logic issue that predates migration.
4. **Performance test** LISTAGG replacement (R-004) before going to production with full historical dataset (est. >1M rows).
5. **Document the Oracle function audit** in the project wiki so future migrations don't rediscover the same issues.

---

## 7. Sign-off

| Role                     | Name         | Date       |
|--------------------------|--------------|------------|
| Data Engineer            | *(Portfolio)*| 2024-03-15 |
| Data Quality Lead        | *(Portfolio)*| 2024-03-15 |
| Migration Architect      | *(Portfolio)*| 2024-03-15 |

---

*This report was produced as part of a mocked portfolio project. All data, names, and systems are fictional.*
