-- =============================================================
-- PROJECT 1: Data Migration Validation Framework
-- Spark SQL Queries (Target System — Microsoft Fabric / Lakehouse)
--
-- These are direct translations of the Oracle queries.
-- Key migration differences are annotated with [MIGRATION NOTE].
-- All schemas and data are fictional.
-- =============================================================


-- -----------------------------------------------------------
-- Q1: Total approved claim value by NHS Trust
-- [MIGRATION NOTE] NVL → COALESCE (ANSI-standard, works in Spark)
-- [MIGRATION NOTE] ROUND() behaviour identical, syntax unchanged
-- -----------------------------------------------------------
SELECT
    t.nhs_trust_id,
    t.nhs_trust_name,
    COUNT(c.claim_id)                              AS total_claims,
    SUM(COALESCE(c.approved_amount, 0))            AS total_approved_value,
    ROUND(AVG(COALESCE(c.approved_amount, 0)), 2)  AS avg_approved_value
FROM
    nhs_claims c
    JOIN nhs_trusts t ON c.nhs_trust_id = t.nhs_trust_id
WHERE
    c.claim_status = 'APPROVED'
GROUP BY
    t.nhs_trust_id,
    t.nhs_trust_name
ORDER BY
    total_approved_value DESC;


-- -----------------------------------------------------------
-- Q2: Claims summary by financial year and procedure type
-- [MIGRATION NOTE] DECODE() is NOT supported in Spark SQL
--                  → Use CASE WHEN ... THEN ... ELSE ... END
-- [MIGRATION NOTE] NULLIF() works in both Oracle and Spark SQL
-- -----------------------------------------------------------
SELECT
    c.financial_year,
    c.procedure_code,
    c.procedure_desc,
    COUNT(*)                                        AS claim_count,
    SUM(c.claimed_amount)                           AS total_claimed,
    SUM(COALESCE(c.approved_amount, 0))             AS total_approved,
    ROUND(
        SUM(COALESCE(c.approved_amount, 0)) /
        NULLIF(SUM(c.claimed_amount), 0) * 100, 2
    )                                               AS approval_rate_pct,
    -- DECODE replaced with CASE WHEN
    CASE
        WHEN COUNT(*) = 0 THEN 'NO CLAIMS'
        ELSE 'HAS CLAIMS'
    END                                             AS activity_flag
FROM
    nhs_claims c
GROUP BY
    c.financial_year,
    c.procedure_code,
    c.procedure_desc
ORDER BY
    c.financial_year,
    claim_count DESC;


-- -----------------------------------------------------------
-- Q3: Claims with missing approved amounts (data quality check)
-- [MIGRATION NOTE] NVL2(expr, val_if_not_null, val_if_null)
--                  → CASE WHEN expr IS NOT NULL THEN ... ELSE ...
-- [MIGRATION NOTE] SYSDATE → current_date() in Spark SQL
-- [MIGRATION NOTE] TRUNC(date) → DATE(timestamp) or CAST(... AS DATE)
-- [MIGRATION NOTE] TO_CHAR(date, mask) → DATE_FORMAT(date, pattern)
--                  Oracle 'YYYY-MM-DD' → Spark 'yyyy-MM-dd'
-- -----------------------------------------------------------
SELECT
    c.claim_id,
    c.nhs_trust_id,
    c.provider_id,
    c.claim_status,
    c.claimed_amount,
    c.approved_amount,
    -- NVL2 → CASE WHEN
    CASE
        WHEN c.approved_amount IS NOT NULL THEN 'HAS_VALUE'
        ELSE 'NULL_VALUE'
    END                                             AS approved_flag,
    DATE_FORMAT(c.service_date, 'yyyy-MM-dd')       AS service_date_fmt,
    DATEDIFF(current_date(), c.submission_date)     AS days_since_submission
FROM
    nhs_claims c
WHERE
    c.approved_amount IS NULL
    AND c.claim_status = 'APPROVED'
ORDER BY
    c.submission_date DESC;


-- -----------------------------------------------------------
-- Q4: Monthly claim volumes using Spark date functions
-- [MIGRATION NOTE] TRUNC(date, 'MM') → DATE_TRUNC('month', date)
-- [MIGRATION NOTE] TO_CHAR(date, 'YYYY-MM') → DATE_FORMAT(date, 'yyyy-MM')
-- -----------------------------------------------------------
SELECT
    DATE_FORMAT(DATE_TRUNC('month', c.service_date), 'yyyy-MM') AS service_month,
    c.nhs_trust_id,
    COUNT(*)                                                     AS claim_count,
    SUM(c.claimed_amount)                                        AS total_claimed,
    MAX(c.claimed_amount)                                        AS max_claim,
    MIN(c.claimed_amount)                                        AS min_claim
FROM
    nhs_claims c
GROUP BY
    DATE_TRUNC('month', c.service_date),
    c.nhs_trust_id
ORDER BY
    service_month,
    c.nhs_trust_id;


-- -----------------------------------------------------------
-- Q5: Provider performance with window functions
-- [MIGRATION NOTE] RANK() OVER → identical syntax in Spark SQL
-- [MIGRATION NOTE] LISTAGG() → COLLECT_LIST() + ARRAY_JOIN()
--                  Note: COLLECT_SET() for distinct values
-- -----------------------------------------------------------
SELECT
    p.provider_id,
    p.provider_name,
    p.provider_type,
    COUNT(c.claim_id)                              AS total_claims,
    SUM(COALESCE(c.approved_amount, 0))            AS total_approved,
    RANK() OVER (
        ORDER BY SUM(COALESCE(c.approved_amount, 0)) DESC
    )                                              AS approval_rank,
    -- LISTAGG(DISTINCT ...) → ARRAY_JOIN(SORT_ARRAY(COLLECT_SET(...)))
    ARRAY_JOIN(
        SORT_ARRAY(COLLECT_SET(c.procedure_code)), ', '
    )                                              AS procedures_offered
FROM
    nhs_claims c
    JOIN providers p ON c.provider_id = p.provider_id
GROUP BY
    p.provider_id,
    p.provider_name,
    p.provider_type;


-- -----------------------------------------------------------
-- Q6: Patient demographic breakdown
-- [MIGRATION NOTE] CASE WHEN is identical in Oracle and Spark SQL
-- [MIGRATION NOTE] WIDTH_BUCKET (if used) → manual CASE WHEN in Spark
-- -----------------------------------------------------------
SELECT
    CASE
        WHEN c.patient_age BETWEEN 0  AND 17  THEN '0-17'
        WHEN c.patient_age BETWEEN 18 AND 34  THEN '18-34'
        WHEN c.patient_age BETWEEN 35 AND 49  THEN '35-49'
        WHEN c.patient_age BETWEEN 50 AND 64  THEN '50-64'
        WHEN c.patient_age >= 65              THEN '65+'
        ELSE 'UNKNOWN'
    END                           AS age_band,
    c.patient_gender,
    COUNT(*)                      AS claim_count,
    ROUND(AVG(c.claimed_amount), 2) AS avg_claimed
FROM
    nhs_claims c
GROUP BY
    CASE
        WHEN c.patient_age BETWEEN 0  AND 17  THEN '0-17'
        WHEN c.patient_age BETWEEN 18 AND 34  THEN '18-34'
        WHEN c.patient_age BETWEEN 35 AND 49  THEN '35-49'
        WHEN c.patient_age BETWEEN 50 AND 64  THEN '50-64'
        WHEN c.patient_age >= 65              THEN '65+'
        ELSE 'UNKNOWN'
    END,
    c.patient_gender
ORDER BY
    age_band,
    c.patient_gender;
