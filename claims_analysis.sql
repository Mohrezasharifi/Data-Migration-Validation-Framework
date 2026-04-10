-- =============================================================
-- PROJECT 1: Data Migration Validation Framework
-- Oracle SQL Queries (Source System)
-- 
-- These represent the kinds of analytical queries that ran
-- against the legacy Oracle system before migration.
-- All schemas and data are fictional.
-- =============================================================


-- -----------------------------------------------------------
-- Q1: Total approved claim value by NHS Trust
-- Oracle-specific: TO_CHAR for date formatting, NVL for nulls
-- -----------------------------------------------------------
SELECT
    t.nhs_trust_id,
    t.nhs_trust_name,
    COUNT(c.claim_id)                          AS total_claims,
    SUM(NVL(c.approved_amount, 0))             AS total_approved_value,
    ROUND(AVG(NVL(c.approved_amount, 0)), 2)   AS avg_approved_value
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
-- Oracle-specific: DECODE, TO_DATE
-- -----------------------------------------------------------
SELECT
    c.financial_year,
    c.procedure_code,
    c.procedure_desc,
    COUNT(*)                                    AS claim_count,
    SUM(c.claimed_amount)                       AS total_claimed,
    SUM(NVL(c.approved_amount, 0))             AS total_approved,
    ROUND(
        SUM(NVL(c.approved_amount, 0)) /
        NULLIF(SUM(c.claimed_amount), 0) * 100, 2
    )                                           AS approval_rate_pct,
    DECODE(
        COUNT(*),
        0, 'NO CLAIMS',
        'HAS CLAIMS'
    )                                           AS activity_flag
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
-- Oracle-specific: NVL2, SYSDATE
-- -----------------------------------------------------------
SELECT
    c.claim_id,
    c.nhs_trust_id,
    c.provider_id,
    c.claim_status,
    c.claimed_amount,
    c.approved_amount,
    NVL2(c.approved_amount, 'HAS_VALUE', 'NULL_VALUE') AS approved_flag,
    TO_CHAR(c.service_date, 'YYYY-MM-DD')              AS service_date_fmt,
    TRUNC(SYSDATE) - TRUNC(c.submission_date)          AS days_since_submission
FROM
    nhs_claims c
WHERE
    c.approved_amount IS NULL
    AND c.claim_status = 'APPROVED'
ORDER BY
    c.submission_date DESC;


-- -----------------------------------------------------------
-- Q4: Monthly claim volumes using Oracle date functions
-- Oracle-specific: TRUNC(date, 'MM'), TO_CHAR date masks
-- -----------------------------------------------------------
SELECT
    TO_CHAR(TRUNC(c.service_date, 'MM'), 'YYYY-MM') AS service_month,
    c.nhs_trust_id,
    COUNT(*)                                         AS claim_count,
    SUM(c.claimed_amount)                            AS total_claimed,
    MAX(c.claimed_amount)                            AS max_claim,
    MIN(c.claimed_amount)                            AS min_claim
FROM
    nhs_claims c
GROUP BY
    TRUNC(c.service_date, 'MM'),
    c.nhs_trust_id
ORDER BY
    service_month,
    c.nhs_trust_id;


-- -----------------------------------------------------------
-- Q5: Provider performance with window functions
-- Oracle-specific: RANK() OVER, LISTAGG
-- -----------------------------------------------------------
SELECT
    p.provider_id,
    p.provider_name,
    p.provider_type,
    COUNT(c.claim_id)                          AS total_claims,
    SUM(NVL(c.approved_amount, 0))             AS total_approved,
    RANK() OVER (
        ORDER BY SUM(NVL(c.approved_amount, 0)) DESC
    )                                          AS approval_rank,
    LISTAGG(DISTINCT c.procedure_code, ', ')
        WITHIN GROUP (ORDER BY c.procedure_code) AS procedures_offered
FROM
    nhs_claims c
    JOIN providers p ON c.provider_id = p.provider_id
GROUP BY
    p.provider_id,
    p.provider_name,
    p.provider_type;


-- -----------------------------------------------------------
-- Q6: Patient demographic breakdown
-- Oracle-specific: CASE WHEN, WIDTH_BUCKET for age banding
-- -----------------------------------------------------------
SELECT
    CASE
        WHEN c.patient_age BETWEEN 0  AND 17  THEN '0-17'
        WHEN c.patient_age BETWEEN 18 AND 34  THEN '18-34'
        WHEN c.patient_age BETWEEN 35 AND 49  THEN '35-49'
        WHEN c.patient_age BETWEEN 50 AND 64  THEN '50-64'
        WHEN c.patient_age >= 65              THEN '65+'
        ELSE 'UNKNOWN'
    END                         AS age_band,
    c.patient_gender,
    COUNT(*)                    AS claim_count,
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
