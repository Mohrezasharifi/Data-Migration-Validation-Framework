"""
sql_translation_helper.py
==========================
A utility module mapping common Oracle SQL functions to their
Spark SQL (PySpark / Microsoft Fabric) equivalents.

Use this as a quick-reference during migration planning or code review.
Can also be used programmatically to suggest replacements in SQL strings.

Inspired by real Oracle → Spark migration work in a healthcare data context.
"""

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class TranslationRule:
    """Represents a single Oracle → Spark SQL translation."""
    oracle_function: str
    spark_equivalent: str
    notes: str
    example_oracle: str
    example_spark: str
    risk_level: str  # LOW | MEDIUM | HIGH


# ─────────────────────────────────────────────────────────────
# Translation Reference Table
# ─────────────────────────────────────────────────────────────

TRANSLATION_RULES: list[TranslationRule] = [

    # --- NULL HANDLING ---
    TranslationRule(
        oracle_function="NVL(expr, default)",
        spark_equivalent="COALESCE(expr, default)",
        notes="NVL is Oracle-specific. COALESCE is ANSI standard and works in Spark.",
        example_oracle="NVL(approved_amount, 0)",
        example_spark="COALESCE(approved_amount, 0)",
        risk_level="LOW",
    ),
    TranslationRule(
        oracle_function="NVL2(expr, val_not_null, val_null)",
        spark_equivalent="CASE WHEN expr IS NOT NULL THEN val_not_null ELSE val_null END",
        notes="NVL2 is Oracle-only. Must be rewritten as CASE WHEN in Spark.",
        example_oracle="NVL2(approved_amount, 'HAS_VALUE', 'NULL_VALUE')",
        example_spark="CASE WHEN approved_amount IS NOT NULL THEN 'HAS_VALUE' ELSE 'NULL_VALUE' END",
        risk_level="LOW",
    ),

    # --- CONDITIONAL LOGIC ---
    TranslationRule(
        oracle_function="DECODE(expr, val1, res1, val2, res2, default)",
        spark_equivalent="CASE WHEN expr = val1 THEN res1 WHEN expr = val2 THEN res2 ELSE default END",
        notes="DECODE is Oracle-specific. Must be rewritten as CASE WHEN. Watch for NULL handling — DECODE treats NULL = NULL as true.",
        example_oracle="DECODE(status, 'A', 'Active', 'I', 'Inactive', 'Unknown')",
        example_spark="CASE WHEN status = 'A' THEN 'Active' WHEN status = 'I' THEN 'Inactive' ELSE 'Unknown' END",
        risk_level="MEDIUM",
    ),

    # --- DATE FUNCTIONS ---
    TranslationRule(
        oracle_function="SYSDATE",
        spark_equivalent="current_date() / current_timestamp()",
        notes="SYSDATE returns date+time in Oracle. Use current_date() for date-only or current_timestamp() for datetime in Spark.",
        example_oracle="WHERE submission_date < SYSDATE",
        example_spark="WHERE submission_date < current_date()",
        risk_level="LOW",
    ),
    TranslationRule(
        oracle_function="TRUNC(date, 'MM')",
        spark_equivalent="DATE_TRUNC('month', date)",
        notes="TRUNC in Oracle handles both numeric and date truncation. In Spark, use DATE_TRUNC for dates.",
        example_oracle="TRUNC(service_date, 'MM')",
        example_spark="DATE_TRUNC('month', service_date)",
        risk_level="MEDIUM",
    ),
    TranslationRule(
        oracle_function="TO_CHAR(date, 'YYYY-MM-DD')",
        spark_equivalent="DATE_FORMAT(date, 'yyyy-MM-dd')",
        notes="Oracle format masks use uppercase (YYYY, MM, DD). Spark uses lowercase (yyyy, MM, dd). Mismatch causes silent errors.",
        example_oracle="TO_CHAR(service_date, 'YYYY-MM')",
        example_spark="DATE_FORMAT(service_date, 'yyyy-MM')",
        risk_level="HIGH",
    ),
    TranslationRule(
        oracle_function="TO_DATE(string, 'YYYY-MM-DD')",
        spark_equivalent="TO_DATE(string, 'yyyy-MM-dd')",
        notes="Function name is the same but format mask case differs. Spark is case-sensitive on format patterns.",
        example_oracle="TO_DATE('2023-04-01', 'YYYY-MM-DD')",
        example_spark="TO_DATE('2023-04-01', 'yyyy-MM-dd')",
        risk_level="MEDIUM",
    ),
    TranslationRule(
        oracle_function="ADD_MONTHS(date, n)",
        spark_equivalent="ADD_MONTHS(date, n)",
        notes="ADD_MONTHS works in both Oracle and Spark SQL — no change required.",
        example_oracle="ADD_MONTHS(service_date, 3)",
        example_spark="ADD_MONTHS(service_date, 3)",
        risk_level="LOW",
    ),
    TranslationRule(
        oracle_function="MONTHS_BETWEEN(d1, d2)",
        spark_equivalent="MONTHS_BETWEEN(d1, d2)",
        notes="MONTHS_BETWEEN works in both. Behaviour is identical.",
        example_oracle="MONTHS_BETWEEN(end_date, start_date)",
        example_spark="MONTHS_BETWEEN(end_date, start_date)",
        risk_level="LOW",
    ),

    # --- STRING FUNCTIONS ---
    TranslationRule(
        oracle_function="SUBSTR(str, pos, len)",
        spark_equivalent="SUBSTRING(str, pos, len) or SUBSTR(str, pos, len)",
        notes="Both SUBSTR and SUBSTRING work in Spark. Note: Oracle is 1-indexed, Spark is also 1-indexed.",
        example_oracle="SUBSTR(claim_id, 4, 6)",
        example_spark="SUBSTRING(claim_id, 4, 6)",
        risk_level="LOW",
    ),
    TranslationRule(
        oracle_function="INSTR(str, substr)",
        spark_equivalent="INSTR(str, substr)",
        notes="INSTR behaves identically in Oracle and Spark SQL.",
        example_oracle="INSTR(provider_name, 'NHS')",
        example_spark="INSTR(provider_name, 'NHS')",
        risk_level="LOW",
    ),
    TranslationRule(
        oracle_function="||  (string concatenation)",
        spark_equivalent="CONCAT(str1, str2) or || operator",
        notes="The || operator works in Spark SQL. CONCAT() is more explicit and portable.",
        example_oracle="first_name || ' ' || last_name",
        example_spark="CONCAT(first_name, ' ', last_name)",
        risk_level="LOW",
    ),

    # --- AGGREGATION ---
    TranslationRule(
        oracle_function="LISTAGG(expr, delim) WITHIN GROUP (ORDER BY ...)",
        spark_equivalent="ARRAY_JOIN(SORT_ARRAY(COLLECT_SET(expr)), delim)",
        notes="LISTAGG is Oracle-specific. In Spark, use COLLECT_SET (distinct) or COLLECT_LIST, then ARRAY_JOIN. For ORDER BY semantics, wrap with SORT_ARRAY.",
        example_oracle="LISTAGG(DISTINCT procedure_code, ', ') WITHIN GROUP (ORDER BY procedure_code)",
        example_spark="ARRAY_JOIN(SORT_ARRAY(COLLECT_SET(procedure_code)), ', ')",
        risk_level="HIGH",
    ),

    # --- ANALYTIC FUNCTIONS ---
    TranslationRule(
        oracle_function="RANK() OVER (...)",
        spark_equivalent="RANK() OVER (...)",
        notes="Window functions syntax is identical. No changes required.",
        example_oracle="RANK() OVER (PARTITION BY trust_id ORDER BY approved_amount DESC)",
        example_spark="RANK() OVER (PARTITION BY trust_id ORDER BY approved_amount DESC)",
        risk_level="LOW",
    ),
    TranslationRule(
        oracle_function="ROW_NUMBER() OVER (...)",
        spark_equivalent="ROW_NUMBER() OVER (...)",
        notes="ROW_NUMBER syntax is identical across Oracle and Spark.",
        example_oracle="ROW_NUMBER() OVER (ORDER BY claim_id)",
        example_spark="ROW_NUMBER() OVER (ORDER BY claim_id)",
        risk_level="LOW",
    ),

    # --- NUMERIC ---
    TranslationRule(
        oracle_function="ROUND(n, d)",
        spark_equivalent="ROUND(n, d)",
        notes="ROUND behaves identically. Note: Spark uses ROUND half-up by default.",
        example_oracle="ROUND(approved_amount, 2)",
        example_spark="ROUND(approved_amount, 2)",
        risk_level="LOW",
    ),
    TranslationRule(
        oracle_function="MOD(n, m)",
        spark_equivalent="MOD(n, m) or n % m",
        notes="MOD works in Spark. The % operator is also supported.",
        example_oracle="MOD(patient_age, 10)",
        example_spark="MOD(patient_age, 10)",
        risk_level="LOW",
    ),

    # --- TYPE CASTING ---
    TranslationRule(
        oracle_function="TO_NUMBER(str)",
        spark_equivalent="CAST(str AS DECIMAL) or CAST(str AS DOUBLE)",
        notes="TO_NUMBER is Oracle-specific. Use CAST in Spark. Be explicit about precision.",
        example_oracle="TO_NUMBER(claimed_amount_str)",
        example_spark="CAST(claimed_amount_str AS DECIMAL(10, 2))",
        risk_level="MEDIUM",
    ),
]


# ─────────────────────────────────────────────────────────────
# Helper Functions
# ─────────────────────────────────────────────────────────────

def print_translation_reference(risk_filter: Optional[str] = None) -> None:
    """
    Prints the full translation reference table.

    Parameters
    ----------
    risk_filter : str, optional
        Filter by risk level: 'LOW', 'MEDIUM', or 'HIGH'.
    """
    rules = TRANSLATION_RULES
    if risk_filter:
        rules = [r for r in rules if r.risk_level == risk_filter.upper()]

    print("\n" + "=" * 80)
    print("  ORACLE → SPARK SQL FUNCTION TRANSLATION REFERENCE")
    if risk_filter:
        print(f"  Filter: {risk_filter.upper()} risk only")
    print("=" * 80)

    for r in rules:
        risk_icon = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🔴"}.get(r.risk_level, "⚪")
        print(f"\n{risk_icon} {r.oracle_function}")
        print(f"   → Spark:   {r.spark_equivalent}")
        print(f"   📝 Notes:  {r.notes}")
        print(f"   Oracle:  {r.example_oracle}")
        print(f"   Spark:   {r.example_spark}")

    print("\n" + "=" * 80)
    print(f"  Total rules: {len(rules)}")
    print("=" * 80 + "\n")


def suggest_translations(sql_string: str) -> list[dict]:
    """
    Scans a SQL string for known Oracle function patterns and
    suggests Spark SQL replacements.

    Parameters
    ----------
    sql_string : str
        A SQL query string to scan.

    Returns
    -------
    list of dicts with detected Oracle functions and their suggestions.
    """
    sql_upper = sql_string.upper()
    suggestions = []

    oracle_patterns = {
        r"\bNVL2\s*\(": TRANSLATION_RULES[1],
        r"\bNVL\s*\(": TRANSLATION_RULES[0],
        r"\bDECODE\s*\(": TRANSLATION_RULES[2],
        r"\bSYSDATE\b": TRANSLATION_RULES[3],
        r"\bTRUNC\s*\([^,]+,\s*'MM'": TRANSLATION_RULES[4],
        r"\bTO_CHAR\s*\(": TRANSLATION_RULES[5],
        r"\bTO_DATE\s*\(": TRANSLATION_RULES[6],
        r"\bLISTAGG\s*\(": TRANSLATION_RULES[10],
        r"\bTO_NUMBER\s*\(": TRANSLATION_RULES[16],
    }

    for pattern, rule in oracle_patterns.items():
        if re.search(pattern, sql_upper):
            suggestions.append({
                "detected": rule.oracle_function,
                "suggestion": rule.spark_equivalent,
                "risk": rule.risk_level,
                "notes": rule.notes,
            })

    return suggestions


def scan_sql_file(filepath: str) -> None:
    """
    Reads a SQL file and reports any Oracle-specific functions detected.
    """
    with open(filepath, "r") as f:
        sql = f.read()

    suggestions = suggest_translations(sql)

    print(f"\nScanning: {filepath}")
    print("-" * 50)
    if not suggestions:
        print("✅ No Oracle-specific functions detected.")
    else:
        print(f"⚠️  {len(suggestions)} Oracle-specific pattern(s) detected:\n")
        for s in suggestions:
            risk_icon = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🔴"}.get(s["risk"], "⚪")
            print(f"  {risk_icon} [{s['risk']}] {s['detected']}")
            print(f"     → Replace with: {s['suggestion']}")
            print(f"     Note: {s['notes']}\n")


if __name__ == "__main__":
    print_translation_reference()
    print("\n--- Scanning Oracle SQL file for migration issues ---")
    scan_sql_file("../sql/oracle/claims_analysis.sql")
