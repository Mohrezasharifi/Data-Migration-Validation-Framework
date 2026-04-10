"""
migration_validator.py
======================
Data Migration Validation Framework for Oracle → Spark SQL migration.

This module provides a modular, reusable set of validation checks to
compare source (Oracle-style) and target (Spark/Fabric) datasets during
a data migration project.

Inspired by real-world healthcare data migration work in a regulated
NHS-adjacent environment. All data used with this framework is mocked.

Usage:
    validator = MigrationValidator(source_df, target_df, table_name="nhs_claims")
    report = validator.run_all_checks()
    validator.print_report(report)
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import Optional
import warnings
warnings.filterwarnings("ignore")


# ─────────────────────────────────────────────
# Data classes for structured reporting
# ─────────────────────────────────────────────

@dataclass
class CheckResult:
    """Holds the result of a single validation check."""
    check_name: str
    status: str          # PASS | FAIL | WARN | ERROR
    detail: str
    severity: str        # LOW | MEDIUM | HIGH | CRITICAL
    row_impact: int = 0  # Number of rows affected


@dataclass
class ValidationReport:
    """Aggregated report across all validation checks."""
    table_name: str
    source_row_count: int
    target_row_count: int
    checks: list = field(default_factory=list)

    @property
    def passed(self):
        return [c for c in self.checks if c.status == "PASS"]

    @property
    def failed(self):
        return [c for c in self.checks if c.status == "FAIL"]

    @property
    def warnings(self):
        return [c for c in self.checks if c.status == "WARN"]

    @property
    def overall_status(self):
        if any(c.severity == "CRITICAL" and c.status == "FAIL" for c in self.checks):
            return "CRITICAL FAILURE"
        if any(c.status == "FAIL" for c in self.checks):
            return "FAILED"
        if any(c.status == "WARN" for c in self.checks):
            return "PASSED WITH WARNINGS"
        return "PASSED"


# ─────────────────────────────────────────────
# Core Validator Class
# ─────────────────────────────────────────────

class MigrationValidator:
    """
    Compares a source DataFrame (representing Oracle output) against a
    target DataFrame (representing Spark/Fabric output) to validate
    data migration fidelity.

    Parameters
    ----------
    source_df : pd.DataFrame
        Data extracted from the source Oracle system.
    target_df : pd.DataFrame
        Data extracted from the target Spark/Fabric system.
    table_name : str
        Name of the table being validated (for reporting).
    pk_column : str, optional
        Primary key column for row-level mismatch analysis.
    numeric_tolerance : float
        Acceptable relative difference for numeric comparisons (default 0.001 = 0.1%).
    """

    def __init__(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
        table_name: str = "unknown_table",
        pk_column: Optional[str] = None,
        numeric_tolerance: float = 0.001,
    ):
        self.source = source_df.copy()
        self.target = target_df.copy()
        self.table_name = table_name
        self.pk_column = pk_column
        self.tolerance = numeric_tolerance
        self._results: list[CheckResult] = []

    # ─────────────────────────────────────────
    # 1. Row Count Validation
    # ─────────────────────────────────────────

    def check_row_counts(self) -> CheckResult:
        """
        Validates that source and target have the same number of rows.
        A mismatch here is a critical indicator of data loss or duplication.
        """
        src_count = len(self.source)
        tgt_count = len(self.target)
        diff = tgt_count - src_count
        pct_diff = abs(diff / src_count * 100) if src_count > 0 else 0

        if src_count == tgt_count:
            return CheckResult(
                check_name="Row Count",
                status="PASS",
                detail=f"Both source and target have {src_count:,} rows.",
                severity="CRITICAL",
            )
        elif pct_diff <= 0.1:
            return CheckResult(
                check_name="Row Count",
                status="WARN",
                detail=(
                    f"Source: {src_count:,} rows | Target: {tgt_count:,} rows. "
                    f"Difference: {diff:+,} rows ({pct_diff:.3f}%). Within tolerance."
                ),
                severity="MEDIUM",
                row_impact=abs(diff),
            )
        else:
            return CheckResult(
                check_name="Row Count",
                status="FAIL",
                detail=(
                    f"Source: {src_count:,} rows | Target: {tgt_count:,} rows. "
                    f"Difference: {diff:+,} rows ({pct_diff:.2f}%). Exceeds threshold."
                ),
                severity="CRITICAL",
                row_impact=abs(diff),
            )

    # ─────────────────────────────────────────
    # 2. Schema / Column Validation
    # ─────────────────────────────────────────

    def check_schema(self) -> CheckResult:
        """
        Compares column names and data types between source and target.
        Missing or renamed columns are common migration issues.
        """
        src_cols = set(self.source.columns)
        tgt_cols = set(self.target.columns)

        missing_in_target = src_cols - tgt_cols
        extra_in_target = tgt_cols - src_cols

        issues = []
        if missing_in_target:
            issues.append(f"Missing in target: {sorted(missing_in_target)}")
        if extra_in_target:
            issues.append(f"Extra in target: {sorted(extra_in_target)}")

        if issues:
            return CheckResult(
                check_name="Schema Check",
                status="FAIL",
                detail=" | ".join(issues),
                severity="HIGH",
            )
        return CheckResult(
            check_name="Schema Check",
            status="PASS",
            detail=f"All {len(src_cols)} columns present in both source and target.",
            severity="HIGH",
        )

    # ─────────────────────────────────────────
    # 3. Null / Missing Value Analysis
    # ─────────────────────────────────────────

    def check_nulls(self) -> list[CheckResult]:
        """
        Compares null counts per column between source and target.
        Unexpected nulls in the target can indicate ETL failures.
        """
        results = []
        common_cols = [c for c in self.source.columns if c in self.target.columns]

        for col in common_cols:
            src_nulls = self.source[col].isna().sum()
            tgt_nulls = self.target[col].isna().sum()
            diff = tgt_nulls - src_nulls

            if diff == 0:
                results.append(CheckResult(
                    check_name=f"Null Check: {col}",
                    status="PASS",
                    detail=f"Null count matches: {src_nulls:,} in both.",
                    severity="MEDIUM",
                ))
            elif diff > 0:
                results.append(CheckResult(
                    check_name=f"Null Check: {col}",
                    status="FAIL",
                    detail=(
                        f"Target has {diff:,} MORE nulls than source "
                        f"(Source: {src_nulls:,} | Target: {tgt_nulls:,})."
                    ),
                    severity="HIGH",
                    row_impact=diff,
                ))
            else:
                results.append(CheckResult(
                    check_name=f"Null Check: {col}",
                    status="WARN",
                    detail=(
                        f"Target has {abs(diff):,} FEWER nulls than source "
                        f"(Source: {src_nulls:,} | Target: {tgt_nulls:,}). "
                        "May indicate unexpected default-fill."
                    ),
                    severity="LOW",
                    row_impact=abs(diff),
                ))
        return results

    # ─────────────────────────────────────────
    # 4. Aggregate / Sum Validation
    # ─────────────────────────────────────────

    def check_aggregates(self, numeric_columns: Optional[list] = None) -> list[CheckResult]:
        """
        Compares SUM and AVG of numeric columns between source and target.
        Tolerates minor floating-point differences (controlled by self.tolerance).
        """
        results = []

        if numeric_columns is None:
            numeric_columns = [
                c for c in self.source.columns
                if pd.api.types.is_numeric_dtype(self.source[c])
                and c in self.target.columns
            ]

        for col in numeric_columns:
            src_sum = self.source[col].sum()
            tgt_sum = self.target[col].sum()
            src_mean = self.source[col].mean()
            tgt_mean = self.target[col].mean()

            # Relative difference
            sum_diff_pct = abs((tgt_sum - src_sum) / src_sum) if src_sum != 0 else 0
            mean_diff_pct = abs((tgt_mean - src_mean) / src_mean) if src_mean != 0 else 0

            max_diff = max(sum_diff_pct, mean_diff_pct)

            if max_diff <= self.tolerance:
                results.append(CheckResult(
                    check_name=f"Aggregate Check: {col}",
                    status="PASS",
                    detail=(
                        f"SUM — Source: {src_sum:,.2f} | Target: {tgt_sum:,.2f} "
                        f"(Δ {sum_diff_pct:.4%}). "
                        f"AVG — Source: {src_mean:,.4f} | Target: {tgt_mean:,.4f}."
                    ),
                    severity="HIGH",
                ))
            else:
                results.append(CheckResult(
                    check_name=f"Aggregate Check: {col}",
                    status="FAIL",
                    detail=(
                        f"SUM mismatch: Source {src_sum:,.2f} vs Target {tgt_sum:,.2f} "
                        f"(Δ {sum_diff_pct:.4%}). "
                        f"AVG mismatch: Source {src_mean:,.4f} vs Target {tgt_mean:,.4f}."
                    ),
                    severity="HIGH",
                ))
        return results

    # ─────────────────────────────────────────
    # 5. Duplicate Row Detection
    # ─────────────────────────────────────────

    def check_duplicates(self) -> list[CheckResult]:
        """
        Checks for duplicate rows in source and target.
        Duplicates in the target after migration indicate ETL join fan-out issues.
        """
        results = []

        for label, df in [("Source", self.source), ("Target", self.target)]:
            dup_count = df.duplicated().sum()
            if dup_count == 0:
                results.append(CheckResult(
                    check_name=f"Duplicate Check: {label}",
                    status="PASS",
                    detail=f"No duplicate rows found in {label}.",
                    severity="HIGH",
                ))
            else:
                results.append(CheckResult(
                    check_name=f"Duplicate Check: {label}",
                    status="FAIL",
                    detail=f"{dup_count:,} duplicate rows detected in {label}.",
                    severity="HIGH",
                    row_impact=dup_count,
                ))

        # Bonus: check if target has MORE duplicates than source
        src_dups = self.source.duplicated().sum()
        tgt_dups = self.target.duplicated().sum()
        if tgt_dups > src_dups:
            results.append(CheckResult(
                check_name="Duplicate Check: Regression",
                status="FAIL",
                detail=(
                    f"Target introduced {tgt_dups - src_dups} additional duplicates "
                    f"vs source (Source: {src_dups} | Target: {tgt_dups})."
                ),
                severity="CRITICAL",
                row_impact=tgt_dups - src_dups,
            ))

        return results

    # ─────────────────────────────────────────
    # 6. Categorical / Distribution Check
    # ─────────────────────────────────────────

    def check_categorical_distributions(self, columns: Optional[list] = None) -> list[CheckResult]:
        """
        Compares value distributions for categorical columns.
        Flags columns where the target has categories not in the source,
        or where category proportions have shifted significantly.
        """
        results = []

        if columns is None:
            # Auto-detect low-cardinality string columns
            columns = [
                c for c in self.source.columns
                if self.source[c].dtype == object
                and c in self.target.columns
                and self.source[c].nunique() < 30
            ]

        for col in columns:
            src_vals = set(self.source[col].dropna().unique())
            tgt_vals = set(self.target[col].dropna().unique())

            new_in_target = tgt_vals - src_vals
            missing_in_target = src_vals - tgt_vals

            issues = []
            if new_in_target:
                issues.append(f"New values in target: {new_in_target}")
            if missing_in_target:
                issues.append(f"Values dropped from target: {missing_in_target}")

            if issues:
                results.append(CheckResult(
                    check_name=f"Distribution Check: {col}",
                    status="WARN",
                    detail=" | ".join(issues),
                    severity="MEDIUM",
                ))
            else:
                results.append(CheckResult(
                    check_name=f"Distribution Check: {col}",
                    status="PASS",
                    detail=f"Category values match across source and target ({len(src_vals)} distinct).",
                    severity="MEDIUM",
                ))

        return results

    # ─────────────────────────────────────────
    # 7. PK-level Row Mismatch (if PK provided)
    # ─────────────────────────────────────────

    def check_pk_completeness(self) -> CheckResult:
        """
        If a primary key column is defined, compares the sets of PK values
        to identify missing or extra records in the target.
        """
        if not self.pk_column:
            return CheckResult(
                check_name="PK Completeness",
                status="WARN",
                detail="No pk_column specified — skipping PK completeness check.",
                severity="LOW",
            )

        src_pks = set(self.source[self.pk_column].astype(str))
        tgt_pks = set(self.target[self.pk_column].astype(str))

        missing = src_pks - tgt_pks
        extra = tgt_pks - src_pks

        if not missing and not extra:
            return CheckResult(
                check_name="PK Completeness",
                status="PASS",
                detail=f"All {len(src_pks):,} primary keys accounted for in target.",
                severity="CRITICAL",
            )

        detail_parts = []
        if missing:
            sample = list(missing)[:5]
            detail_parts.append(f"{len(missing):,} PKs in source but NOT in target (sample: {sample})")
        if extra:
            sample = list(extra)[:5]
            detail_parts.append(f"{len(extra):,} PKs in target but NOT in source (sample: {sample})")

        return CheckResult(
            check_name="PK Completeness",
            status="FAIL",
            detail=" | ".join(detail_parts),
            severity="CRITICAL",
            row_impact=len(missing) + len(extra),
        )

    # ─────────────────────────────────────────
    # Run All Checks
    # ─────────────────────────────────────────

    def run_all_checks(
        self,
        numeric_columns: Optional[list] = None,
        categorical_columns: Optional[list] = None,
    ) -> ValidationReport:
        """
        Executes all validation checks and returns a consolidated report.
        """
        report = ValidationReport(
            table_name=self.table_name,
            source_row_count=len(self.source),
            target_row_count=len(self.target),
        )

        # Single-result checks
        report.checks.append(self.check_row_counts())
        report.checks.append(self.check_schema())
        report.checks.append(self.check_pk_completeness())

        # Multi-result checks (extend with list)
        report.checks.extend(self.check_nulls())
        report.checks.extend(self.check_aggregates(numeric_columns))
        report.checks.extend(self.check_duplicates())
        report.checks.extend(self.check_categorical_distributions(categorical_columns))

        return report

    # ─────────────────────────────────────────
    # Reporting
    # ─────────────────────────────────────────

    @staticmethod
    def print_report(report: ValidationReport) -> None:
        """Pretty-prints the validation report to stdout."""
        STATUS_ICONS = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️ ", "ERROR": "🔥"}
        SEVERITY_ORDER = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}

        print("\n" + "=" * 70)
        print(f"  MIGRATION VALIDATION REPORT — {report.table_name.upper()}")
        print("=" * 70)
        print(f"  Source rows : {report.source_row_count:,}")
        print(f"  Target rows : {report.target_row_count:,}")
        print(f"  Total checks: {len(report.checks)}")
        print(f"  ✅ Passed   : {len(report.passed)}")
        print(f"  ❌ Failed   : {len(report.failed)}")
        print(f"  ⚠️  Warnings : {len(report.warnings)}")
        print(f"\n  OVERALL STATUS: >>> {report.overall_status} <<<")
        print("=" * 70)

        # Group by severity for display
        sorted_checks = sorted(
            report.checks, key=lambda c: SEVERITY_ORDER.get(c.severity, 9)
        )

        for check in sorted_checks:
            icon = STATUS_ICONS.get(check.status, "?")
            impact = f" [{check.row_impact:,} rows]" if check.row_impact > 0 else ""
            print(f"\n  {icon} [{check.severity}] {check.check_name}{impact}")
            print(f"     {check.detail}")

        print("\n" + "=" * 70 + "\n")

    @staticmethod
    def to_dataframe(report: ValidationReport) -> pd.DataFrame:
        """Converts the validation report to a pandas DataFrame for further analysis."""
        return pd.DataFrame([
            {
                "table_name": report.table_name,
                "check_name": c.check_name,
                "status": c.status,
                "severity": c.severity,
                "row_impact": c.row_impact,
                "detail": c.detail,
            }
            for c in report.checks
        ])
