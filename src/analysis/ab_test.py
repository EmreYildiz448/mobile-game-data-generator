# src/analysis/ab_test.py
from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd
import numpy as np
import scipy.stats as stats
import statsmodels.stats.proportion as smprop

ALPHA = 0.05  # global threshold

# ---------- Helpers ----------
def shapiro_test(x, name, alpha=ALPHA):
    stat, p = stats.shapiro(x)
    return stat, p

def levene_test(x, y, center='mean', alpha=ALPHA):
    stat, p = stats.levene(x, y, center=center)
    return stat, p

def student_t(x, y, alternative='two-sided', alpha=ALPHA):
    stat, p = stats.ttest_ind(x, y, equal_var=True, alternative=alternative)
    return {"test":"student_t", "stat":stat, "p":p}

def welch_t(x, y, alternative='two-sided', alpha=ALPHA):
    stat, p = stats.ttest_ind(x, y, equal_var=False, alternative=alternative)
    return {"test":"welch_t", "stat":stat, "p":p}

def mann_whitney(x, y, alternative='two-sided', alpha=ALPHA):
    stat, p = stats.mannwhitneyu(x, y, alternative=alternative)
    return {"test":"mann_whitney", "stat":stat, "p":p}

def proportions_z(counts, totals, alternative='two-sided', alpha=ALPHA):
    z, p = smprop.proportions_ztest(count=counts, nobs=totals, alternative=alternative)
    return {"test":"prop_z", "z":z, "p":p}

def chi2_2x2(x1, n1, x2, n2, alpha=ALPHA):
    table = np.array([[x1, n1-x1],[x2, n2-x2]])
    chi2, p, dof, expected = stats.chi2_contingency(table)
    return {"test":"chi2", "chi2":chi2, "p":p, "dof":dof, "expected":expected}

# ---------- Managers ----------
def continuous_test_manager(x, y, label,
                            normality_alpha=ALPHA,
                            variance_alpha=ALPHA,
                            alternative='two-sided'):
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)

    _, p_x = shapiro_test(x, f"{label} (A)", alpha=normality_alpha)
    _, p_y = shapiro_test(y, f"{label} (B)", alpha=normality_alpha)
    normal_ok = (p_x > normality_alpha) and (p_y > normality_alpha)

    if normal_ok:
        _, p_var = levene_test(x, y, center='mean', alpha=variance_alpha)
        if p_var > variance_alpha:
            res = student_t(x, y, alternative=alternative, alpha=ALPHA)
            basis = "parametric: Student t (equal var)"
        else:
            res = welch_t(x, y, alternative=alternative, alpha=ALPHA)
            basis = "parametric: Welch t (unequal var)"
    else:
        res = mann_whitney(x, y, alternative=alternative, alpha=ALPHA)
        basis = "nonparametric: Mann–Whitney U"

    final = "Reject H0" if res["p"] < ALPHA else "Fail to reject H0"
    return {"label":label, "basis":basis, "result":res, "decision_0.05":final}

def proportion_test_manager(x1, n1, x2, n2, label,
                            alternative='two-sided',
                            ci_method='wald', compare='diff'):
    z_res = proportions_z([x1, x2], [n1, n2], alternative=alternative, alpha=ALPHA)
    c2_res = chi2_2x2(x1, n1, x2, n2, alpha=ALPHA)
    ci_low, ci_high = smprop.confint_proportions_2indep(x2, n2, x1, n1,
                                                        method=ci_method, compare=compare)
    final = "Reject H0" if z_res["p"] < ALPHA else "Fail to reject H0"
    return {"label":label, "z":z_res, "chi2":c2_res,
            "ci":{"compare":compare,"method":ci_method,"low":ci_low,"high":ci_high},
            "decision_0.05":final}

# ---------- Public API ----------
def run_ab_tests(db_path: Path, report_dir: Path) -> Path:
    """
    Run all A/B tests reading from DuckDB analytics schema and write a CSV + TXT report.
    Returns path to the CSV report.
    """
    report_dir = Path(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    try:
        # Load analytics tables (renamed from CSVs)
        daily_revenue = con.sql("SELECT * FROM analytics.ab_test_daily_revenue").df()
        user_count    = con.sql("SELECT * FROM analytics.ab_test_daily_accounts").df()
        account_level = con.sql("SELECT * FROM analytics.ab_test_eda").df()

        # ARPDAU
        revenue_agg = daily_revenue.groupby(["session_date", "group_type"], as_index=False)["cost_usd"].sum()
        pivot = revenue_agg.pivot(index='session_date', columns='group_type', values='cost_usd')\
                           .rename(columns={'control':'control_revenue','test':'test_revenue'})
        merged = user_count.merge(pivot, on="session_date")
        merged["control_arpdau"] = merged["control_revenue"] / merged["control_unique_accounts"]
        merged["test_arpdau"]    = merged["test_revenue"]    / merged["test_unique_accounts"]

        # Slices
        ctrl_all = account_level.query("group_type == 'control'")
        test_all = account_level.query("group_type == 'test'")
        ctrl_conv = ctrl_all.query("converted == True")
        test_conv = test_all.query("converted == True")

        # Arrays
        r_arpdau     = continuous_test_manager(merged["control_arpdau"], merged["test_arpdau"], "ARPDAU (daily)")
        r_rev_all    = continuous_test_manager(ctrl_all["total_revenue"],  test_all["total_revenue"],  "Total Revenue / Account (all users)")
        r_rev_conv   = continuous_test_manager(ctrl_conv["total_revenue"], test_conv["total_revenue"], "Total Revenue / Account (converters)")
        r_purch_all  = continuous_test_manager(ctrl_all["purchase_count"],  test_all["purchase_count"],  "Purchase Count / Account (all users)")
        r_purch_conv = continuous_test_manager(ctrl_conv["purchase_count"], test_conv["purchase_count"], "Purchase Count / Account (converters)")

        # Conversion 2x2
        ctab = pd.crosstab(account_level["group_type"], account_level["converted"], margins=True)
        x1, n1 = int(ctab.loc["control", True]), int(ctab.loc["control","All"])
        x2, n2 = int(ctab.loc["test", True]),    int(ctab.loc["test","All"])
        r_conv = proportion_test_manager(x1, n1, x2, n2, "Conversion Rate",
                                         alternative="two-sided", ci_method="wald", compare="diff")

        # Flatten to rows
        def _row_cont(r): return {
            "metric": r["label"], "domain":"continuous", "test_used":r["result"]["test"],
            "basis": r["basis"], "statistic": float(r["result"]["stat"]),
            "p_value": float(r["result"]["p"]), "alpha": ALPHA, "decision": r["decision_0.05"]
        }
        def _row_prop(r): return {
            "metric": r["label"], "domain":"proportion", "test_used":"two_proportion_z",
            "basis":"z + chi2", "statistic": float(r["z"]["z"]), "p_value": float(r["z"]["p"]),
            "alpha": ALPHA, "decision": r["decision_0.05"],
            "ci_compare": r["ci"]["compare"], "ci_method": r["ci"]["method"],
            "ci_low": float(r["ci"]["low"]), "ci_high": float(r["ci"]["high"]),
            "chi2_stat": float(r["chi2"]["chi2"]), "chi2_p_value": float(r["chi2"]["p"]),
            "chi2_dof": int(r["chi2"]["dof"])
        }

        rows = [
            _row_cont(r_arpdau),
            _row_cont(r_rev_all),
            _row_cont(r_rev_conv),
            _row_cont(r_purch_all),
            _row_cont(r_purch_conv),
            _row_prop(r_conv),
        ]
        results = pd.DataFrame(rows)

        # Persist
        csv_path = report_dir / "ab_test_results.csv"
        txt_path = report_dir / "ab_test_results.txt"
        results.to_csv(csv_path, index=False)

        # ---- Pretty, human-readable TXT ----
        # split into continuous vs proportion sections
        cont = results.loc[results["domain"] == "continuous"].copy()
        prop = results.loc[results["domain"] == "proportion"].copy()

        def _fmt_p(p):
            return f"{p:.4f}" if pd.notna(p) else "NA"

        lines = []
        lines.append(f"A/B Test Results (alpha={ALPHA})")
        lines.append("=" * 72)
        lines.append("")

        # Continuous outcomes
        lines.append("CONTINUOUS OUTCOMES")
        lines.append("-" * 72)
        for _, r in cont.iterrows():
            lines.append(f"• {r['metric']}")
            lines.append(f"  Basis       : {r['basis']}")
            lines.append(f"  Test used   : {r['test_used']}")
            lines.append(f"  Statistic   : {r['statistic']:.3f}")
            lines.append(f"  p-value     : {_fmt_p(r['p_value'])}")
            lines.append(f"  Decision    : {r['decision']}")
            lines.append("")

        # Proportion outcomes
        if not prop.empty:
            lines.append("PROPORTIONS / RATES")
            lines.append("-" * 72)
            for _, r in prop.iterrows():
                lines.append(f"• {r['metric']}")
                lines.append(f"  Basis       : {r['basis']} (two-proportion z + χ²)")
                lines.append(f"  z-statistic : {r['statistic']:.3f}")
                lines.append(f"  p-value (z) : {_fmt_p(r['p_value'])}")
                # optional chi2 details
                if pd.notna(r.get("chi2_stat")):
                    lines.append(f"  χ²          : {r['chi2_stat']:.3f} (df={int(r['chi2_dof'])}), p={_fmt_p(r['chi2_p_value'])}")
                # CI details
                if pd.notna(r.get("ci_low")) and pd.notna(r.get("ci_high")):
                    lines.append(f"  CI ({r['ci_method']}, {r['ci_compare']}) : [{r['ci_low']:.6f}, {r['ci_high']:.6f}]")
                lines.append(f"  Decision    : {r['decision']}")
                lines.append("")

        # Final note
        lines.append("-" * 72)
        lines.append("Interpretation key:")
        lines.append("  • Reject H0          : groups differ at the 0.05 level")
        lines.append("  • Fail to reject H0  : no statistically significant difference at 0.05")
        lines.append("")

        with open(txt_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        print(f"[A/B] wrote CSV: {csv_path}")
        print(f"[A/B] wrote TXT: {txt_path}")

        return csv_path
    
    finally:
        con.close()