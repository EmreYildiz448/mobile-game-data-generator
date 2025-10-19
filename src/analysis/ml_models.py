# src/analysis/ml_models.py
from __future__ import annotations
import matplotlib

matplotlib.use("Agg")  # prevents GUI windows

from pathlib import Path
import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.model_selection import KFold
from sklearn.metrics import accuracy_score, r2_score
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor, plot_tree
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LinearRegression
from xgboost import XGBClassifier, XGBRegressor
import shap

# ──────────────────────────────────────────────────────────────────────────────
# Data prep (reads from DuckDB analytics.equipment_contribution_eda)
# ──────────────────────────────────────────────────────────────────────────────
def _load_equipment_contribution_eda(db_path: Path) -> pd.DataFrame:
    con = duckdb.connect(str(db_path))
    try:
        return con.sql("SELECT * FROM analytics.equipment_contribution_eda").df()
    finally:
        con.close()

def _append_master_feature_importance(avg_imp: pd.Series,
                                      model_name: str,
                                      target_col: str,
                                      ml_base_dir: Path) -> Path:
    """
    Append to one consolidated CSV under ml/summary/.
    Columns: [model, target, feature, importance]
    """
    summary_dir = Path(ml_base_dir) / "summary"
    summary_dir.mkdir(parents=True, exist_ok=True)
    master = summary_dir / "feature_importance_results.csv"
    df = pd.DataFrame({
        "model": model_name,
        "target": target_col,
        "feature": avg_imp.index,
        "importance": avg_imp.values,
    })
    df.to_csv(master, mode=("a" if master.exists() else "w"), header=not master.exists(), index=False)
    return master

def _approach_name(target_column: str, residual_mode: bool) -> str:
    if target_column == "is_attempt_successful":
        return "binary"
    if target_column == "stars_gained":
        return "ordinal"
    if target_column == "total_score" and residual_mode:
        return "residual_regression"
    return "raw_regression"

def _slug(s: str) -> str:
    return s.lower().replace(" ", "_").replace("-", "")

def _append_master_metrics(scores: list[float],
                           model_name: str,
                           target_col: str,
                           ml_base_dir: Path,
                           is_classifier: bool,
                           n_samples: int) -> Path:
    """
    Append CV metrics to ml/summary/model_metrics.csv
    Columns: model,target,metric,mean,std,n_splits,n_samples
    """
    summary_dir = Path(ml_base_dir) / "summary"
    summary_dir.mkdir(parents=True, exist_ok=True)
    path = summary_dir / "model_metrics.csv"
    df = pd.DataFrame([{
        "model": model_name,
        "target": target_col,
        "metric": "accuracy" if is_classifier else "r2",
        "mean": float(np.mean(scores)),
        "std": float(np.std(scores)),
        "n_splits": int(len(scores)),
        "n_samples": int(n_samples),
    }])
    df.to_csv(path, mode=("a" if path.exists() else "w"),
              header=not path.exists(), index=False)
    return path

def _append_human_topk(avg_imp_sorted: pd.Series,
                       score_mean: float,
                       score_std: float,
                       is_classifier: bool,
                       model_name: str,
                       target_col: str,
                       ml_base_dir: Path,
                       k: int = 3) -> Path:
    """
    Append a short, human-readable summary to: ml/summary/feature_importance_topk.txt

    Example block:
      XGBoost - Binary Classification (target=is_attempt_successful)
      Average accuracy = 0.6385 (±0.0123, 5-fold CV)
      Top contributors:
        • success_score_floor: 0.2311 (42.3% of top-3)
        • base_held_item_potion: 0.1975 (36.1% of top-3)
        • base_weapon_staff: 0.1174 (21.5% of top-3)
    """
    summary_dir = Path(ml_base_dir) / "summary"
    summary_dir.mkdir(parents=True, exist_ok=True)
    path = summary_dir / "feature_importance_topk.txt"

    metric = "accuracy" if is_classifier else "R²"

    top = avg_imp_sorted.head(k)
    total_top = float(top.sum()) if len(top) else 0.0

    lines = []
    lines.append(f"{model_name} (target={target_col})")
    lines.append(f"Average {metric} = {score_mean:.4f} (±{score_std:.4f}, 5-fold CV)")
    lines.append("Top contributors:")
    for feat, val in top.items():
        pct = (val / total_top * 100.0) if total_top > 0 else 0.0
        lines.append(f"  • {feat}: {val:.4f} ({pct:.1f}% of top-{k})")
    lines.append("")  # blank line between models

    with open(path, "a", encoding="utf-8") as f:
        f.write("\n".join(lines))

    return path

def preprocess_data(db_path: Path, target_type: str) -> pd.DataFrame:
    df = _load_equipment_contribution_eda(db_path)
    df["total_score"]  = pd.to_numeric(df["total_score"],  errors="coerce")
    df["stars_gained"] = pd.to_numeric(df["stars_gained"], errors="coerce")
    df = df[df["total_score"].notnull()]
    df = df[~df.level_id.astype(str).str.startswith('T')]
    df["level_id"] = df["level_id"].astype(str).str.split("_", expand=True)[1].astype("int32")
    df["success_score_floor"] = 800 + (df["level_id"] - 1) * 50

    rarity_order = {"c": 1, "u": 2, "r": 3, "e": 4, "l": 5}
    def split_item_string(item_str):
        try:
            parts = str(item_str).split("_")
            rarity = parts[0]
            base_item = "_".join(parts[2:]) if len(parts) > 2 else "unknown"
            return rarity_order.get(rarity, 0), base_item
        except Exception:
            return 0, "unknown"

    for slot in ["weapon", "armor", "held_item"]:
        df[f"rarity_{slot}"], df[f"base_{slot}"] = zip(*df[f"equipped_{slot}"].fillna("").map(split_item_string))

    def parse_skin(skin_str):
        try:
            parts = str(skin_str).split("_")
            return "_".join(parts[1:]) if len(parts) >= 3 else "missing"
        except Exception:
            return "missing"
    df["skin"] = df["equipped_skin"].fillna("missing").map(parse_skin)
    df = pd.get_dummies(df, columns=["base_weapon", "base_armor", "base_held_item", "skin"])

    drop_cols = ['equipped_hero','equipped_weapon','equipped_armor','equipped_held_item','equipped_skin','event_date','level_id']
    df.drop(columns=[c for c in drop_cols if c in df.columns], inplace=True)

    if target_type == "binary":
        df["is_attempt_successful"] = df["is_attempt_successful"].astype(bool)
    elif target_type == "ordinal":
        # Drop rows with null total_score
        df = df[df["total_score"].notnull()]

        # Convert stars_gained to numeric safely
        df["stars_gained"] = pd.to_numeric(df["stars_gained"], errors="coerce")

        # Drop rows where stars_gained could not be parsed
        df = df[df["stars_gained"].notnull()]

        # Cast to compact int type and flip scale (3=best, 0=worst)
        df["stars_gained"] = (3 - df["stars_gained"]).astype("int8")

    elif target_type in ["regression", "residual"]:
        df = df[df["total_score"].notnull()]

    return df.copy()

# ──────────────────────────────────────────────────────────────────────────────
# Model runner
# ──────────────────────────────────────────────────────────────────────────────
def run_model(model_cls, target_column, df, *,
              is_classifier=True, residual_mode=False, drop_cols=None,
              model_name="Model", model_library="sklearn",
              save_tree_image=False, fig_dir_local: Path | None=None,
              explain=False, indiv_dir: Path | None=None):
    
    # compute folders from the base dir the caller passes
    ml_base_dir   = Path(indiv_dir) if indiv_dir else None
    approach      = _approach_name(target_column, residual_mode)
    indiv_dir     = (ml_base_dir / "individual" / approach) if ml_base_dir else None
    fig_dir_local = (indiv_dir / "figures") if indiv_dir else None
    summary_dir   = (ml_base_dir / "summary") if ml_base_dir else None

    # ensure dirs exist if reporting is enabled
    if indiv_dir:
        indiv_dir.mkdir(parents=True, exist_ok=True)
        fig_dir_local.mkdir(parents=True, exist_ok=True)
    if summary_dir:
        summary_dir.mkdir(parents=True, exist_ok=True)

    drop_cols = drop_cols or []
    if residual_mode:
        X_full = df.drop(columns=["total_score"] + drop_cols)
        y_total = df["total_score"]
        baseline = LinearRegression().fit(df[["success_score_floor"]], y_total)
        y_baseline = baseline.predict(df[["success_score_floor"]])
        y = y_total - y_baseline
        X = X_full.drop(columns=["success_score_floor"], errors='ignore')
    else:
        y = df[target_column]
        X = df.drop(columns=[target_column] + drop_cols)

    kf = KFold(n_splits=5, shuffle=True, random_state=42)
    scores, importances = [], []

    # Fit/eval across folds
    for train_idx, test_idx in kf.split(X):
        if model_library == "sklearn":
            model = model_cls()
        elif model_library == "xgboost":
            model = XGBClassifier(eval_metric="logloss") if is_classifier else XGBRegressor()
        else:
            raise ValueError("model_library must be 'sklearn' or 'xgboost'")
        model.fit(X.iloc[train_idx], y.iloc[train_idx])
        y_pred = model.predict(X.iloc[test_idx])
        score = accuracy_score(y.iloc[test_idx], y_pred) if is_classifier else r2_score(y.iloc[test_idx], y_pred)
        scores.append(score)
        importances.append(getattr(model, "feature_importances_", np.zeros(X.shape[1])))

    avg_imp = pd.Series(np.mean(importances, axis=0), index=X.columns)
    print(f"[{model_name}] Average {'Accuracy' if is_classifier else 'R²'}: {np.mean(scores):.4f}")
    print(f"[{model_name}] Top features:\n{avg_imp.sort_values(ascending=False).head(10)}\n")

    score_mean = float(np.mean(scores))
    avg_imp_sorted = avg_imp.sort_values(ascending=False)

    if indiv_dir:
        indiv_dir.mkdir(parents=True, exist_ok=True)

        # 1) Per-model feature-importance (unchanged)
        fi_csv = indiv_dir / (_slug(model_name) + "_feature_importance.csv")
        avg_imp_sorted.to_csv(fi_csv, header=["importance"])
        print(f"[{model_name}] wrote: {fi_csv}")

        # 2) Consolidated long table (unchanged logic)
        master = _append_master_feature_importance(
            avg_imp_sorted, model_name=model_name,
            target_col=target_column, ml_base_dir=ml_base_dir
        )
        print(f"[{model_name}] appended to: {master}")

        # 3) NEW: consolidated CV metrics (one row per model/target)
        metrics_master = _append_master_metrics(
            scores=scores, model_name=model_name, target_col=target_column,
            ml_base_dir=ml_base_dir, is_classifier=is_classifier,
            n_samples=len(X),
        )
        print(f"[{model_name}] metrics appended to: {metrics_master}")

        # NEW: human-readable top-k text summary
        topk_txt = _append_human_topk(
            avg_imp_sorted=avg_imp_sorted,
            score_mean=score_mean,
            score_std=float(np.std(scores)),
            is_classifier=is_classifier,
            model_name=model_name,
            target_col=target_column,
            ml_base_dir=ml_base_dir,
            k=3
        )
        print(f"[{model_name}] top-k (text) appended to: {topk_txt}")

        # 5) Optional: per-model sidecar summary (easy to open beside the FI CSV)
        per_model_summary = indiv_dir / (_slug(model_name) + "_summary.csv")
        pd.DataFrame([{
            "metric": "accuracy" if is_classifier else "r2",
            "mean": score_mean,
            "std": float(np.std(scores)),
            "n_splits": int(len(scores)),
            "n_samples": int(len(X)),
            "top1": avg_imp_sorted.index[0] if len(avg_imp_sorted) > 0 else "",
            "top1_imp": float(avg_imp_sorted.iloc[0]) if len(avg_imp_sorted) > 0 else np.nan,
            "top2": avg_imp_sorted.index[1] if len(avg_imp_sorted) > 1 else "",
            "top2_imp": float(avg_imp_sorted.iloc[1]) if len(avg_imp_sorted) > 1 else np.nan,
            "top3": avg_imp_sorted.index[2] if len(avg_imp_sorted) > 2 else "",
            "top3_imp": float(avg_imp_sorted.iloc[2]) if len(avg_imp_sorted) > 2 else np.nan,
        }]).to_csv(per_model_summary, index=False)
        print(f"[{model_name}] wrote summary: {per_model_summary}")

    # Optional tree image for tree-based sklearn models
    if save_tree_image and hasattr(model, "tree_") and fig_dir_local:
        fig_dir_local.mkdir(parents=True, exist_ok=True)
        fig = plt.figure(figsize=(36, 24), dpi=120)
        plot_tree(model, feature_names=X.columns, filled=True, rounded=True, fontsize=10)
        plt.title(model_name, fontsize=16)
        out = fig_dir_local / (model_name.lower().replace(" ", "_").replace("-", "") + ".png")
        plt.savefig(out)
        plt.close(fig)
        print(f"[{model_name}] wrote image: {out}")

    # SHAP (XGBoost only, optional)
    if explain and model_library == "xgboost" and fig_dir_local and indiv_dir:
        fig_dir_local.mkdir(parents=True, exist_ok=True)
        indiv_dir.mkdir(parents=True, exist_ok=True)

        sample_size = min(1000, len(X))
        Xs = X.sample(n=sample_size, random_state=42).astype(np.float32)
        explainer = shap.Explainer(model, Xs)
        sv = explainer(Xs)

        if hasattr(sv, "values"):
            vals = sv.values

            # Older SHAP sometimes returns a list: one Explanation per class
            if isinstance(vals, list):
                k = 0  # class index to explain 
                sv = shap.Explanation(
                    values=vals[k],
                    base_values=sv.base_values[k],
                    data=sv.data,
                    feature_names=sv.feature_names,
                )

            # Newer SHAP: 3-D array (n_samples, n_features, n_classes)
            elif getattr(vals, "ndim", 0) == 3:
                k = 0  # class index to explain 
                base = sv.base_values
                # base can be (samples, classes) – take the same class
                if getattr(base, "ndim", 1) == 2:
                    base = base[:, k]
                sv = shap.Explanation(
                    values=vals[..., k],     # keep SIGNED values for that class
                    base_values=base,
                    data=sv.data,
                    feature_names=sv.feature_names,
                )

        # -----------------------------
        # Global bar (top features)
        # -----------------------------
        try:
            shap.plots.bar(sv, max_display=10, show=False)
            plt.title(f"{model_name} - SHAP Bar")
            plt.tight_layout()
            out = fig_dir_local / (model_name.lower().replace(" ", "_").replace("-", "") + "_shap_bar.png")
            plt.savefig(out); plt.close()
            print(f"[{model_name}] wrote: {out}")
        except Exception as e:
            print(f"[{model_name}] SHAP bar skipped: {e}")

        # -----------------------------
        # Beeswarm (signed)
        # -----------------------------
        try:
            shap.plots.beeswarm(sv, max_display=10, show=False)
            plt.title(f"{model_name} - SHAP Beeswarm")
            plt.tight_layout()
            out = fig_dir_local / (model_name.lower().replace(" ", "_").replace("-", "") + "_shap_beeswarm.png")
            plt.savefig(out); plt.close()
            print(f"[{model_name}] wrote: {out}")
        except Exception as e:
            print(f"[{model_name}] SHAP beeswarm skipped: {e}")

        # -----------------------------
        # Dependence plots for top N features
        # -----------------------------
        try:
            mean_abs = np.abs(sv.values).mean(axis=0)
            feature_names = sv.feature_names or list(Xs.columns)
            summary_df = pd.DataFrame(
                {"feature": feature_names, "mean_abs_shap": mean_abs}
            ).sort_values("mean_abs_shap", ascending=False)

            # Save the summary table (unchanged behavior)
            out_csv = indiv_dir / (model_name.lower().replace(" ", "_").replace("-", "") + "_shap_summary.csv")
            summary_df.to_csv(out_csv, index=False)
            print(f"[{model_name}] wrote: {out_csv}")

            topN = 3
            top_feats = summary_df["feature"].head(topN).tolist()

            for feat in top_feats:
                try:
                    shap.plots.scatter(sv[:, feat], color=sv, show=False)
                    plt.title(f"{model_name} - SHAP Dependence: {feat}")
                    plt.tight_layout()
                    out = fig_dir_local / (model_name.lower().replace(" ", "_").replace("-", "") + f"_shap_dependence_{feat}.png")
                    plt.savefig(out); plt.close()
                    print(f"[{model_name}] wrote: {out}")
                except Exception as e:
                    print(f"[{model_name}] SHAP dependence skipped for {feat}: {e}")
        except Exception as e:
            print(f"[{model_name}] SHAP dependence block skipped: {e}")

        # -----------------------------
        # Decision plots
        # -----------------------------
        try:
            # Log-odds (additive) decision plot – always valid
            shap.plots.decision(
                base_value=sv.base_values[0] if np.ndim(sv.base_values) else sv.base_values,
                shap_values=sv.values[:20],
                feature_names=sv.feature_names,
                show=False
            )
            plt.title(f"{model_name} – SHAP Decision Plot (log-odds)")
            plt.tight_layout()
            out = fig_dir_local / (model_name.lower().replace(" ", "_").replace("-", "") + "_shap_decision_logit.png")
            plt.savefig(out); plt.close()
            print(f"[{model_name}] wrote: {out}")
        except Exception as e:
            print(f"[{model_name}] SHAP decision (log-odds) skipped: {e}")

        # Probability-space decision plot (only meaningful for classification)
        if is_classifier:
            try:
                shap.plots.decision(
                    base_value=sv.base_values[0] if np.ndim(sv.base_values) else sv.base_values,
                    shap_values=sv.values[:20],
                    feature_names=sv.feature_names,
                    link="logit",  # convert log-odds path to probability
                    show=False
                )
                plt.title(f"{model_name} – SHAP Decision Plot (probability)")
                plt.tight_layout()
                out = fig_dir_local / (model_name.lower().replace(" ", "_").replace("-", "") + "_shap_decision_prob.png")
                plt.savefig(out); plt.close()
                print(f"[{model_name}] wrote: {out}")
            except Exception as e:
                print(f"[{model_name}] SHAP decision (prob) skipped: {e}")

        # Summary CSV
        try:
            mean_abs = np.abs(sv.values).mean(axis=0)
            summary_df = pd.DataFrame({"feature": sv.feature_names or list(Xs.columns),
                                       "mean_abs_shap": mean_abs}).sort_values("mean_abs_shap", ascending=False)
            out_csv = indiv_dir / (model_name.lower().replace(" ", "_").replace("-", "") + "_shap_summary.csv")
            summary_df.to_csv(out_csv, index=False)
            print(f"[{model_name}] wrote: {out_csv}")
        except Exception as e:
            print(f"[{model_name}] SHAP summary skipped: {e}")

def run_ml_suite(db_path: Path, indiv_dir: Path, fig_dir_local: Path) -> None:
    """
    Run the full ML suite and write outputs to indiv_dir (CSVs) and fig_dir_local (images).
    Mirrors your original models & settings.
    """
    # Binary
    df_bin = preprocess_data(db_path, "binary")
    run_model(lambda: DecisionTreeClassifier(max_depth=5, random_state=42),
              "is_attempt_successful", df_bin, is_classifier=True,
              drop_cols=["total_score","stars_gained"],
              model_name="Decision Tree - Binary Classification",
              save_tree_image=True, fig_dir_local=fig_dir_local, indiv_dir=indiv_dir)

    run_model(lambda: RandomForestClassifier(n_estimators=100, max_depth=8, random_state=42, n_jobs=-1),
              "is_attempt_successful", df_bin, is_classifier=True,
              drop_cols=["total_score","stars_gained"],
              model_name="Random Forest - Binary Classification",
              fig_dir_local=fig_dir_local, indiv_dir=indiv_dir)

    # Ordinal
    df_ord = preprocess_data(db_path, "ordinal")
    run_model(lambda: DecisionTreeClassifier(max_depth=5, random_state=42),
              "stars_gained", df_ord, is_classifier=True,
              drop_cols=["total_score","is_attempt_successful"],
              model_name="Decision Tree - Ordinal Classification",
              save_tree_image=True, fig_dir_local=fig_dir_local, indiv_dir=indiv_dir)

    run_model(lambda: RandomForestClassifier(n_estimators=100, max_depth=8, random_state=42, n_jobs=-1),
              "stars_gained", df_ord, is_classifier=True,
              drop_cols=["total_score","is_attempt_successful"],
              model_name="Random Forest - Ordinal Classification",
              fig_dir_local=fig_dir_local, indiv_dir=indiv_dir)

    # Regression
    df_reg = preprocess_data(db_path, "regression")
    run_model(lambda: DecisionTreeRegressor(max_depth=5, random_state=42),
              "total_score", df_reg, is_classifier=False,
              drop_cols=["stars_gained","is_attempt_successful"],
              model_name="Decision Tree - Raw Regression",
              save_tree_image=True, fig_dir_local=fig_dir_local, indiv_dir=indiv_dir)

    run_model(lambda: RandomForestRegressor(n_estimators=100, max_depth=8, random_state=42, n_jobs=-1),
              "total_score", df_reg, is_classifier=False,
              drop_cols=["stars_gained","is_attempt_successful"],
              model_name="Random Forest - Raw Regression",
              fig_dir_local=fig_dir_local, indiv_dir=indiv_dir)

    # Residual regression
    df_resid = preprocess_data(db_path, "residual")
    run_model(lambda: DecisionTreeRegressor(max_depth=5, random_state=42),
              "total_score", df_resid, is_classifier=False, residual_mode=True,
              drop_cols=["stars_gained","is_attempt_successful"],
              model_name="Decision Tree - Residual Regression",
              save_tree_image=True, fig_dir_local=fig_dir_local, indiv_dir=indiv_dir)

    run_model(lambda: RandomForestRegressor(n_estimators=100, max_depth=8, random_state=42, n_jobs=-1),
              "total_score", df_resid, is_classifier=False, residual_mode=True,
              drop_cols=["stars_gained","is_attempt_successful"],
              model_name="Random Forest - Residual Regression",
              fig_dir_local=fig_dir_local, indiv_dir=indiv_dir)

    # XGBoost + SHAP on all targets
    run_model(None, "is_attempt_successful", df_bin, is_classifier=True,
              drop_cols=["total_score","stars_gained"],
              model_name="XGBoost - Binary Classification",
              model_library="xgboost", explain=True, fig_dir_local=fig_dir_local, indiv_dir=indiv_dir)

    run_model(None, "stars_gained", df_ord, is_classifier=True,
              drop_cols=["total_score","is_attempt_successful"],
              model_name="XGBoost - Ordinal Classification",
              model_library="xgboost", explain=True, fig_dir_local=fig_dir_local, indiv_dir=indiv_dir)

    run_model(None, "total_score", df_reg, is_classifier=False,
              drop_cols=["stars_gained","is_attempt_successful"],
              model_name="XGBoost - Raw Regression",
              model_library="xgboost", explain=True, fig_dir_local=fig_dir_local, indiv_dir=indiv_dir)

    run_model(None, "total_score", df_resid, is_classifier=False, residual_mode=True,
              drop_cols=["stars_gained","is_attempt_successful"],
              model_name="XGBoost - Residual Regression",
              model_library="xgboost", explain=True, fig_dir_local=fig_dir_local, indiv_dir=indiv_dir)