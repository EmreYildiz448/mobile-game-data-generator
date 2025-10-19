from pathlib import Path
import json
import uuid
import pandas as pd

def _ensure_dir(path):
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p

def _normalize_objects(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make DataFrame columns CSV/Parquet-friendly:
      - uuid.UUID  -> str
      - dict/list  -> JSON string (double-quoted via json.dumps)
      - object datetimes -> pandas datetime
    """
    out = df.copy()
    for col in out.columns:
        s = out[col]
        if s.dtype != "object":
            continue

        # find a non-null sample
        sample = next((v for v in s.values if v is not None and not (isinstance(v, float) and pd.isna(v))), None)
        if sample is None:
            continue

        if isinstance(sample, uuid.UUID):
            out[col] = s.astype(str)
        elif isinstance(sample, (dict, list)):
            out[col] = s.apply(
                lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list))
                else (None if x is None or (isinstance(x, float) and pd.isna(x)) else str(x))
            )
        elif not pd.api.types.is_datetime64_any_dtype(s) and (
            hasattr(sample, "isoformat") and "datetime" in type(sample).__module__
        ):
            out[col] = pd.to_datetime(s, errors="coerce")
    return out

# keep the parquet-specific helper for clarity
def _normalize_for_parquet(df):
    return _normalize_objects(df)

def write_tables(tables, out_dir="data", fmt="csv", sample_rows=None):
    out = _ensure_dir(out_dir)
    if sample_rows is not None:
        sample_rows = int(sample_rows)

    written = {}
    for name, df in tables.items():
        df_to_write = df

        if fmt == "csv":
            # NEW: normalize dict/list/uuid/datetime to JSON-friendly strings BEFORE CSV write
            df_to_write = _normalize_objects(df_to_write)
            path = out / f"{name}.csv"
            df_to_write.to_csv(path, index=False, encoding="utf-8", na_rep="")
        elif fmt == "parquet":
            path = out / f"{name}.parquet"
            df_to_write = _normalize_for_parquet(df_to_write)
            df_to_write.to_parquet(path, index=False)
        else:
            raise ValueError(f"Unsupported format: {fmt}")

        written[name] = path

        if sample_rows:
            sample = df_to_write.head(sample_rows)
            if fmt == "csv":
                sample.to_csv(out / f"{name}_sample.csv", index=False, encoding="utf-8", na_rep="")
            else:
                sample.to_parquet(out / f"{name}_sample.parquet", index=False)

    return written
