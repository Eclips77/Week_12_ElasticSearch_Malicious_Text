import pandas as pd
from typing import Optional, List, Any

def normalize_for_elasticsearch(
    df: pd.DataFrame,
    id_col: str = "TweetID",
    date_col: str = "CreateDate",
    bool_col: str = "Antisemitic",
    text_col: str = "text",
    weapons_col: str = "weapons"
) -> pd.DataFrame:
    """
    Normalize a DataFrame to be safely indexed into Elasticsearch.
    
    This function:
    1) Converts `date_col` to ISO-8601 with Z (UTC).
    2) Converts `bool_col` values like 0/1, "0"/"1", "true"/"false", etc. into real booleans.
    3) Converts `id_col` to a stable string (avoids scientific notation for large integers).
    4) Ensures `text_col` is a string.
    5) Ensures `weapons_col` is a list[str] (empty list if missing/NaN).
    
    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame to normalize. The function does not mutate the original.
    id_col : str
        Column name that holds the external document ID (kept as a field, not _id).
    date_col : str
        Column name for date/time.
    bool_col : str
        Column name for a boolean field.
    text_col : str
        Column name for the main text field.
    weapons_col : str
        Column name for weapons (string or list -> coerced to list[str]).
    
    Returns
    -------
    pd.DataFrame
        A new DataFrame with normalized types and values.
    """
    out = df.copy()

    if date_col in out.columns:
        s = pd.to_datetime(out[date_col], errors="coerce", utc=True)
        out[date_col] = s.dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    if bool_col in out.columns:
        def _to_bool(v: Any) -> Optional[bool]:
            if pd.isna(v):
                return False
            if isinstance(v, bool):
                return v
            try:
                n = pd.to_numeric(v, errors="coerce")
                if pd.isna(n):
                    pass
                else:
                    return bool(int(n))
            except Exception:
                pass
            s = str(v).strip().lower()
            if s in {"true", "t", "yes", "y"}:
                return True
            if s in {"false", "f", "no", "n"}:
                return False
            return False
        out[bool_col] = out[bool_col].map(_to_bool)

    if id_col in out.columns:
        def _to_stable_str(v: Any) -> Optional[str]:
            if pd.isna(v):
                return None
            try:
                n = pd.to_numeric(v, errors="coerce")
                if pd.isna(n):
                    return str(v)
                if float(n).is_integer():
                    return str(int(n))
                return str(v)
            except Exception:
                return str(v)
        out[id_col] = out[id_col].map(_to_stable_str)

    if text_col in out.columns:
        out[text_col] = out[text_col].astype(str)

    if weapons_col in out.columns:
        def _to_list_str(v: Any) -> List[str]:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return []
            if isinstance(v, list):
                return [str(x) for x in v if not pd.isna(x)]
            return [str(v)]
        out[weapons_col] = out[weapons_col].map(_to_list_str)

    return out
