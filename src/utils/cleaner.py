import pandas as pd
from typing import Optional, List, Any

class DataFrameNormalizer:
    """
    A class to normalize DataFrames for Elasticsearch indexing.
    Handles missing values, type conversions, and data cleaning.
    """
    
    def __init__(
        self,
        id_col: str = "TweetID",
        date_col: str = "CreateDate", 
        bool_col: str = "Antisemitic",
        text_col: str = "text",
    ):
        """
        Initialize the normalizer with column names.
        
        Parameters
        ----------
        id_col : str
            Column name for document ID
        date_col : str
            Column name for date/time
        bool_col : str
            Column name for boolean field
        text_col : str
            Column name for main text
        weapons_col : str
            Column name for weapons list
        """
        self.id_col = id_col
        self.date_col = date_col
        self.bool_col = bool_col
        self.text_col = text_col
    
    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main method to normalize the entire DataFrame.
        
        Parameters
        ----------
        df : pd.DataFrame
            Input DataFrame to normalize
            
        Returns
        -------
        pd.DataFrame
            Normalized DataFrame with cleaned data and handled NaN values
        """
        out = df.copy()
        
        out = self._handle_missing_values(out)
        out = self._normalize_date_column(out)
        out = self._normalize_bool_column(out)
        out = self._normalize_id_column(out)
        
        return out
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle NaN values across the DataFrame."""
        out = df.copy()
        
        if self.text_col in out.columns:
            out[self.text_col] = out[self.text_col].fillna("")
        
        if self.bool_col in out.columns:
            out[self.bool_col] = out[self.bool_col].fillna(False)
           
        return out
    
    def _normalize_date_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize date column to ISO-8601 UTC format."""
        if self.date_col not in df.columns:
            return df
            
        out = df.copy()
        s = pd.to_datetime(out[self.date_col], errors="coerce", utc=True)
        out[self.date_col] = s.dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return out
    
    def _normalize_bool_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize boolean column values."""
        if self.bool_col not in df.columns:
            return df
            
        out = df.copy()
        out[self.bool_col] = out[self.bool_col].map(self._to_bool)
        return out
    
    def _normalize_id_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize ID column to stable string format."""
        if self.id_col not in df.columns:
            return df
            
        out = df.copy()
        out[self.id_col] = out[self.id_col].map(self._to_stable_str)
        return out
    
    def _to_bool(self, v: Any) -> bool:
        """Convert value to boolean, handling various formats."""
        if pd.isna(v):
            return False
        if isinstance(v, bool):
            return v
        try:
            n = pd.to_numeric(v, errors="coerce")
            if not pd.isna(n):
                return bool(int(n))
        except Exception:
            pass
        s = str(v).strip().lower()
        if s in {"true", "t", "yes", "y"}:
            return True
        if s in {"false", "f", "no", "n"}:
            return False
        return False
    
    def _to_stable_str(self, v: Any) -> Optional[str]:
        """Convert value to stable string format."""
        if pd.isna(v):
            return None
        try:
            n = pd.to_numeric(v, errors="coerce")
            if not pd.isna(n) and float(n).is_integer():
                return str(int(n))
            return str(v)
        except Exception:
            return str(v)
    
    def _to_list_str(self, v: Any) -> List[str]:
        """Convert value to list of strings."""
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return []
        if isinstance(v, list):
            return [str(x) for x in v if not pd.isna(x)]
        return [str(v)]


