# controllers/common.py
"""
Common utilities and helper functions shared across controllers.
"""

from typing import Any, Dict, List
from datetime import datetime
import pandas as pd
import numpy as np
import logging
import traceback

# Configure logging
logger = logging.getLogger(__name__)


def safe_convert_to_records(df: Any, default_value: Any = None) -> List[Dict]:
    """
    Safely convert DataFrame/Series to list of records.
    Handles various data types and edge cases including NaN values.
    """
    if default_value is None:
        default_value = []

    if df is None:
        return default_value

    # Handle Series
    if isinstance(df, pd.Series):
        result = []
        for idx, val in df.items():
            # Handle NaN in Series
            if pd.isna(val):
                val = None
            elif isinstance(val, (np.floating, np.integer)):
                val = None if np.isnan(val) else val.item()
            result.append({"index": str(idx), "value": val})
        return result

    # Handle DataFrame
    if isinstance(df, pd.DataFrame):
        if df.empty:
            return default_value

        # Handle MultiIndex columns
        if isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = ['_'.join(map(str, col)).strip('_') for col in df.columns.values]

        # Convert to records
        records = df.to_dict(orient="records")

        # Clean up special types (datetime, numpy types, NaN)
        for record in records:
            for key, value in record.items():
                if value is None:
                    continue
                # Handle NaN/Inf float values
                if isinstance(value, float):
                    if np.isnan(value) or np.isinf(value):
                        record[key] = None
                # Handle numpy types
                elif isinstance(value, (np.floating)):
                    if np.isnan(value) or np.isinf(value):
                        record[key] = None
                    else:
                        record[key] = float(value)
                elif isinstance(value, np.integer):
                    record[key] = int(value)
                # Handle datetime
                elif isinstance(value, (datetime, pd.Timestamp)):
                    record[key] = value.isoformat() if pd.notna(value) else None
                # Handle pandas NA
                elif pd.isna(value):
                    record[key] = None

        return records

    # Handle dict
    if isinstance(df, dict):
        return [df]

    # Handle list
    if isinstance(df, list):
        return df

    return default_value

