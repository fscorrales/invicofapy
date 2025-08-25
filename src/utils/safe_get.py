__all__ = ["sanitize_dataframe_for_json", "sanitize_dataframe_for_json_with_datetime"]

from datetime import datetime

import numpy as np
import pandas as pd


# -------------------------------------------------
def sanitize_dataframe_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia un DataFrame para que sea seguro convertirlo a JSON, MongoDB o para validación con Pydantic.

    - Reemplaza np.nan, np.inf, -np.inf con None
    - Convierte np.* types a tipos nativos de Python
    """
    with pd.option_context("future.no_silent_downcasting", True):
        # Reemplazar NaN e infinitos por None
        df_clean = df.replace([np.nan, np.inf, -np.inf], None).infer_objects(copy=False)

        df_clean = df_clean.astype(object).where(pd.notnull(df_clean), None)

        # Convertir a object donde haya valores nulos para asegurar compatibilidad
        df_clean = df_clean.apply(
            lambda col: col.map(lambda x: x.item() if hasattr(x, "item") else x)
        )

        return df_clean


# -------------------------------------------------
def sanitize_dataframe_for_json_with_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia un DataFrame para que sea seguro convertirlo a JSON o subirlo a Google Sheets.

    Reemplaza:
    - np.nan, np.inf, -np.inf con None
    - np.* types con sus equivalentes nativos
    """
    with pd.option_context("future.no_silent_downcasting", True):
        # Reemplazar NaN e infinitos por None
        df_clean = df.replace([np.nan, np.inf, -np.inf, None], "").infer_objects(
            copy=False
        )
        # Convertir a object donde haya valores nulos para asegurar compatibilidad
        df_clean = df_clean.astype(object).where(pd.notnull(df_clean), None)

        # Convertir np.* types (como np.int64, np.float64) a sus tipos nativos
        df_clean = df_clean.apply(
            lambda col: col.map(lambda x: x.item() if hasattr(x, "item") else x)
        )

        def convert_value(x):
            if pd.isna(x):
                return None
            if isinstance(x, (np.integer, np.floating)):
                return x.item()
            if isinstance(x, (pd.Timestamp, datetime)):
                return x.isoformat()
            return x

        df_clean = df_clean.applymap(convert_value)

        return df_clean
