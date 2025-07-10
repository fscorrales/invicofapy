__all__ = ["sanitize_dataframe_for_json"]

import numpy as np
import pandas as pd

def sanitize_dataframe_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia un DataFrame para que sea seguro convertirlo a JSON, MongoDB o para validación con Pydantic.

    - Reemplaza np.nan, np.inf, -np.inf con None
    - Convierte np.* types a tipos nativos de Python
    """
    with pd.option_context("future.no_silent_downcasting", True):
        # Reemplazar NaN e infinitos por None
        # df_clean = df.replace([np.nan, np.inf, -np.inf], None)
        df_clean = df.replace([np.nan, np.inf, -np.inf], None).infer_objects(
            copy=False
        )
        df_clean = df_clean.astype(object).where(pd.notnull(df_clean), None)
        # df_clean = df_clean.astype(object)

        # Convertir a object donde haya valores nulos para asegurar compatibilidad

        # Convertir np.* types (como np.int64, np.float64) a sus tipos nativos
        df_clean = df_clean.apply(
            lambda col: col.map(lambda x: x.item() if hasattr(x, "item") else x)
        )
        # df_clean = df_clean.applymap(lambda x: x.item() if hasattr(x, "item") else x)

        # Convertir todo a string para evitar problemas con tipos
        # df_clean = df_clean.astype(str)

        return df_clean
