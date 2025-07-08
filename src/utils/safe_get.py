__all__ = ["sanitize_dataframe_for_json"]

import numpy as np
import pandas as pd

def sanitize_dataframe_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia un DataFrame para que sea seguro convertirlo a JSON, MongoDB o para validación con Pydantic.

    - Reemplaza np.nan, np.inf, -np.inf con None
    - Convierte np.* types a tipos nativos de Python
    """

    # Reemplazar NaN e infinitos por None
    df = df.replace([np.nan, np.inf, -np.inf], None)

    # Asegurar que todos los valores se conviertan a tipos nativos
    df = df.applymap(lambda x: x.item() if hasattr(x, "item") else x)

    return df
