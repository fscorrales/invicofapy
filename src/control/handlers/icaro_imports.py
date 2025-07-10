__all__ = ["get_icaro_carga"]

import pandas as pd
from fastapi import HTTPException

from ...config import logger
from ...icaro.repositories import CargaRepository


# --------------------------------------------------
async def get_icaro_carga(ejercicio: int = None, filters: dict = {}) -> pd.DataFrame:
    """
    Get the icaro_carga data from the repository.
    """
    try:
        if ejercicio is not None:
            filters["ejercicio"] = ejercicio
        docs = await CargaRepository().find_by_filter(filters=filters)
        df = pd.DataFrame(docs)
        return df
    except Exception as e:
        logger.error(f"Error retrieving Icaro's Carga Data from database: {e}")
        raise HTTPException(
            status_code=500,
            detail="Error retrieving Icaro's Carga Data from the database",
        )
