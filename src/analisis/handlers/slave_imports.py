__all__ = [
    "get_slave_honorarios",
]

import pandas as pd
from fastapi import HTTPException

from ...config import logger
from ...slave.repositories import HonorariosRepository


# --------------------------------------------------
async def get_slave_honorarios(
    ejercicio: int = None, filters: dict = {}
) -> pd.DataFrame:
    """
    Get the slave_honorarios data from the repository.
    """
    try:
        if ejercicio is not None:
            filters["ejercicio"] = ejercicio
        docs = await HonorariosRepository().find_by_filter(filters=filters)
        df = pd.DataFrame(docs)
        return df
    except Exception as e:
        logger.error(
            f"Error retrieving Slave's Honorarios Factureros Data from database: {e}"
        )
        raise HTTPException(
            status_code=500,
            detail="Error retrieving Slave's Honorarios Factureros Data from the database",
        )
