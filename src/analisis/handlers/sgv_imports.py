__all__ = [
    "get_sgv_saldos_barrios_evolucion",
]

import pandas as pd
from fastapi import HTTPException

from ...config import logger
from ...sgv.repositories import SaldosBarriosEvolucionRepository


# --------------------------------------------------
async def get_sgv_saldos_barrios_evolucion(
    ejercicio: int = None, filters: dict = {}
) -> pd.DataFrame:
    """
    Get the saldos barrios evolucion data from the repository.
    """
    try:
        if ejercicio is not None:
            filters["ejercicio"] = ejercicio
        docs = await SaldosBarriosEvolucionRepository().find_by_filter(filters=filters)
        df = pd.DataFrame(docs)
        return df
    except Exception as e:
        logger.error(
            f"Error retrieving SGV's saldos barrios evolucion from database: {e}"
        )
        raise HTTPException(
            status_code=500,
            detail="Error retrieving SGV's saldos barrios evolucion from the database",
        )
