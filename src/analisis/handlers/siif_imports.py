__all__ = [
    "get_siif_rfondo07tp",
    "get_siif_rf602",
    "get_siif_desc_pres",
    "get_siif_ri102",
]

import datetime as dt
from typing import List, Union

import pandas as pd
from fastapi import HTTPException

from ...config import logger
from ...siif.repositories import (
    Rf602Repository,
    Rf610Repository,
    Rfondo07tpRepository,
    Ri102Repository,
)


# --------------------------------------------------
async def get_siif_rfondo07tp(
    ejercicio: int = None, filters: dict = {}
) -> pd.DataFrame:
    """
    Get the rfondo07tp (PA6) data from the repository.
    """
    try:
        if ejercicio is not None:
            filters["ejercicio"] = ejercicio
        docs = await Rfondo07tpRepository().find_by_filter(filters=filters)
        df = pd.DataFrame(docs)
        return df
    except Exception as e:
        logger.error(f"Error retrieving SIIF's rfondo07tp (PA6) from database: {e}")
        raise HTTPException(
            status_code=500,
            detail="Error retrieving SIIF's rfondo07tp (PA6) from the database",
        )


# --------------------------------------------------
async def get_siif_rf602(ejercicio: int = None, filters: dict = {}) -> pd.DataFrame:
    """
    Get the rf602 data from the repository.
    """
    try:
        if ejercicio is not None:
            filters["ejercicio"] = ejercicio
        docs = await Rf602Repository().find_by_filter(filters=filters)
        df = pd.DataFrame(docs)
        return df
    except Exception as e:
        logger.error(f"Error retrieving SIIF's rf602 from database: {e}")
        raise HTTPException(
            status_code=500,
            detail="Error retrieving SIIF's rf602 from the database",
        )


# --------------------------------------------------
async def get_siif_desc_pres(
    ejercicio_to: Union[int, List] = int(dt.datetime.now().year),
) -> pd.DataFrame:
    """
    Get the rf610 data from the repository.
    """

    if ejercicio_to is None:
        docs = await Rf610Repository().get_all()
    elif isinstance(ejercicio_to, list):
        docs = await Rf610Repository().find_by_filter(
            filters={
                "ejercicio__in": ejercicio_to,
            }
        )
    else:
        docs = await Rf610Repository().find_by_filter(
            filters={
                "ejercicio__lte": int(ejercicio_to),
            }
        )

    df = pd.DataFrame(docs)
    df.sort_values(
        by=["ejercicio", "estructura"], inplace=True, ascending=[False, True]
    )
    # Programas únicos
    df_prog = df.loc[:, ["programa", "desc_programa"]]
    df_prog.drop_duplicates(subset=["programa"], inplace=True, keep="first")
    # Subprogramas únicos
    df_subprog = df.loc[:, ["programa", "subprograma", "desc_subprograma"]]
    df_subprog.drop_duplicates(
        subset=["programa", "subprograma"], inplace=True, keep="first"
    )
    # Proyectos únicos
    df_proy = df.loc[:, ["programa", "subprograma", "proyecto", "desc_proyecto"]]
    df_proy.drop_duplicates(
        subset=["programa", "subprograma", "proyecto"], inplace=True, keep="first"
    )
    # Actividades únicos
    df_act = df.loc[
        :,
        [
            "estructura",
            "programa",
            "subprograma",
            "proyecto",
            "actividad",
            "desc_actividad",
        ],
    ]
    df_act.drop_duplicates(subset=["estructura"], inplace=True, keep="first")
    # Merge all
    df = df_act.merge(df_prog, how="left", on="programa", copy=False)
    df = df.merge(df_subprog, how="left", on=["programa", "subprograma"], copy=False)
    df = df.merge(
        df_proy, how="left", on=["programa", "subprograma", "proyecto"], copy=False
    )
    df["desc_programa"] = df.programa + " - " + df.desc_programa
    df["desc_subprograma"] = df.subprograma + " - " + df.desc_subprograma
    df["desc_proyecto"] = df.proyecto + " - " + df.desc_proyecto
    df["desc_actividad"] = df.actividad + " - " + df.desc_actividad
    df.drop(
        labels=["programa", "subprograma", "proyecto", "actividad"],
        axis=1,
        inplace=True,
    )
    return df


# --------------------------------------------------
async def get_siif_ri102(ejercicio: int = None, filters: dict = {}) -> pd.DataFrame:
    """
    Get the ri102 data from the repository.
    """
    if ejercicio is not None:
        filters["ejercicio"] = ejercicio
    docs = await Ri102Repository().safe_find_by_filter(filters=filters)
    df = pd.DataFrame(docs)
    return df
