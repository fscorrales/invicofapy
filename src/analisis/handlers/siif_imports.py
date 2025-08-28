__all__ = [
    "get_siif_rfondo07tp",
    "get_siif_rf602",
    "get_siif_desc_pres",
    "get_siif_ri102",
    "get_siif_rci02_unified_cta_cte",
    "get_siif_comprobantes_gtos_joined",
    "get_planillometro_hist",
]

import datetime as dt
from typing import List, Union

import pandas as pd
from fastapi import HTTPException

from ...config import logger
from ...siif.repositories import (
    PlanillometroHistRepository,
    Rcg01UejpRepository,
    Rci02Repository,
    Rf602Repository,
    Rf610Repository,
    Rfondo07tpRepository,
    Ri102Repository,
    Rpa03gRepository,
)
from ...sscc.repositories import CtasCtesRepository


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
async def get_siif_comprobantes_gtos_joined() -> pd.DataFrame:
    """
    Join gto_rpa03g (gtos_gpo_part) with rcg01_uejp (gtos)
    """
    try:
        df_gtos_gpo_part = pd.DataFrame(await Rpa03gRepository().get_all())
        df_gtos = pd.DataFrame(await Rcg01UejpRepository().get_all())
        df_gtos_filtered = df_gtos[
            [
                "nro_comprobante",
                "nro_fondo",
                "fuente",
                "cta_cte",
                "cuit",
                "clase_reg",
                "clase_mod",
                "clase_gto",
                "es_comprometido",
                "es_verificado",
                "es_aprobado",
                "es_pagado",
            ]
        ]
        df = pd.merge(
            left=df_gtos_gpo_part,
            right=df_gtos_filtered,
            on=["nro_comprobante"],
            how="left",
        )
        # self.df.drop(["grupo"], axis=1, inplace=True)
        # self.df = pd.merge(left=self.df, right=self.df_part, on=["partida"], how="left")
        return df
    except Exception as e:
        logger.error(f"Error retrieving SIIF's rf602 from database: {e}")
        raise HTTPException(
            status_code=500,
            detail="Error retrieving SIIF's rf602 from the database",
        )


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


# --------------------------------------------------
async def get_siif_rci02_unified_cta_cte(
    ejercicio: int = None, filters: dict = {}
) -> pd.DataFrame:
    """
    Get the rci02 data from the repository.
    """
    if ejercicio is not None:
        filters["ejercicio"] = ejercicio
    docs = await Rci02Repository().safe_find_by_filter(filters=filters)
    df = pd.DataFrame(docs)
    # logger.info(f"df.shape: {df.shape} - df.head: {df.head()}")
    df.reset_index(drop=True, inplace=True)
    ctas_ctes = pd.DataFrame(await CtasCtesRepository().get_all())
    # logger.info(f"ctas_ctes.shape: {ctas_ctes.shape} - ctas_ctes.head: {ctas_ctes.head()}")
    map_to = ctas_ctes.loc[:, ["map_to", "siif_recursos_cta_cte"]]
    df = pd.merge(
        df, map_to, how="left", left_on="cta_cte", right_on="siif_recursos_cta_cte"
    )
    # logger.info(f"df.shape: {df.shape} - df.head: {df.head()}")
    df["cta_cte"] = df["map_to"]
    df.drop(["map_to", "siif_recursos_cta_cte", "_id"], axis="columns", inplace=True)
    # logger.info(f"df.shape: {df.shape} - df.head: {df.head()}")
    return df


# --------------------------------------------------
async def get_planillometro_hist(filters: dict = {}) -> pd.DataFrame:
    """
    Get the Planillometro Hist data from the repository.
    """
    try:
        docs = await PlanillometroHistRepository().find_by_filter(filters=filters)
        df = pd.DataFrame(docs)
        return df
    except Exception as e:
        logger.error(f"Error retrieving Planillometro Historico from database: {e}")
        raise HTTPException(
            status_code=500,
            detail="Error retrieving Planillometro Historico from the database",
        )
