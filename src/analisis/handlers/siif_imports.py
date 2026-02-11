__all__ = [
    "get_siif_rfondos04",
    "get_siif_rfondo07tp",
    "get_siif_rf602",
    "get_siif_desc_pres",
    "get_siif_ri102",
    "get_siif_rci02_unified_cta_cte",
    "get_siif_rcg01_uejp",
    "get_siif_comprobantes_gtos_joined",
    "get_siif_comprobantes_gtos_unified_cta_cte",
    "get_planillometro_hist",
    "get_siif_rfp_p605b",
    "get_siif_rdeu012_unified_cta_cte",
    "get_siif_rvicon03",
    "get_siif_rcocc31",
    "get_siif_comprobantes_haberes",
    "get_siif_comprobantes_honorarios",
]

import datetime as dt
from typing import List, Union

import numpy as np
import pandas as pd
from fastapi import HTTPException

from ...config import logger
from ...siif.repositories import (
    PlanillometroHistRepository,
    Rcg01UejpRepository,
    Rci02Repository,
    Rcocc31Repository,
    Rdeu012Repository,
    Rf602Repository,
    Rf610Repository,
    Rfondo07tpRepository,
    Rfondos04Repository,
    RfpP605bRepository,
    Ri102Repository,
    Rpa03gRepository,
    Rvicon03Repository,
)
from ...sscc.repositories import CtasCtesRepository


# --------------------------------------------------
async def get_siif_rfondos04(ejercicio: int = None, filters: dict = {}) -> pd.DataFrame:
    """
    Get the rfondos04 (PA3, REV) data from the repository.
    """
    try:
        if ejercicio is not None:
            filters["ejercicio"] = ejercicio
        docs = await Rfondos04Repository().find_by_filter(filters=filters)
        df = pd.DataFrame(docs)
        return df
    except Exception as e:
        logger.error(f"Error retrieving SIIF's rfondos04 from database: {e}")
        raise HTTPException(
            status_code=500,
            detail="Error retrieving SIIF's rfondos04 from the database",
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
    # Reemplazar los NaN por una cadena vacía en la columna 'desc_actividad'
    df['desc_actividad'] = df['desc_actividad'].fillna('')
    
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
async def get_siif_rcg01_uejp(
    ejercicio: int = None, filters: dict = {}
) -> pd.DataFrame:
    """
    Get the rcg01_uejp data from the repository.
    """
    if ejercicio is not None:
        filters["ejercicio"] = ejercicio
    docs = await Rcg01UejpRepository().safe_find_by_filter(filters=filters)
    df = pd.DataFrame(docs)
    return df


# --------------------------------------------------
async def get_siif_comprobantes_gtos_joined(
    ejercicio: int = None, partidas: list = []
) -> pd.DataFrame:
    """
    Join gto_rpa03g (gtos_gpo_part) with rcg01_uejp (gtos)
    """
    try:
        if ejercicio is None:
            docs_gtos_gpo_part = await Rpa03gRepository().get_all()
            docs_gtos = await Rcg01UejpRepository().get_all()
        else:
            filters = {
                "ejercicio": int(ejercicio),
            }
            docs_gtos = await Rcg01UejpRepository().find_by_filter(filters=filters)
            if len(partidas) > 0:
                filters.update(
                    {
                        "partida__in": partidas,
                    }
                )
            docs_gtos_gpo_part = await Rpa03gRepository().find_by_filter(
                filters=filters
            )
        df_gtos_gpo_part = pd.DataFrame(docs_gtos_gpo_part)
        df_gtos = pd.DataFrame(docs_gtos)
        # df_gtos_gpo_part = pd.DataFrame(await Rpa03gRepository().get_all())
        # df_gtos = pd.DataFrame(await Rcg01UejpRepository().get_all())
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
async def get_siif_comprobantes_gtos_unified_cta_cte(
    ejercicio: int = None, partidas: list = []
) -> pd.DataFrame:
    """
    Get the comprobantes gtos joined data from the repository.
    """
    df = await get_siif_comprobantes_gtos_joined(ejercicio=ejercicio, partidas=partidas)
    # logger.info(f"len(docs): {len(docs)}")
    if not df.empty:
        # logger.info(f"df.shape: {df.shape} - df.head: {df.head()}")
        ctas_ctes = pd.DataFrame(await CtasCtesRepository().get_all())
        map_to = ctas_ctes.loc[:, ["map_to", "siif_gastos_cta_cte"]]
        df = pd.merge(
            df,
            map_to,
            how="left",
            left_on="cta_cte",
            right_on="siif_gastos_cta_cte",
        )
        df["cta_cte"] = df["map_to"]
        df.drop(["map_to", "siif_gastos_cta_cte"], axis="columns", inplace=True)
        # logger.info(f"df.shape: {df.shape} - df.head: {df.head()}")
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


# --------------------------------------------------
async def get_siif_rfp_p605b(ejercicio: int = None, filters: dict = {}) -> pd.DataFrame:
    """
    Get the rfp_p605b data from the repository.
    """
    if ejercicio is not None:
        filters["ejercicio"] = ejercicio
    docs = await RfpP605bRepository().safe_find_by_filter(filters=filters)
    df = pd.DataFrame(docs)
    return df


# --------------------------------------------------
async def get_siif_rdeu012_unified_cta_cte(
    ejercicio: int = None, filters: dict = {}
) -> pd.DataFrame:
    """
    Get the rdeu012 data from the repository.
    """
    if ejercicio is not None:
        filters.update({"ejercicio": ejercicio})
    docs = await Rdeu012Repository().find_by_filter(filters=filters)
    # logger.info(f"len(docs): {len(docs)}")
    df = pd.DataFrame(docs)
    df.reset_index(drop=True, inplace=True)
    if not df.empty:
        # logger.info(f"df.shape: {df.shape} - df.head: {df.head()}")
        ctas_ctes = pd.DataFrame(await CtasCtesRepository().get_all())
        map_to = ctas_ctes.loc[:, ["map_to", "siif_contabilidad_cta_cte"]]
        df = pd.merge(
            df,
            map_to,
            how="left",
            left_on="cta_cte",
            right_on="siif_contabilidad_cta_cte",
        )
        df["cta_cte"] = df["map_to"]
        df.drop(["map_to", "siif_contabilidad_cta_cte"], axis="columns", inplace=True)
        # logger.info(f"df.shape: {df.shape} - df.head: {df.head()}")
    return df


# --------------------------------------------------
async def get_siif_rvicon03(ejercicio: int = None, filters: dict = {}) -> pd.DataFrame:
    """
    Get the rvicon03 data from the repository.
    """
    if ejercicio is not None:
        filters["ejercicio"] = ejercicio
    docs = await Rvicon03Repository().safe_find_by_filter(filters=filters)
    df = pd.DataFrame(docs)
    return df


# --------------------------------------------------
async def get_siif_rcocc31(ejercicio: int = None, filters: dict = {}) -> pd.DataFrame:
    """
    Get the rcocc31 data from the repository.
    """
    if ejercicio is not None:
        filters["ejercicio"] = ejercicio
    docs = await Rcocc31Repository().safe_find_by_filter(filters=filters)
    df = pd.DataFrame(docs)
    return df


# --------------------------------------------------
async def get_siif_comprobantes_haberes(
    ejercicio: str = None,
    neto_art: bool = False,
    neto_gcias_310: bool = False,
) -> pd.DataFrame:
    """
    Get comprobantes haberes data from the repository.
    """
    df = await get_siif_comprobantes_gtos_unified_cta_cte(ejercicio=ejercicio)
    df = df.loc[df["cta_cte"] == "130832-04"]
    if neto_art:
        df = df.loc[~df["partida"].isin(["150", "151"])]
    if neto_gcias_310:
        filters = {"auxiliar_1__in": ["245", "310"], "tipo_comprobante": {"$ne": "APE"}}
        gcias_310 = await get_siif_rcocc31(ejercicio=ejercicio, filters=filters)
        gcias_310["nro_comprobante"] = (
            gcias_310["nro_entrada"].str.zfill(5)
            + "/"
            + gcias_310["ejercicio"].astype(str).str[-2:]
            + "A"
        )
        gcias_310["importe"] = gcias_310["creditos"] * (-1)
        gcias_310["grupo"] = "100"
        gcias_310["partida"] = gcias_310["auxiliar_1"]
        gcias_310["nro_origen"] = gcias_310["nro_entrada"]
        gcias_310["nro_expte"] = "90000000" + gcias_310["ejercicio"].astype(str)
        gcias_310["glosa"] = np.where(
            gcias_310["auxiliar_1"] == "245",
            "RET. GCIAS. 4TA CATEGORÍA",
            "HABERES ERRONEOS COD 310",
        )
        gcias_310["beneficiario"] = "INSTITUTO DE VIVIENDA DE CORRIENTES"
        gcias_310["nro_fondo"] = None
        gcias_310["fuente"] = "11"
        gcias_310["cta_cte"] = "130832-04"
        gcias_310["cuit"] = "30632351514"
        gcias_310["clase_reg"] = "CYO"
        gcias_310["clase_mod"] = "NOR"
        gcias_310["clase_gto"] = "REM"
        gcias_310["es_comprometido"] = True
        gcias_310["es_verificado"] = True
        gcias_310["es_aprobado"] = True
        gcias_310["es_pagado"] = True
        gcias_310 = gcias_310.loc[
            :,
            [
                "ejercicio",
                "mes",
                "fecha",
                "nro_comprobante",
                "importe",
                "grupo",
                "partida",
                "nro_entrada",
                "nro_origen",
                "nro_expte",
                "glosa",
                "beneficiario",
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
            ],
        ]
        df = pd.concat([df, gcias_310])

    return df


# --------------------------------------------------
async def get_siif_comprobantes_honorarios(
    ejercicio: str = None,
) -> pd.DataFrame:
    """
    Get comprobantes honorarios factureros data from the repository.
    """
    df = await get_siif_comprobantes_gtos_unified_cta_cte(ejercicio=ejercicio)
    df = df.loc[df["cuit"] == "30632351514"]
    df = df.loc[df["grupo"] == "300"]
    df = df.loc[df["partida"] != "384"]
    df = df.loc[df["cta_cte"].isin(["130832-05", "130832-07"])]
    keep = ["HONOR", "RECON", "LOC"]
    df = df.loc[df.glosa.str.contains("|".join(keep))]
    df = df.reset_index(drop=True)
    return df
