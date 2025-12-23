__all__ = [
    "get_icaro_carga",
    "get_icaro_obras",
    "get_icaro_proveedores",
    "get_icaro_estructuras_desc",
    "get_icaro_carga_unified_cta_cte",
    "generate_icaro_carga_desc",
]

import pandas as pd
from fastapi import HTTPException

from ...config import logger
from ...icaro.repositories import (
    CargaRepository,
    EstructurasRepository,
    ObrasRepository,
    ProveedoresRepository,
)
from ...sscc.repositories import CtasCtesRepository
from .siif_imports import get_siif_desc_pres

# --------------------------------------------------
async def get_icaro_carga_unified_cta_cte(
    ejercicio: int = None, filters: dict = {}
) -> pd.DataFrame:
    """
    Get the Icaro's Carga data from the repository.
    """
    if ejercicio is not None:
        filters.update({"ejercicio": ejercicio})
    docs = await CargaRepository().find_by_filter(filters=filters)
    # logger.info(f"len(docs): {len(docs)}")
    df = pd.DataFrame(docs)
    df.reset_index(drop=True, inplace=True)
    if not df.empty:
        # logger.info(f"df.shape: {df.shape} - df.head: {df.head()}")
        ctas_ctes = pd.DataFrame(await CtasCtesRepository().get_all())
        map_to = ctas_ctes.loc[:, ["map_to", "icaro_cta_cte"]]
        df = pd.merge(df, map_to, how="left", left_on="cta_cte", right_on="icaro_cta_cte")
        df["cta_cte"] = df["map_to"]
        df.drop(["map_to", "icaro_cta_cte"], axis="columns", inplace=True)
        # logger.info(f"df.shape: {df.shape} - df.head: {df.head()}")
    return df

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


# --------------------------------------------------
async def get_icaro_obras() -> pd.DataFrame:
    """
    Get the icaro_carga data from the repository.
    """
    try:
        docs = await ObrasRepository().get_all()
        df = pd.DataFrame(docs)
        return df
    except Exception as e:
        logger.error(f"Error retrieving Icaro's Obras Data from database: {e}")
        raise HTTPException(
            status_code=500,
            detail="Error retrieving Icaro's Obras Data from the database",
        )


# --------------------------------------------------
async def get_icaro_proveedores() -> pd.DataFrame:
    """
    Get the icaro_proveedores data from the repository.
    """
    try:
        docs = await ProveedoresRepository().get_all()
        df = pd.DataFrame(docs)
        return df
    except Exception as e:
        logger.error(f"Error retrieving Icaro's Proveedores Data from database: {e}")
        raise HTTPException(
            status_code=500,
            detail="Error retrieving Icaro's Proveedores Data from the database",
        )


# --------------------------------------------------
async def get_icaro_estructuras_desc() -> pd.DataFrame:
    """
    Get the icaro_estructuras data from the repository.
    """
    try:
        docs = await EstructurasRepository().get_all()
        df = pd.DataFrame(docs)
        df_prog = df.loc[len(df["estructura"]) == 2]
        df_prog = df_prog.rename(
            columns={"estructura": "programa", "desc_estructura": "desc_programa"}
        )
        df_subprog = df.loc[len(df["estructura"]) == 5]
        df_subprog = df_prog.rename(
            columns={"estructura": "subprograma", "desc_estructura": "desc_subprograma"}
        )
        df_proy = df.loc[len(df["estructura"]) == 8]
        df_proy = df_prog.rename(
            columns={"estructura": "proyecto", "desc_estructura": "desc_proyecto"}
        )
        df_act = df.loc[len(df["estructura"]) == 11]
        df_act = df_prog.rename(
            columns={"estructura": "actividad", "desc_estructura": "desc_actividad"}
        )
        df_act["programa"] = df_act["actividad"].str[0:2]
        df_act["subprograma"] = df_act["actividad"].str[0:5]
        df_act["proyecto"] = df_act["actividad"].str[0:8]
        # Merge all
        df = df_act.merge(df_proy, how="left", on="proyecto", copy=False)
        df = df.merge(df_subprog, how="left", on="subprograma", copy=False)
        df = df.merge(df_prog, how="left", on="programa", copy=False)
        # Combine number with description
        df["programa_desc"] = df["actividad"].str[0:2] + " - " + df["desc_programa"]
        df.desc_subprograma.fillna(value="", inplace=True)
        df["subprograma_desc"] = (
            df["actividad"].str[3:5] + " - " + df["desc_subprograma"]
        )
        df["proyecto_desc"] = df["actividad"].str[6:8] + " - " + df["desc_proyecto"]
        df["actividad_desc"] = df["actividad"].str[9:11] + " - " + df["desc_actividad"]

        df = df.loc[
            :,
            [
                "actividad",
                "programa_desc",
                "subprograma_desc",
                "proyecto_desc",
                "actividad_desc",
            ],
        ]
        return df
    except Exception as e:
        logger.error(f"Error retrieving Icaro's Estructuras Data from database: {e}")
        raise HTTPException(
            status_code=500,
            detail="Error retrieving Icaro's Estructuras Data from the database",
        )

# --------------------------------------------------
async def generate_icaro_carga_desc(
    ejercicio: int = None,
    es_desc_siif: bool = True,
    es_ejercicio_to: bool = True,
    es_neto_pa6: bool = True,
):
    filters = {}
    filters["partida"] = {"$in": ["421", "422"]}

    if es_ejercicio_to:
        filters["ejercicio"] = {"$lte": ejercicio}
    else:
        filters["ejercicio"] = ejercicio

    if es_neto_pa6:
        filters["tipo"] = {"$ne": "PA6"}
    else:
        filters["tipo"] = {"$ne": "REG"}

    df = await get_icaro_carga(filters=filters)

    if es_desc_siif:
        df["estructura"] = df["actividad"] + "-" + df["partida"]
        df = df.merge(
            await get_siif_desc_pres(ejercicio_to=ejercicio),
            how="left",
            on="estructura",
            copy=False,
        )
        df.drop(labels=["estructura"], axis="columns", inplace=True)
    else:
        df = df.merge(
            await get_icaro_estructuras_desc(),
            how="left",
            on="actividad",
            copy=False,
        )
    df.reset_index(drop=True, inplace=True)
    prov = await get_icaro_proveedores()
    prov = prov.loc[:, ["cuit", "desc_proveedor"]]
    prov.drop_duplicates(subset=["cuit"], inplace=True)
    prov.rename(columns={"desc_proveedor": "proveedor"}, inplace=True)
    df = df.merge(prov, how="left", on="cuit", copy=False)
    return df