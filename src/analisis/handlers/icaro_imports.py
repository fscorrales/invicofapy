__all__ = [
    "get_icaro_carga",
    "get_icaro_obras",
    "get_icaro_proveedores",
    "get_icaro_estructuras_desc",
    "get_icaro_carga_unified_cta_cte",
    "get_full_icaro_carga_desc",
    "get_icaro_planillometro_contabilidad",
]

import datetime as dt

import numpy as np
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
from .siif_imports import get_planillometro_hist, get_siif_desc_pres


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
        df = pd.merge(
            df, map_to, how="left", left_on="cta_cte", right_on="icaro_cta_cte"
        )
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
        df = df.drop(columns=["_id"])

        df_prog = df.loc[df["estructura"].str.len() == 2].copy()
        df_prog = df_prog.rename(
            columns={"estructura": "programa", "desc_estructura": "desc_programa"}
        )
        df_subprog = df.loc[df["estructura"].str.len() == 5].copy()
        df_subprog = df_subprog.rename(
            columns={"estructura": "subprograma", "desc_estructura": "desc_subprograma"}
        )
        df_proy = df.loc[df["estructura"].str.len() == 8].copy()
        df_proy = df_proy.rename(
            columns={"estructura": "proyecto", "desc_estructura": "desc_proyecto"}
        )
        df_act = df.loc[df["estructura"].str.len() == 11].copy()
        df_act = df_act.rename(
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
        df["desc_programa"] = df["actividad"].str[0:2] + " - " + df["desc_programa"]
        df.desc_subprograma.fillna(value="", inplace=True)
        df["desc_subprograma"] = (
            df["actividad"].str[3:5] + " - " + df["desc_subprograma"]
        )
        df["desc_proyecto"] = df["actividad"].str[6:8] + " - " + df["desc_proyecto"]
        df["desc_actividad"] = df["actividad"].str[9:11] + " - " + df["desc_actividad"]

        df = df.loc[
            :,
            [
                "actividad",
                "desc_programa",
                "desc_subprograma",
                "desc_proyecto",
                "desc_actividad",
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
async def get_full_icaro_carga_desc(
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

    # Agregar proveedores
    prov = await get_icaro_proveedores()
    prov = prov.loc[:, ["cuit", "desc_proveedor"]]
    prov.drop_duplicates(subset=["cuit"], inplace=True)
    prov.rename(columns={"desc_proveedor": "proveedor"}, inplace=True)
    df = df.merge(prov, how="left", on="cuit", copy=False)
    return df


# --------------------------------------------------
async def get_icaro_planillometro_contabilidad(
    ejercicio: int = None,
    es_desc_siif: bool = True,
    ultimos_ejercicios: str = "All",
    desagregar_desc_subprog: bool = True,
    desagregar_obras: bool = False,
    desagregar_partida: bool = False,
    desagregar_fuente: bool = False,
    agregar_acum_2008: bool = True,
    date_up_to: dt.date = None,
    include_pa6: bool = False,
):
    df = await get_full_icaro_carga_desc(ejercicio=ejercicio, es_desc_siif=es_desc_siif)
    df.sort_values(["actividad", "partida", "fuente"], inplace=True)

    # Grupos de columnas
    group_cols = ["desc_programa"]
    if desagregar_desc_subprog:
        group_cols = group_cols + ["desc_subprograma"]
    group_cols = group_cols + ["desc_proyecto", "desc_actividad", "actividad", "partida"]
    if desagregar_obras:
        group_cols = group_cols + ["desc_obra"]
    if desagregar_fuente:
        group_cols = group_cols + ["fuente"]

    # Eliminamos aquellos ejercicios anteriores a 2009
    df = df.loc[df.ejercicio.astype(int) >= 2009]

    # Incluimos PA6 (ultimo ejercicio)
    if include_pa6:
        df = df.loc[df.ejercicio.astype(int) < int(ejercicio)]
        df_last = await get_full_icaro_carga_desc(
            ejercicio=ejercicio,
            es_desc_siif=es_desc_siif,
            es_ejercicio_to=False,
            es_neto_pa6=False,
        )
        df = pd.concat([df, df_last], axis=0)

    # Filtramos hasta una fecha máxima
    if date_up_to:
        date_up_to = np.datetime64(date_up_to)
        df = df.loc[df["fecha"] <= date_up_to]

    # Agregamos ejecución acumulada de Patricia
    if agregar_acum_2008:
        df_acum_2008 = await get_planillometro_hist()
        df_acum_2008["ejercicio"] = 2008
        df_acum_2008["avance"] = 1
        df_acum_2008["desc_obra"] = df_acum_2008["desc_actividad"]
        df_acum_2008 = df_acum_2008.rename(columns={"acum_2008": "importe"})
        df["estructura"] = df["actividad"] + "-" + df["partida"]
        df_dif = df_acum_2008.loc[
            df_acum_2008["estructura"].isin(df["estructura"].unique().tolist())
        ]
        df_dif = df_dif.drop(
            columns=[
                "desc_programa",
                "desc_subprograma",
                "desc_proyecto",
                "desc_actividad",
            ]
        )
        if desagregar_desc_subprog:
            columns_to_merge = [
                "estructura",
                "desc_programa",
                "desc_subprograma",
                "desc_proyecto",
                "desc_actividad",
            ]
        else:
            columns_to_merge = [
                "estructura",
                "desc_programa",
                "desc_proyecto",
                "desc_actividad",
            ]
        df_dif = pd.merge(
            df_dif,
            df.loc[:, columns_to_merge].drop_duplicates(),
            on=["estructura"],
            how="left",
        )
        df_acum_2008 = df_acum_2008.loc[
            ~df_acum_2008["estructura"].isin(df_dif["estructura"].unique().tolist())
        ]
        df_acum_2008 = pd.concat([df_acum_2008, df_dif])
        df = pd.concat([df, df_acum_2008])
        df = df.drop(columns=["estructura"])


    # Ejercicio alta
    df_alta = df.groupby(group_cols).ejercicio.min().reset_index()
    df_alta.rename(columns={"ejercicio": "alta"}, inplace=True)

    df_ejercicios = df.copy()
    if ultimos_ejercicios != "All":
        ejercicios = int(ultimos_ejercicios)
        ejercicios = df_ejercicios.sort_values(
            "ejercicio", ascending=False
        ).ejercicio.unique()[0:ejercicios]
        # df_anos = df_anos.loc[df_anos.ejercicio.isin(ejercicios)]
    else:
        ejercicios = df_ejercicios.sort_values(
            "ejercicio", ascending=False
        ).ejercicio.unique()


    # Ejercicio actual
    df_ejec_actual = df.copy()
    df_ejec_actual = df_ejec_actual.loc[df_ejec_actual.ejercicio.isin(ejercicios)]
    df_ejec_actual = (
        df_ejec_actual.groupby(group_cols + ["ejercicio"]).importe.sum().reset_index()
    )
    df_ejec_actual.rename(columns={"importe": "ejecucion"}, inplace=True)

    # Ejecucion Acumulada
    df_acum = pd.DataFrame()
    for ejercicio in ejercicios:
        df_ejercicio = df.copy()
        df_ejercicio = df_ejercicio.loc[
            df_ejercicio.ejercicio.astype(int) <= int(ejercicio)
        ]
        df_ejercicio["ejercicio"] = ejercicio
        df_ejercicio = (
            df_ejercicio.groupby(group_cols + ["ejercicio"]).importe.sum().reset_index()
        )
        
        df_ejercicio.rename(columns={"importe": "acum"}, inplace=True)
        df_acum = pd.concat([df_acum, df_ejercicio])


    # Obras en curso
    df_curso = pd.DataFrame()
    for ejercicio in ejercicios:
        df_ejercicio = df.copy()
        df_ejercicio = df_ejercicio.loc[
            df_ejercicio.ejercicio.astype(int) <= int(ejercicio)
        ]
        df_ejercicio["ejercicio"] = ejercicio
        obras_curso = df_ejercicio.groupby(["desc_obra"]).avance.max().to_frame()
        obras_curso = obras_curso.loc[obras_curso.avance < 1].reset_index().desc_obra
        df_ejercicio = (
            df_ejercicio.loc[df_ejercicio.desc_obra.isin(obras_curso)]
            .groupby(group_cols + ["ejercicio"])
            .importe.sum()
            .reset_index()
        )
        df_ejercicio.rename(columns={"importe": "en_curso"}, inplace=True)
        df_curso = pd.concat([df_curso, df_ejercicio])

    # Obras terminadas anterior
    df_term_ant = pd.DataFrame()
    for ejercicio in ejercicios:
        df_ejercicio = df.copy()
        df_ejercicio = df_ejercicio.loc[
            df_ejercicio.ejercicio.astype(int) < int(ejercicio)
        ]
        df_ejercicio["ejercicio"] = ejercicio
        obras_term_ant = df_ejercicio.groupby(["desc_obra"]).avance.max().to_frame()
        obras_term_ant = (
            obras_term_ant.loc[obras_term_ant.avance == 1].reset_index().desc_obra
        )
        df_ejercicio = (
            df_ejercicio.loc[df_ejercicio.desc_obra.isin(obras_term_ant)]
            .groupby(group_cols + ["ejercicio"])
            .importe.sum()
            .reset_index()
        )
        df_ejercicio.rename(columns={"importe": "terminadas_ant"}, inplace=True)
        df_term_ant = pd.concat([df_term_ant, df_ejercicio])

    df = pd.merge(df_alta, df_acum, on=group_cols, how="left")
    df = pd.merge(df, df_ejec_actual, on=group_cols + ["ejercicio"], how="left")
    cols = df.columns.tolist()
    penultima_col = cols.pop(-2)  # Elimina la penúltima columna y la guarda
    cols.append(penultima_col)  # Agrega la penúltima columna al final
    df = df[cols]  # Reordena las columnas
    df = pd.merge(df, df_curso, on=group_cols + ["ejercicio"], how="left")
    df = pd.merge(df, df_term_ant, on=group_cols + ["ejercicio"], how="left")
    df = df.fillna(0)
    df["terminadas_actual"] = df.acum - df.en_curso - df.terminadas_ant
    df["actividad"] = df["actividad"] + "-" + df["partida"]
    df = df.rename(columns={"actividad": "estructura"})
    if not desagregar_partida:
        df = df.drop(columns=["partida"])

    return df
