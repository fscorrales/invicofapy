__all__ = [
    "get_resumen_rend_prov_unified_cta_cte",
    "get_resumen_rend_prov_with_desc",
]


import pandas as pd
from fastapi import HTTPException

from ...config import logger
from ...icaro.repositories import ProveedoresRepository
from ...sgf.repositories import ResumenRendProvRepository
from ...sscc.repositories import CtasCtesRepository


# --------------------------------------------------
async def get_resumen_rend_prov_unified_cta_cte(
    ejercicio: int = None, filters: dict = {}
) -> pd.DataFrame:
    """
    Get the Resumen Rendición por Proveedor data from the repository.
    """
    try:
        if ejercicio is not None:
            filters["ejercicio"] = ejercicio
        docs = await ResumenRendProvRepository().safe_find_by_filter(filters=filters)
        df = pd.DataFrame(docs)
        df.reset_index(drop=True, inplace=True)
        ctas_ctes = pd.DataFrame(await CtasCtesRepository().get_all())
        map_to = ctas_ctes.loc[:, ["map_to", "sgf_cta_cte"]]
        df = pd.merge(df, map_to, how="left", left_on="cta_cte", right_on="sgf_cta_cte")
        df["cta_cte"] = df["map_to"]
        df.drop(["map_to", "sgf_cta_cte"], axis="columns", inplace=True)

        # Filtramos los registros duplicados en la 106
        df_106 = df.copy()
        df_106 = df_106.loc[df_106["cta_cte"] == "106"]
        df_106 = df_106.drop_duplicates(
            subset=["mes", "fecha", "beneficiario", "libramiento_sgf", "importe_bruto"]
        )
        df = pd.concat([df[df["cta_cte"] != "106"], df_106], ignore_index=True)

        # Filtramos los registros duplicados en la 07
        df_07 = df.copy()
        df_07 = df_07.loc[df_07["cta_cte"] == "130832-07"]
        df_07 = df_07.sort_values(["libramiento_sgf", "destino"], ascending=False)
        df_07 = df_07.drop_duplicates(
            subset=[
                "mes", "fecha", "beneficiario", "libramiento_sgf", "importe_bruto", 
                "gcias", "sellos", "iibb", "suss", "invico", "seguro", "salud", 
                "mutual", "otras", "retenciones", "importe_neto"
            ]
        )
        df = pd.concat([df[df["cta_cte"] != "130832-07"], df_07], ignore_index=True)

        # Filtramos los registros duplicados en la 03
        df_03 = df.copy()
        df_03 = df_03.loc[df_03["cta_cte"] == "130832-03"]
        df_03 = df_03.sort_values(["libramiento_sgf", "destino"], ascending=False)
        df_03 = df_03.drop_duplicates(
            subset=[
                "mes", "fecha", "beneficiario", "libramiento_sgf", "importe_bruto", 
                "gcias", "sellos", "iibb", "suss", "invico", "seguro", "salud", 
                "mutual", "otras", "retenciones", "importe_neto"
            ]
        )
        df = pd.concat([df[df["cta_cte"] != "130832-03"], df_03], ignore_index=True)

        # Filtramos los registros duplicados en la 221078150
        df_2210178150 = df.copy()
        df_2210178150 = df_2210178150.loc[df_2210178150["cta_cte"] == "2210178150"]
        df_2210178150 = df_2210178150.drop_duplicates(
            subset=["mes", "fecha", "beneficiario", "libramiento_sgf", "importe_bruto"]
        )
        # df = df[df["cta_cte"] != "2210178150"]
        df = pd.concat(
            [df[df["cta_cte"] != "2210178150"], df_2210178150], ignore_index=True
        )
        return df
    except Exception as e:
        logger.error(
            f"Error retrieving Resumen Rend Prov with unified Cta Cte Data from database: {e}"
        )
        raise HTTPException(
            status_code=500,
            detail="Error Resumen Rend Prov Data with unified Cta Cte from the database",
        )


# --------------------------------------------------
async def get_resumen_rend_prov_with_desc(
    ejercicio: int = None, filters: dict = {}
) -> pd.DataFrame:
    try:
        sgf = await get_resumen_rend_prov_unified_cta_cte(
            ejercicio=ejercicio, filters=filters
        )
        prov = pd.DataFrame(await ProveedoresRepository().get_all())
        prov = prov.loc[:, ["cuit", "desc_proveedor"]]
        df = pd.merge(
            left=sgf,
            right=prov,
            left_on="beneficiario",
            right_on="desc_proveedor",
            how="left",
        )
        df.drop(["desc_proveedor"], axis="columns", inplace=True)
        return df
    except Exception as e:
        logger.error(
            f"Error retrieving Resumen Rend Prov with Desc Proveedor Data from database: {e}"
        )
    raise HTTPException(
        status_code=500,
        detail="Error Resumen Rend Prov with Desc Proveedor Data from the database",
    )
