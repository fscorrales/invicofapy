__all__ = [
    "get_resumen_rend_prov_unified_cta_cte",
    "get_resumen_rend_prov_with_desc",
]


import pandas as pd

from ...sgf.repositories import ResumenRendProvRepository
from ...sscc.repositories import CtasCtesRepository


# --------------------------------------------------
async def get_resumen_rend_prov_unified_cta_cte(
    ejercicio: int = None, filters: dict = {}
) -> pd.DataFrame:
    """
    Get the Resumen Rendición por Proveedor data from the repository.
    """
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

    # Filtramos los registros duplicados en la 221078150
    df_2210178150 = df.copy()
    df_2210178150 = df_2210178150.loc[df_2210178150["cta_cte"] == "2210178150"]
    df_2210178150 = df_2210178150.drop_duplicates(
        subset=["mes", "fecha", "beneficiario", "libramiento_sgf", "importe_bruto"]
    )
    df = df[df["cta_cte"] != "2210178150"]
    df = pd.concat(
        [df[df["cta_cte"] != "2210178150"], df_2210178150], ignore_index=True
    )
    return df


# --------------------------------------------------
async def get_resumen_rend_prov_with_desc(
    ejercicio: int = None, filters: dict = {}
) -> pd.DataFrame:
    df = await get_resumen_rend_prov_unified_cta_cte(
        ejercicio=ejercicio, filters=filters
    )
    return df
