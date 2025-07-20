__all__ = [
    "get_banco_invico_unified_cta_cte",
]


import pandas as pd

from ...sscc.repositories import BancoINVICORepository, CtasCtesRepository


# --------------------------------------------------
async def get_banco_invico_unified_cta_cte(
    ejercicio: int = None, filters: dict = {}
) -> pd.DataFrame:
    """
    Get the Banco INVICO data from the repository.
    """
    if ejercicio is not None:
        filters["ejercicio"] = ejercicio
    docs = await BancoINVICORepository().safe_find_by_filter(filters=filters)
    df = pd.DataFrame(docs)
    df.reset_index(drop=True, inplace=True)
    ctas_ctes = pd.DataFrame(await CtasCtesRepository().get_all())
    map_to = ctas_ctes.loc[:, ["map_to", "sscc_cta_cte"]]
    df = pd.merge(df, map_to, how="left", left_on="cta_cte", right_on="sscc_cta_cte")
    df["cta_cte"] = df["map_to"]
    df.drop(["map_to", "sscc_cta_cte"], axis="columns", inplace=True)
    return df
