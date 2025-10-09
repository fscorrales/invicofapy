__all__ = ["get_banco_invico_unified_cta_cte", "get_banco_invico_cert_neg"]


import pandas as pd

from ...sscc.repositories import BancoINVICORepository, CtasCtesRepository

from ...config import logger


# --------------------------------------------------
async def get_banco_invico_unified_cta_cte(
    ejercicio: int = None, filters: dict = {}
) -> pd.DataFrame:
    """
    Get the Banco INVICO data from the repository.
    """
    if ejercicio is not None:
        filters.update({"ejercicio": ejercicio})
    docs = await BancoINVICORepository().safe_find_by_filter(filters=filters)
    # logger.info(f"len(docs): {len(docs)}")
    df = pd.DataFrame(docs)
    df.reset_index(drop=True, inplace=True)
    if not df.empty:
        # logger.info(f"df.shape: {df.shape} - df.head: {df.head()}")
        ctas_ctes = pd.DataFrame(await CtasCtesRepository().get_all())
        map_to = ctas_ctes.loc[:, ["map_to", "sscc_cta_cte"]]
        df = pd.merge(df, map_to, how="left", left_on="cta_cte", right_on="sscc_cta_cte")
        df["cta_cte"] = df["map_to"]
        df.drop(["map_to", "sscc_cta_cte"], axis="columns", inplace=True)
        # logger.info(f"df.shape: {df.shape} - df.head: {df.head()}")
    return df


# --------------------------------------------------
async def get_banco_invico_cert_neg(
    ejercicio: int = None, filters: dict = {}
) -> pd.DataFrame:
    filters["cod_imputacion"] = "018"
    filters["es_cheque"] = False
    filters["movimiento"] = "DEPOSITO"
    df = await get_banco_invico_unified_cta_cte(ejercicio=ejercicio, filters=filters)
    if not df.empty:
        df["origen"] = "BANCO"
        df["cuit"] = "30632351514"
        df["beneficiario"] = df["concepto"]
        df["destino"] = df["imputacion"]
        df["importe_bruto"] = df["importe"] * (-1)
        df["importe_neto"] = df["importe_bruto"]
        df = df.loc[
            :,
            [
                "ejercicio",
                "mes",
                "fecha",
                "cta_cte",
                "origen",
                "cuit",
                "beneficiario",
                "movimiento",
                "destino",
                "importe_bruto",
                "importe_neto",
            ],
        ]
    return df
