#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Date   :06-jul-2025
Purpose: Join gto_rpa03g (gtos_gpo_part) with rcg01_uejp (gtos)
"""

__all__ = ["JoinComprobantesGtosGpoPart"]

import argparse
import asyncio

import pandas as pd

from ..repositories import Rcg01UejpRepository, Rpa03gRepository

# from .detalle_partidas_rog01 import DetallePartidasRog01


class JoinComprobantesGtosGpoPart:
    """Join gto_rpa03g (gtos_gpo_part) with rcg01_uejp (gtos)"""

    df: pd.DataFrame = None

    # --------------------------------------------------
    # def from_external_report(
    #     self, gtos_gpo_part_xls_path: str, gtos_xls_path: str, part_xlx_path: str
    # ) -> pd.DataFrame:
    #     self.df_gtos_gpo_part = ComprobantesGtosGpoPartGtoRpa03g().from_external_report(
    #         gtos_gpo_part_xls_path
    #     )
    #     self.df_gtos = ComprobantesGtosRcg01Uejp().from_external_report(gtos_xls_path)
    #     self.df_part = DetallePartidasRog01().from_external_report(part_xlx_path)
    #     self.join_df()
    #     return self.df

    # --------------------------------------------------
    async def from_mongo(self) -> pd.DataFrame:
        self.df_gtos_gpo_part = pd.DataFrame(Rpa03gRepository().get_all())
        self.df_gtos = pd.DataFrame(Rcg01UejpRepository().get_all())
        # self.df_part = DetallePartidasRog01().from_sql(sql_path)
        self.join_df()
        return self.df

    # --------------------------------------------------
    def join_df(self) -> pd.DataFrame:
        df_gtos_filtered = self.df_gtos[
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
        self.df = pd.merge(
            left=self.df_gtos_gpo_part,
            right=df_gtos_filtered,
            on=["nro_comprobante"],
            how="left",
        )
        # self.df.drop(["grupo"], axis=1, inplace=True)
        # self.df = pd.merge(left=self.df, right=self.df_part, on=["partida"], how="left")
        return self.df


# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description="Join gto_rpa03g (gtos_gpo_part) with rcg01_uejp (gtos)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # parser.add_argument(
    #     "-f",
    #     "--sql_file",
    #     metavar="sql_file",
    #     default="siif.sqlite",
    #     type=str,
    #     help="SIIF' sqlite DataBase file name. Must be in the same folder",
    # )

    return parser.parse_args()


# --------------------------------------------------
async def main():
    """Let's try it"""
    from ...config import Database

    Database.initialize()
    try:
        await Database.client.admin.command("ping")
        print("Connected to MongoDB")
    except Exception as e:
        print("Error connecting to MongoDB:", e)
        return

    args = get_args()

    siif_join_comprobantes_gtos = JoinComprobantesGtosGpoPart()
    siif_join_comprobantes_gtos.from_mongo()
    print(siif_join_comprobantes_gtos.df)


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.siif.handlers.join_comprobantes_gtos_gpo_part
