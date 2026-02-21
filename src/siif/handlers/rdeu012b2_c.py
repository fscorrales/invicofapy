#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 21-feb-2026
Purpose: Migrate from Deuda Flotante (TPF) in CSV to MongoDB
"""

__all__ = ["Rdeu012b2CMongoMigrator"]

import argparse
import asyncio
import inspect
import os

import numpy as np
import pandas as pd

from ...config import logger
from ...utils import (
    RouteReturnSchema,
    read_xls,
    sync_validated_to_repository,
    validate_and_extract_data_from_df,
)
from ..repositories import Rdeu012b2CRepository
from ..schemas import Rdeu012b2CReport


def validate_excel_file(path):
    if not os.path.exists(path):
        raise argparse.ArgumentTypeError(f"El archivo {path} no existe")
    if not path.endswith(".xlsx") and not path.endswith(".xls"):
        raise argparse.ArgumentTypeError(
            f"El archivo {path} no parece ser un archivo Excel"
        )
    try:
        pd.read_excel(path, nrows=1)  # Solo intenta leer la primera fila
    except Exception as e:
        raise argparse.ArgumentTypeError(f"Error al abrir el archivo Excel {path}: {e}")
    return path


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

    parser = argparse.ArgumentParser(
        description="Migrate from Deuda Flotante (TPF) in XLSX to MongoDB",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-f",
        "--file",
        metavar="excel_path",
        default=os.path.join(path, "rdeu012b2_c.xlsx"),
        type=validate_excel_file,
        help="Path al archivo Excel de Deuda Flotante (TPF)",
    )

    args = parser.parse_args()

    return args


# --------------------------------------------------
class Rdeu012b2CMongoMigrator:
    # --------------------------------------------------
    def __init__(self, excel_path: str):
        self.excel_path = excel_path

        # Repositorios por colección
        self.rdeu012b2_c_repo = Rdeu012b2CRepository()

    # --------------------------------------------------
    def from_excel(self) -> pd.DataFrame:
        df = read_xls(self.excel_path, header=0)
        df["fecha_desde"] = df["0"].iloc[6][6:16]
        df["fecha_desde"] = pd.to_datetime(df["fecha_desde"], format="%d/%m/%Y")
        df["fecha_hasta"] = df["0"].iloc[6][-10:]
        df["fecha_hasta"] = pd.to_datetime(df["fecha_hasta"], format="%d/%m/%Y")
        df["ejercicio"] = df["fecha_hasta"].dt.year.astype(str)
        df["mes_hasta"] = df["fecha_hasta"].dt.strftime("%m/%Y")
        df["entidad"] = df.loc[df["0"] == "Entidad"]["3"]
        df["entidad"].fillna(method="ffill", inplace=True)
        df = df.iloc[9:]
        df = df.loc[df["1"] != ""]
        df["ejercicio_deuda"] = df.loc[df["0"] == ""]["1"].str[-4:]
        df["ejercicio_deuda"].fillna(method="bfill", inplace=True)
        df = df.loc[df["7"] != ""]
        df = df.loc[df["0"] != "Entrada"]
        df.rename(
            columns={
                "0": "nro_entrada",
                "1": "nro_origen",
                "2": "fuente",
                "3": "org_fin",
                "4": "importe",
                "5": "saldo",
                "6": "nro_expte",
                "7": "cta_cte",
                "8": "glosa",
            },
            inplace=True,
        )
        df["importe"] = df["importe"].str.replace(".", "", regex=False)
        df["importe"] = df["importe"].str.replace(",", ".", regex=False)
        df["importe"] = df["importe"].astype(float)
        df["saldo"] = df["saldo"].str.replace(".", "", regex=False)
        df["saldo"] = df["saldo"].str.replace(",", ".", regex=False)
        df["saldo"] = df["saldo"].astype(float)
        first_cols = [
            "ejercicio",
            "mes_hasta",
            "entidad",
            "ejercicio_deuda",
            "fuente",
            "nro_entrada",
            "nro_origen",
            "importe",
            "saldo",
        ]
        df = df.loc[:, first_cols].join(df.drop(columns=first_cols, axis=1))
        # df = df >>\
        #     dplyr.select(
        #         f.ejercicio, f.mes_hasta, f.entidad, f.ejercicio_deuda,
        #         f.fuente, f.nro_entrada, f.nro_origen, f.importe, f.saldo,
        #         dplyr.everything()
        #     )
        return df

    # --------------------------------------------------
    async def migrate_deuda_flotante(self):
        df = self.from_excel()
        await self.rdeu012b2_c_repo.delete_all()
        await self.rdeu012b2_c_repo.save_all(df.to_dict(orient="records"))

    # --------------------------------------------------
    async def sync_validated_excel_to_repository(self) -> RouteReturnSchema:
        """Download, process and sync the planillometro report to the repository."""
        try:
            df = self.from_excel()

            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=df,
                model=Rdeu012b2CReport,
                field_id="estructura",
            )

            return await sync_validated_to_repository(
                repository=Rdeu012b2CRepository(),
                validation=validate_and_errors,
                delete_filter=None,
                title="Sync Deuda Flotante (TPF) from Excel",
                logger=logger,
                label="Sync Deuda Flotante (TPF) from Excel",
            )
        except Exception as e:
            print(f"Error migrar y sincronizar el reporte: {e}")


# --------------------------------------------------
async def main():
    """Make a jazz noise here"""
    from ...config import Database

    Database.initialize()
    try:
        await Database.client.admin.command("ping")
        print("Connected to MongoDB")
    except Exception as e:
        print("Error connecting to MongoDB:", e)
        return

    args = get_args()
    try:
        migrator = Rdeu012b2CMongoMigrator(
            excel_path=args.file,
        )

        await migrator.migrate_deuda_flotante()
    except Exception as e:
        print(f"Error during migration: {e}")


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.siif.handlers.rdeu012b2_c
