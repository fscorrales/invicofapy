#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 21-feb-2026
Purpose: Migrate from Saldos SSCC in CSV to MongoDB
"""

__all__ = ["BancoINVICOSdoFinal"]

import argparse
import asyncio
import inspect
import os
from pathlib import Path

import pandas as pd

from ...config import logger
from ...utils import (
    RouteReturnSchema,
    get_df_from_sql_table,
    sync_validated_to_repository,
    validate_and_extract_data_from_df,
)
from ..repositories import BancoINVICOSdoFinalRepository
from ..schemas import BancoINVICOSdoFinalReport


# --------------------------------------------------
def read_csv_file(file_path: Path) -> pd.DataFrame:
    """Read csv file"""
    try:
        df = pd.read_csv(
            file_path,
            index_col=None,
            header=None,
            na_filter=False,
            dtype=str,
            encoding="ISO-8859-1",
        )
        df.columns = [str(x) for x in range(df.shape[1])]
        return df
    except Exception as e:
        print(f"Error al leer el archivo: {e}")
        return None


# --------------------------------------------------
def validate_csv_file(path):
    # 1. Verificar existencia
    if not os.path.exists(path):
        raise argparse.ArgumentTypeError(f"El archivo {path} no existe")

    # 2. Verificar extensión
    if not path.endswith(".csv"):
        raise argparse.ArgumentTypeError(
            f"El archivo {path} no parece ser un archivo CSV"
        )

    # 3. Intentar lectura mínima
    try:
        # Probamos leer la primera fila.
        # Nota: Si tus CSV usan ';' puedes agregar sep=None, engine='python'
        pd.read_csv(
            path,
            nrows=1,
            encoding="ISO-8859-1",
        )
    except Exception as e:
        raise argparse.ArgumentTypeError(f"Error al abrir el archivo CSV {path}: {e}")

    return path


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

    parser = argparse.ArgumentParser(
        description="Migrate from Saldos SSCC in CSV to MongoDB",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-f",
        "--file",
        metavar="csv_path",
        default=os.path.join(path, "saldos_sscc.csv"),
        type=validate_csv_file,
        help="Path al archivo CSV de Saldos SSCC",
    )

    args = parser.parse_args()

    return args


# --------------------------------------------------
class BancoINVICOSdoFinal:
    # --------------------------------------------------
    def __init__(self):
        self.clean_df = pd.DataFrame()

        # Repositorios por colección
        # self.rdeu012b2_cuit_repo = Rdeu012b2CuitRepository()

    # --------------------------------------------------
    def from_csv(self, csv_path: str = None) -> pd.DataFrame:
        df = read_csv_file(csv_path)
        df = df.loc[:, ["5", "11", "12", "13", "14"]]
        df.columns = ["ejercicio", "cta_cte", "desc_cta_cte", "desc_banco", "saldo"]
        df["ejercicio"] = df["ejercicio"].str[-4:]
        df["ejercicio"] = pd.to_numeric(df["ejercicio"], errors="coerce")
        df["saldo"] = df["saldo"].str.replace(".", "", regex=False)
        df["saldo"] = df["saldo"].str.replace(",", ".", regex=False)
        df["saldo"] = df["saldo"].astype(float)
        self.clean_df = df
        return self.clean_df

    # --------------------------------------------------
    # async def migrate_deuda_flotante(self):
    #     df = self.from_pdf()
    #     await self.rdeu012b2_cuit_repo.delete_all()
    #     await self.rdeu012b2_cuit_repo.save_all(df.to_dict(orient="records"))

    # --------------------------------------------------
    async def sync_validated_sqlite_to_repository(
        self, sqlite_path: str
    ) -> RouteReturnSchema:
        """Download, process and sync the deuda flotante report to the repository."""
        try:
            df = get_df_from_sql_table(sqlite_path, table="sdo_final_banco_invico")
            df.drop(columns=["id"], inplace=True)
            df["ejercicio"] = pd.to_numeric(df["ejercicio"], errors="coerce")
            df = df.loc[df["ejercicio"] < 2025]

            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=df,
                model=BancoINVICOSdoFinalReport,
                field_id="cta_cte",
            )

            return await sync_validated_to_repository(
                repository=BancoINVICOSdoFinalRepository(),
                validation=validate_and_errors,
                delete_filter={"ejercicio": {"$lt": 2025}},
                title="Sync SIIF BancoINVICOSdoFinal Report from SQLite",
                logger=logger,
                label="Sync SIIF BancoINVICOSdoFinal Report from SQLite",
            )
        except Exception as e:
            print(f"Error migrar y sincronizar el reporte: {e}")

    # --------------------------------------------------
    async def sync_validated_csv_to_repository(
        self, csv_path: str
    ) -> RouteReturnSchema:
        """Download, process and sync the planillometro report to the repository."""
        try:
            df = self.from_csv(csv_path)

            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=df,
                model=BancoINVICOSdoFinalReport,
                field_id="cta_cte",
            )

            ejercicio = df["ejercicio"].iloc[0] if not df.empty else None

            return await sync_validated_to_repository(
                repository=BancoINVICOSdoFinalRepository(),
                validation=validate_and_errors,
                delete_filter={"ejercicio": int(ejercicio)},
                title="Sync SIIF BancoINVICOSdoFinal Report from CSV",
                logger=logger,
                label="Sync SIIF BancoINVICOSdoFinal Report from CSV",
            )
        except Exception as e:
            print(f"Error migrar y sincronizar el reporte: {e}")


# --------------------------------------------------
async def main():
    # """Make a jazz noise here"""
    # from ...config import Database

    # Database.initialize()
    # try:
    #     await Database.client.admin.command("ping")
    #     print("Connected to MongoDB")
    # except Exception as e:
    #     print("Error connecting to MongoDB:", e)
    #     return

    # args = get_args()
    # try:
    #     migrator = Rdeu012b2Cuit(
    #         pdf_path=args.file,
    #     )

    #     await migrator.migrate_deuda_flotante()
    # except Exception as e:
    #     print(f"Error during migration: {e}")

    """Make a jazz noise here"""

    args = get_args()

    try:
        banco_invico_sdo_final = BancoINVICOSdoFinal()
        banco_invico_sdo_final.from_csv(csv_path=args.file)
        print(banco_invico_sdo_final.clean_df)
    except Exception as e:
        print(f"Error al iniciar sesión: {e}")


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.sscc.handlers.banco_invico_sdo_final
