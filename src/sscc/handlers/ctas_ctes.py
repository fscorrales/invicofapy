#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 30-jun-2025
Purpose: Migrate from Ctas Ctes in XLSX to MongoDB
"""

__all__ = ["CtasCtesMongoMigrator"]

import argparse
import asyncio
import inspect
import os

import pandas as pd

from ...config import logger
from ...utils import (
    RouteReturnSchema,
    read_xls,
    sync_validated_to_repository,
    validate_and_extract_data_from_df,
)
from ..repositories import CtasCtesRepository
from ..schemas import CtasCtesReport


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
        description="Migrate from Ctas Ctes in XLSX to MongoDB",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-f",
        "--file",
        metavar="excel_path",
        default=os.path.join(path, "cta_cte.xlsx"),
        type=validate_excel_file,
        help="Path al archivo Excel de Ctas Ctes",
    )

    args = parser.parse_args()

    return args


# --------------------------------------------------
class CtasCtesMongoMigrator:
    # --------------------------------------------------
    def __init__(self, excel_path: str):
        self.excel_path = excel_path

        # Repositorios por colección
        self.ctas_ctes_repo = CtasCtesRepository()

    # --------------------------------------------------
    def from_excel(self) -> pd.DataFrame:
        df = read_xls(self.excel_path, header=0)
        df = df.replace("NA", None)
        return df

    # --------------------------------------------------
    async def migrate_ctas_ctes(self):
        df = self.from_excel()
        await self.ctas_ctes_repo.delete_all()
        await self.ctas_ctes_repo.save_all(df.to_dict(orient="records"))

    # --------------------------------------------------
    async def sync_validated_excel_to_repository(self) -> RouteReturnSchema:
        """Download, process and sync the rci02 report to the repository."""
        try:
            df = self.from_excel()

            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=df,
                model=CtasCtesReport,
                field_id="map_to",
            )

            return await sync_validated_to_repository(
                repository=CtasCtesRepository(),
                validation=validate_and_errors,
                delete_filter=None,
                title="Sync Cuentas Corrientes from Excel",
                logger=logger,
                label="Sync Cuentas Corrientes from Excel",
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

    migrator = CtasCtesMongoMigrator(
        excel_path=args.file,
    )

    await migrator.migrate_ctas_ctes()


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.sscc.handlers.ctas_ctes
