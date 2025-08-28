#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 28-aug-2025
Purpose: Migrate from Planillometro Historico (Patricia) in XLSX to MongoDB
"""

__all__ = ["PlanillometroHistMongoMigrator"]

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
from ..repositories import PlanillometroHistRepository
from ..schemas import PlanillometroHistReport


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
        description="Migrate from Planillometro Historico in XLSX to MongoDB",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-f",
        "--file",
        metavar="excel_path",
        default=os.path.join(path, "planillometro_hist.xlsx"),
        type=validate_excel_file,
        help="Path al archivo Excel de Ctas Ctes",
    )

    args = parser.parse_args()

    return args


# --------------------------------------------------
class PlanillometroHistMongoMigrator:
    # --------------------------------------------------
    def __init__(self, excel_path: str):
        self.excel_path = excel_path

        # Repositorios por colección
        self.planillometro_repo = PlanillometroHistRepository()

    # --------------------------------------------------
    def from_excel(self) -> pd.DataFrame:
        df = read_xls(self.excel_path, header=0)
        df = df.replace("", None)
        df["desc_programa"] = np.where(
            df["proy"].isna(), df["prog"] + " - " + df["Descripción"], np.nan
        )
        df["desc_programa"] = df["desc_programa"].ffill()
        df["desc_subprograma"] = df["subprog"] + " - --"
        df["desc_proyecto"] = np.where(
            df["obra"].isna(), df["proy"] + " - " + df["Descripción"], np.nan
        )
        df["desc_proyecto"] = df["desc_proyecto"].ffill()
        df["desc_actividad"] = np.where(
            ~df["estructura"].isna(), df["obra"] + " - " + df["Descripción"], np.nan
        )
        df = df.dropna(subset=["estructura"])
        df["acum_2008"] = df["acum_2008"].astype(float)
        df["alta"] = pd.to_numeric(df["alta"], errors="coerce")
        df = df.loc[
            :,
            [
                "desc_programa",
                "desc_subprograma",
                "desc_proyecto",
                "desc_actividad",
                "actividad",
                "partida",
                "estructura",
                "alta",
                "acum_2008",
            ],
        ]
        return df

    # --------------------------------------------------
    async def migrate_planillometro(self):
        df = self.from_excel()
        await self.planillometro_repo.delete_all()
        await self.planillometro_repo.save_all(df.to_dict(orient="records"))

    # --------------------------------------------------
    async def sync_validated_excel_to_repository(self) -> RouteReturnSchema:
        """Download, process and sync the planillometro report to the repository."""
        try:
            df = self.from_excel()

            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=df,
                model=PlanillometroHistReport,
                field_id="estructura",
            )

            return await sync_validated_to_repository(
                repository=PlanillometroHistRepository(),
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
    try:
        migrator = PlanillometroHistMongoMigrator(
            excel_path=args.file,
        )

        await migrator.migrate_planillometro()
    except Exception as e:
        print(f"Error during migration: {e}")


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.siif.handlers.planillometro_hist
