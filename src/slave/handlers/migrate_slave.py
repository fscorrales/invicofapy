#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 17-oct-2025
Purpose: Migrate from old Slave.mdb to new DB
"""

__all__ = ["SlaveMongoMigrator"]

import argparse
import asyncio
import inspect
import os
import subprocess
import sys
from io import StringIO
from typing import List

import pandas as pd
import pyodbc

from ...config import logger
from ...utils import (
    RouteReturnSchema,
    sync_validated_to_repository,
    validate_and_extract_data_from_df,
)
from ..repositories import (
    FacturerosRepository,
)
from ..schemas import FacturerosReport


def validate_mdb_file(path):
    if not os.path.exists(path):
        raise argparse.ArgumentTypeError(f"El archivo {path} no existe")
    if not path.endswith(".mdb"):
        raise argparse.ArgumentTypeError(
            f"El archivo {path} no parece ser un archivo Access (.mdb)"
        )
    # try:
    #     sqlite3.connect(path)
    # except sqlite3.Error as e:
    #     raise argparse.ArgumentTypeError(
    #         f"Error al conectar al archivo SQLite {path}: {e}"
    #     )
    return path


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

    parser = argparse.ArgumentParser(
        description="Migrate from old Slave.mdb to new DB",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-f",
        "--file",
        metavar="mdb_file",
        default=os.path.join(path, "Slave.mdb"),
        type=validate_mdb_file,
        help="Path al archivo Access de Slave.mdb",
    )

    args = parser.parse_args()

    return args


# --------------------------------------------------
class SlaveMongoMigrator:
    # --------------------------------------------------
    def __init__(self, mdb_path: str):
        self.mdb_path = mdb_path

        # Repositorios por colección
        self.factureros_repo = FacturerosRepository()

    # --------------------------------------------------
    def from_mdb(self, table_name: str) -> pd.DataFrame:
        """
        Lee una tabla desde un archivo Access (.mdb o .accdb) y la convierte a DataFrame.
        Requiere: pyodbc
        """
        conn_str = (
            r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
            f"DBQ={self.mdb_path};"
        )
        # with pyodbc.connect(conn_str) as conn:
        #     query = f"SELECT * FROM [{table_name}]"
        #     df = pd.read_sql(query, conn)
        # return df
        try:
            with pyodbc.connect(conn_str) as conn:
                return pd.read_sql(f"SELECT * FROM [{table_name}]", conn)
        except pyodbc.Error as e:
            # Si el error indica versión vieja, intentar con mdbtools
            if "Cannot open a database created with a previous version" in str(e):
                print(
                    "⚠️ Versión antigua de Access detectada. Intentando leer con mdbtools..."
                )
                cmd = ["mdb-export", self.mdb_path, table_name]
                output = subprocess.check_output(cmd, text=True)
                return pd.read_csv(StringIO(output))
            else:
                raise

    # --------------------------------------------------
    async def migrate_factureros(self) -> pd.DataFrame:
        """ "Migrate table PRECARIZADOS"""
        if sys.platform.startswith("win32"):
            df = self.from_mdb("PRECARIZADOS")
            df.rename(
                columns={
                    "Agentes": "razon_social",
                    "Actividad": "actividad",
                    "Partida": "partida",
                },
                inplace=True,
            )
        df.drop_duplicates(inplace=True)
        df["actividad"] = df["actividad"].str[0:3] + "00-" + df["actividad"].str[3:]

        # Validar datos usando Pydantic
        validate_and_errors = validate_and_extract_data_from_df(
            dataframe=df, model=FacturerosReport, field_id="razon_social"
        )
        # await self.programas_repo.delete_all()
        # await self.programas_repo.save_all(df.to_dict(orient="records"))
        return await sync_validated_to_repository(
            repository=self.factureros_repo,
            validation=validate_and_errors,
            delete_filter=None,
            title="SLAVE Precarizados Migration",
            logger=logger,
            label="Tabla Precarizados de Slave",
        )

    # --------------------------------------------------
    async def migrate_all(self) -> List[RouteReturnSchema]:
        return_schema = []
        return_schema.append(await self.migrate_factureros())
        # return_schema.append(await self.migrate_honorarios_factureros())
        return return_schema


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

    migrator = SlaveMongoMigrator(
        mdb_path=args.file,
    )

    await migrator.migrate_factureros()


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.slave.handlers.migrate_slave
