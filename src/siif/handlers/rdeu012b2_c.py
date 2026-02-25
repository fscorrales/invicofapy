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
import pdfplumber
import pandas as pd
import re

PDF_PATH = "202512-rdeu012b2_Cuit.pdf"  # Cambiar por la ruta real del archivo

# --------------------------------------------------
def parsear_fila(texto):
    """
    Script para leer el PDF de deuda flotante (rdeu012b2_Cuit) y cargarlo en un DataFrame.
    Requiere: pip install pdfplumber pandas
    Parsea una fila del PDF con el formato:
    NroEntrada NroOrigen CodFte Org_Fin Monto SaldoAPagar NroExpediente CtaCte Descripcion

    Ejemplo:
    '393 393 13 0 5.951.535,09 1.965.478,36 900011962016 130868045 P/TRANSFERENCIAS...'
    """
    # Numeros con formato argentino: punto de miles, coma decimal. Ej: 5.951.535,09
    patron_monto = r'\d{1,3}(?:\.\d{3})+,\d+'

    # Numero de expediente y cuenta corriente (9+ digitos consecutivos sin puntos)
    patron_expediente = r'\b\d{9,}\b'

    montos = re.findall(patron_monto, texto)
    expedientes = re.findall(patron_expediente, texto)

    partes = texto.split()
    nro_entrada = partes[0] if len(partes) > 0 else ""
    nro_origen  = partes[1] if len(partes) > 1 else ""
    cod_fte     = partes[2] if len(partes) > 2 else ""
    org_fin     = partes[3] if len(partes) > 3 else ""

    monto          = montos[0] if len(montos) > 0 else ""
    saldo_a_pagar  = montos[1] if len(montos) > 1 else ""
    nro_expediente = expedientes[0] if len(expedientes) > 0 else ""
    cta_cte        = expedientes[1] if len(expedientes) > 1 else ""

    desc_match = re.search(r'P/TRANSFER.*', texto, re.IGNORECASE)
    descripcion = desc_match.group(0).strip() if desc_match else ""

    return {
        "NroEntrada":    nro_entrada,
        "NroOrigen":     nro_origen,
        "CodFte":        cod_fte,
        "Org_Fin":       org_fin,
        "Monto":         monto,
        "SaldoAPagar":   saldo_a_pagar,
        "NroExpediente": nro_expediente,
        "CtaCte":        cta_cte,
        "Descripcion":   descripcion,
        "TextoCompleto": texto,
    }

# --------------------------------------------------
def extraer_datos_pdf(pdf_path):
    filas = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    if not row or len(row) == 0:
                        continue
                    texto = row[0].strip() if row[0] else ""

                    # Ignorar encabezados, totales y metadatos
                    if (not texto
                            or texto.startswith("Nro")
                            or texto.startswith("Total")
                            or texto.startswith("Beneficiario")
                            or texto.startswith("Entidad")
                            or texto.startswith("DESDE")):
                        continue

                    # Solo procesar filas que empiezan con un numero (registros reales)
                    if re.match(r'^\d+', texto):
                        filas.append(parsear_fila(texto))

    df = pd.DataFrame(filas)
    return df

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

# if __name__ == "__main__":
#     df = extraer_datos_pdf(PDF_PATH)

#     print(f"Filas extraidas: {len(df)}")
#     print(f"Columnas: {list(df.columns)}")
#     print("\nMuestra de datos (columnas principales):")
#     print(df[["NroEntrada", "CodFte", "Monto", "SaldoAPagar", "NroExpediente", "CtaCte"]].to_string())

#     # Guardar a CSV
#     output_csv = "deuda_flotante.csv"
#     df.to_csv(output_csv, index=False, encoding="utf-8-sig")
#     print(f"\nDatos guardados en: {output_csv}")