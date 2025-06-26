#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 20-jun-2025
Purpose: Migrate from old Icaro.sqlite to new DB
"""

__all__ = ["MigrateIcaro"]

import argparse
import asyncio
import datetime as dt
import inspect
import os
import sqlite3
from dataclasses import dataclass
from motor.motor_asyncio import AsyncIOMotorClient

import pandas as pd

from ..repositories import ProgramasRepository, SubprogramasRepository, ProyectosRepository, ActividadesRepository, EstructurasRepository


def validate_sqlite_file(path):
    if not os.path.exists(path):
        raise argparse.ArgumentTypeError(f"El archivo {path} no existe")
    if not path.endswith(".sqlite") and not path.endswith(".db"):
        raise argparse.ArgumentTypeError(f"El archivo {path} no parece ser un archivo SQLite")
    try:
        sqlite3.connect(path)
    except sqlite3.Error as e:
        raise argparse.ArgumentTypeError(f"Error al conectar al archivo SQLite {path}: {e}")
    return path


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    path = os.path.dirname(
        os.path.abspath(inspect.getfile(inspect.currentframe()))
    )

    parser = argparse.ArgumentParser(
        description="Migrate from old Icaro.sqlite to new DB",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-f",
        "--file",
        metavar="sqlite_file",
        default= os.path.join(path, "ICARO.sqlite"),
        type= validate_sqlite_file,
        help= "Path al archivo SQLite de Icaro",
    )

    args = parser.parse_args()

    return args


# --------------------------------------------------
class IcaroMongoMigrator:
    def __init__(self, sqlite_path: str):
        self.sqlite_path = sqlite_path

        # Repositorios por colección
        self.programas_repo = ProgramasRepository()
        self.subprogramas_repo = SubprogramasRepository()
        self.proyectos_repo = ProyectosRepository()
        self.actividades_repo = ActividadesRepository()
        self.estructuras_repo = EstructurasRepository()
        # Agregás más repos aquí

    def from_sql(self, table: str) -> pd.DataFrame:
        import sqlite3

        with sqlite3.connect(self.sqlite_path) as conn:
            return pd.read_sql_query(f"SELECT * FROM {table}", conn)

    async def migrate_programas(self):
        df = self.from_sql("PROGRAMAS")
        df.rename(
            columns={"Programa": "nro_prog", "DescProg": "desc_prog"}, inplace=True
        )
        await self.programas_repo.delete_all()
        await self.programas_repo.save_all(df.to_dict(orient="records"))

    async def migrate_subprogramas(self):
        df = self.from_sql("SUBPROGRAMAS")
        df.rename(
            columns={
                "Programa": "nro_prog",
                "Subprograma": "nro_subprog",
                "DescSubprog": "desc_subprog",
            },
            inplace=True,
        )
        await self.subprogramas_repo.delete_all()
        await self.subprogramas_repo.save_all(df.to_dict(orient="records"))

    async def migrate_proyectos(self):
        df = self.from_sql("PROYECTOS")
        df.rename(
            columns={
                "Subprograma": "nro_subprog",
                "Proyecto": "nro_proy",
                "DescProy": "desc_proy",
            },
            inplace=True,
        )
        await self.proyectos_repo.delete_all()
        await self.proyectos_repo.save_all(df.to_dict(orient="records"))

    async def migrate_actividades(self):
        df = self.from_sql("ACTIVIDADES")
        df.rename(
            columns={
                "Proyecto": "nro_proy",
                "Actividad": "nro_act",
                "DescAct": "desc_act",
            },
            inplace=True,
        )
        await self.actividades_repo.delete_all()
        await self.actividades_repo.save_all(df.to_dict(orient="records"))

    async def migrate_estructuras(self):
        await self.estructuras_repo.delete_all()
        # Programas
        df = self.from_sql("PROGRAMAS")
        df.rename(
            columns={"Programa": "nro_estructura", "DescProg": "desc_estructura"}, inplace=True
        )
        await self.estructuras_repo.save_all(df.to_dict(orient="records"))

        # Subprogramas
        df = self.from_sql("SUBPROGRAMAS")
        df.rename(
            columns={
                "Subprograma": "nro_estructura",
                "DescSubprog": "desc_estructura",
            },
            inplace=True,
        )
        df.drop(["Programa"], axis=1, inplace=True)
        await self.estructuras_repo.save_all(df.to_dict(orient="records"))

        # Proyectos
        df = self.from_sql("PROYECTOS")
        df.rename(
            columns={
                "Proyecto": "nro_estructura",
                "DescProy": "desc_estructura",
            },
            inplace=True,
        )
        df.drop(["Subprograma"], axis=1, inplace=True)
        await self.estructuras_repo.save_all(df.to_dict(orient="records"))

        # Actividades
        df = self.from_sql("ACTIVIDADES")
        df.rename(
            columns={
                "Actividad": "nro_estructura",
                "DescAct": "desc_estructura",
            },
            inplace=True,
        )
        df.drop(["Proyecto"], axis=1, inplace=True)
        await self.estructuras_repo.save_all(df.to_dict(orient="records"))

    async def migrate_all(self):
        await self.migrate_programas()
        await self.migrate_subprogramas()
        await self.migrate_proyectos()
        await self.migrate_actividades()
        await self.migrate_estructuras()
        # ... el resto de las tablas


# --------------------------------------------------
@dataclass
class MigrateIcaro:
    """Migrate from old Icaro.sqlite to new DB"""

    path_old_icaro: str = "ICARO.sqlite"
    path_new_icaro: str = "icaro_new.sqlite"
    # _SQL_MODEL = IcaroModel
    _INDEX_COL = None

    # --------------------------------------------------
    def migrate_all(self):
        self.migrate_programas()
        self.migrate_subprogramas()
        self.migrate_proyectos()
        self.migrate_actividades()
        self.migrate_ctas_ctes()
        self.migrate_fuentes()
        self.migrate_partidas()
        self.migrate_proveedores()
        self.migrate_obras()
        self.migrate_carga()
        self.migrate_retenciones()
        self.migrate_certificados_obras()
        self.migrate_resumen_rend_obras()

    # --------------------------------------------------
    def migrate_programas(self) -> pd.DataFrame:
        """ "Migrate table programas"""
        self._TABLE_NAME = "PROGRAMAS"
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(
            columns={"Programa": "programa", "DescProg": "desc_prog"}, inplace=True
        )
        self._TABLE_NAME = "programas"
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_subprogramas(self) -> pd.DataFrame:
        """ "Migrate table subprogramas"""
        self._TABLE_NAME = "SUBPROGRAMAS"
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(
            columns={
                "Programa": "programa",
                "DescSubprog": "desc_subprog",
                "Subprograma": "subprograma",
            },
            inplace=True,
        )
        self._TABLE_NAME = "subprogramas"
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_proyectos(self) -> pd.DataFrame:
        """ "Migrate table proyectos"""
        self._TABLE_NAME = "PROYECTOS"
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(
            columns={
                "Proyecto": "proyecto",
                "DescProy": "desc_proy",
                "Subprograma": "subprograma",
            },
            inplace=True,
        )
        self._TABLE_NAME = "proyectos"
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_actividades(self) -> pd.DataFrame:
        """ "Migrate table actividades"""
        self._TABLE_NAME = "ACTIVIDADES"
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(
            columns={
                "Actividad": "actividad",
                "DescAct": "desc_act",
                "Proyecto": "proyecto",
            },
            inplace=True,
        )
        self._TABLE_NAME = "actividades"
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_ctas_ctes(self) -> pd.DataFrame:
        """ "Migrate table ctas_ctes"""
        self._TABLE_NAME = "CUENTASBANCARIAS"
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(
            columns={
                "CuentaAnterior": "cta_cte_anterior",
                "Cuenta": "cta_cte",
                "Descripcion": "desc_cta_cte",
                "Banco": "banco",
            },
            inplace=True,
        )
        self._TABLE_NAME = "ctas_ctes"
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_fuentes(self) -> pd.DataFrame:
        """ "Migrate table fuentes"""
        self._TABLE_NAME = "FUENTES"
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(
            columns={
                "Fuente": "fuente",
                "Descripcion": "desc_fte",
                "Abreviatura": "abreviatura",
            },
            inplace=True,
        )
        self._TABLE_NAME = "fuentes"
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_partidas(self) -> pd.DataFrame:
        """ "Migrate table partidas"""
        self._TABLE_NAME = "PARTIDAS"
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(
            columns={
                "Grupo": "grupo",
                "DescGrupo": "desc_grupo",
                "PartidaParcial": "partida_parcial",
                "DescPartidaParcial": "desc_part_parcial",
                "Partida": "partida",
                "DescPartida": "desc_part",
            },
            inplace=True,
        )
        self._TABLE_NAME = "partidas"
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_proveedores(self) -> pd.DataFrame:
        """ "Migrate table proveedores"""
        self._TABLE_NAME = "PROVEEDORES"
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(
            columns={
                "Codigo": "codigo",
                "Descripcion": "desc_prov",
                "Domicilio": "domicilio",
                "Localidad": "localidad",
                "Telefono": "telefono",
                "CUIT": "cuit",
                "CondicionIVA": "condicion_iva",
            },
            inplace=True,
        )
        self._TABLE_NAME = "proveedores"
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_obras(self) -> pd.DataFrame:
        """ "Migrate table obras"""
        self._TABLE_NAME = "OBRAS"
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(
            columns={
                "Localidad": "localidad",
                "CUIT": "cuit",
                "Imputacion": "actividad",
                "Partida": "partida",
                "Fuente": "fuente",
                "MontoDeContrato": "monto_contrato",
                "Adicional": "adicional",
                "Cuenta": "cta_cte",
                "NormaLegal": "norma_legal",
                "Descripcion": "obra",
                "InformacionAdicional": "info_adicional",
            },
            inplace=True,
        )
        self._TABLE_NAME = "obras"
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_carga(self) -> pd.DataFrame:
        """ "Migrate table carga"""
        self._TABLE_NAME = "CARGA"
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(
            columns={
                "Fecha": "fecha",
                "Fuente": "fuente",
                "CUIT": "cuit",
                "Importe": "importe",
                "FondoDeReparo": "fondo_reparo",
                "Cuenta": "cta_cte",
                "Avance": "avance",
                "Certificado": "certificado",
                "Comprobante": "nro_comprobante",
                "Obra": "obra",
                "Origen": "origen",
                "Tipo": "tipo",
                "Imputacion": "actividad",
                "Partida": "partida",
            },
            inplace=True,
        )
        self.df = self.df.loc[~self.df.actividad.isnull()]
        self.df["fecha"] = pd.TimedeltaIndex(self.df["fecha"], unit="d") + dt.datetime(
            1970, 1, 1
        )
        self.df["id"] = self.df["nro_comprobante"] + "C"
        self.df.loc[self.df["tipo"] == "PA6", "id"] = self.df["nro_comprobante"] + "F"
        self.df["ejercicio"] = self.df["fecha"].dt.year.astype(str)
        self.df["mes"] = (
            self.df["fecha"].dt.month.astype(str).str.zfill(2)
            + "/"
            + self.df["ejercicio"]
        )
        self._TABLE_NAME = "carga"
        # # Imprimir los registros duplicados
        # duplicates = self.df[self.df.duplicated(subset='id', keep=False)]
        # print("Registros duplicados en el campo 'id':")
        # print(duplicates)
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_retenciones(self) -> pd.DataFrame:
        """ "Migrate table retenciones"""
        self._TABLE_NAME = "RETENCIONES"
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(
            columns={
                "Codigo": "codigo",
                "Importe": "importe",
                "Comprobante": "nro_comprobante",
                "Tipo": "tipo",
            },
            inplace=True,
        )
        self.df["id_carga"] = self.df["nro_comprobante"] + "C"
        self.df.loc[self.df["tipo"] == "PA6", "id_carga"] = (
            self.df["nro_comprobante"] + "F"
        )
        self.df.drop(["nro_comprobante", "tipo"], axis=1, inplace=True)
        self._TABLE_NAME = "retenciones"
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_certificados_obras(self) -> pd.DataFrame:
        """ "Migrate table certificados_obras"""
        self._TABLE_NAME = "CERTIFICADOS"
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(
            columns={
                "NroComprobanteSIIF": "nro_comprobante",
                "TipoComprobanteSIIF": "tipo",
                "Origen": "origen",
                "Periodo": "ejercicio",
                "Beneficiario": "beneficiario",
                "Obra": "obra",
                "NroCertificado": "nro_certificado",
                "MontoCertificado": "monto_certificado",
                "FondoDeReparo": "fondo_reparo",
                "ImporteBruto": "importe_bruto",
                "IIBB": "iibb",
                "LP": "lp",
                "SUSS": "suss",
                "GCIAS": "gcias",
                "INVICO": "invico",
                "ImporteNeto": "importe_neto",
            },
            inplace=True,
        )
        self.df["otros"] = 0
        self.df["cod_obra"] = self.df["obra"].str.split(" ", n=1).str[0]
        self.df.loc[self.df["nro_comprobante"] != "", "id_carga"] = (
            self.df["nro_comprobante"] + "C"
        )
        self.df.loc[self.df["tipo"] == "PA6", "id_carga"] = (
            self.df["nro_comprobante"] + "F"
        )
        self.df.drop(["nro_comprobante", "tipo"], axis=1, inplace=True)
        self._TABLE_NAME = "certificados_obras"
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_resumen_rend_obras(self) -> pd.DataFrame:
        """ "Migrate table resumen_rend_obras"""
        self._TABLE_NAME = "EPAM"
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(
            columns={
                "NroComprobanteSIIF": "nro_comprobante",
                "TipoComprobanteSIIF": "tipo",
                "Origen": "origen",
                "Obra": "obra",
                "Periodo": "ejercicio",
                "Beneficiario": "beneficiario",
                "LibramientoSGF": "libramiento_sgf",
                "FechaPago": "fecha",
                "ImporteBruto": "importe_bruto",
                "IIBB": "iibb",
                "TL": "lp",
                "Sellos": "sellos",
                "SUSS": "suss",
                "GCIAS": "gcias",
                "ImporteNeto": "importe_neto",
            },
            inplace=True,
        )
        self.df["destino"] = ""
        self.df["movimiento"] = ""
        self.df["seguro"] = 0
        self.df["salud"] = 0
        self.df["mutual"] = 0
        self.df["cod_obra"] = self.df["obra"].str.split("-", n=1).str[0]
        self.df["fecha"] = pd.TimedeltaIndex(self.df["fecha"], unit="d") + dt.datetime(
            1970, 1, 1
        )
        self.df["ejercicio"] = self.df["fecha"].dt.year.astype(str)
        self.df["mes"] = (
            self.df["fecha"].dt.month.astype(str).str.zfill(2)
            + "/"
            + self.df["ejercicio"]
        )
        self.df.loc[self.df["nro_comprobante"] != "", "id_carga"] = (
            self.df["nro_comprobante"] + "C"
        )
        self.df.loc[self.df["tipo"] == "PA6", "id_carga"] = (
            self.df["nro_comprobante"] + "F"
        )
        self.df.drop(["nro_comprobante", "tipo"], axis=1, inplace=True)
        self._TABLE_NAME = "resumen_rend_obras"
        self.to_sql(self.path_new_icaro, True)


# --------------------------------------------------
async def main():
    """Make a jazz noise here"""
    from ...config import settings, Database

    Database.initialize()
    try:
        await Database.client.admin.command("ping")
        print("Connected to MongoDB")
    except Exception as e:
        print("Error connecting to MongoDB:", e)
        return

    args = get_args()

    migrator = IcaroMongoMigrator(
        sqlite_path=args.file,
    )

    await migrator.migrate_all()

# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.icaro.handlers.migrate_icaro
