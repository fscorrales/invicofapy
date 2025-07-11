#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 20-jun-2025
Purpose: Migrate from old Icaro.sqlite to new DB
"""

__all__ = ["IcaroMongoMigrator"]

import argparse
import asyncio
import inspect
import os
import sqlite3

import pandas as pd

from ...config import logger
from ...utils import (
    RouteReturnSchema,
    sync_validated_to_repository,
    validate_and_extract_data_from_df,
)
from ..repositories import (
    ActividadesRepository,
    CargaRepository,
    CertificadosRepository,
    CtasCtesRepository,
    EstructurasRepository,
    FuentesRepository,
    ObrasRepository,
    PartidasRepository,
    ProgramasRepository,
    ProveedoresRepository,
    ProyectosRepository,
    ResumenRendObrasRepository,
    RetencionesRepository,
    SubprogramasRepository,
)
from ..schemas import CargaReport


def validate_sqlite_file(path):
    if not os.path.exists(path):
        raise argparse.ArgumentTypeError(f"El archivo {path} no existe")
    if not path.endswith(".sqlite") and not path.endswith(".db"):
        raise argparse.ArgumentTypeError(
            f"El archivo {path} no parece ser un archivo SQLite"
        )
    try:
        sqlite3.connect(path)
    except sqlite3.Error as e:
        raise argparse.ArgumentTypeError(
            f"Error al conectar al archivo SQLite {path}: {e}"
        )
    return path


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

    parser = argparse.ArgumentParser(
        description="Migrate from old Icaro.sqlite to new DB",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-f",
        "--file",
        metavar="sqlite_file",
        default=os.path.join(path, "ICARO.sqlite"),
        type=validate_sqlite_file,
        help="Path al archivo SQLite de Icaro",
    )

    args = parser.parse_args()

    return args


# --------------------------------------------------
class IcaroMongoMigrator:
    # --------------------------------------------------
    def __init__(self, sqlite_path: str):
        self.sqlite_path = sqlite_path

        # Repositorios por colección
        self.programas_repo = ProgramasRepository()
        self.subprogramas_repo = SubprogramasRepository()
        self.proyectos_repo = ProyectosRepository()
        self.actividades_repo = ActividadesRepository()
        self.estructuras_repo = EstructurasRepository()
        self.ctas_ctes_repo = CtasCtesRepository()
        self.fuentes_repo = FuentesRepository()
        self.partidas_repo = PartidasRepository()
        self.proveedores_repo = ProveedoresRepository()
        self.obras_repo = ObrasRepository()
        self.carga_repo = CargaRepository()
        self.retenciones_repo = RetencionesRepository()
        self.certificados_repo = CertificadosRepository()
        self.resumen_rend_obras_repo = ResumenRendObrasRepository()

    # --------------------------------------------------
    def from_sql(self, table: str) -> pd.DataFrame:
        import sqlite3

        with sqlite3.connect(self.sqlite_path) as conn:
            return pd.read_sql_query(f"SELECT * FROM {table}", conn)

    # --------------------------------------------------
    async def migrate_programas(self):
        df = self.from_sql("PROGRAMAS")
        df.rename(
            columns={"Programa": "programa", "DescProg": "desc_programa"}, inplace=True
        )
        await self.programas_repo.delete_all()
        await self.programas_repo.save_all(df.to_dict(orient="records"))

    # --------------------------------------------------
    async def migrate_subprogramas(self):
        df = self.from_sql("SUBPROGRAMAS")
        df.rename(
            columns={
                "Programa": "programa",
                "Subprograma": "subprograma",
                "DescSubprog": "desc_subprograma",
            },
            inplace=True,
        )
        await self.subprogramas_repo.delete_all()
        await self.subprogramas_repo.save_all(df.to_dict(orient="records"))

    # --------------------------------------------------
    async def migrate_proyectos(self):
        df = self.from_sql("PROYECTOS")
        df.rename(
            columns={
                "Subprograma": "subprograma",
                "Proyecto": "proyecto",
                "DescProy": "desc_proyecto",
            },
            inplace=True,
        )
        await self.proyectos_repo.delete_all()
        await self.proyectos_repo.save_all(df.to_dict(orient="records"))

    # --------------------------------------------------
    async def migrate_actividades(self):
        df = self.from_sql("ACTIVIDADES")
        df.rename(
            columns={
                "Proyecto": "proyecto",
                "Actividad": "actividad",
                "DescAct": "desc_actividad",
            },
            inplace=True,
        )
        await self.actividades_repo.delete_all()
        await self.actividades_repo.save_all(df.to_dict(orient="records"))

    # --------------------------------------------------
    async def migrate_estructuras(self):
        await self.estructuras_repo.delete_all()
        # Programas
        df = self.from_sql("PROGRAMAS")
        df.rename(
            columns={"Programa": "estructura", "DescProg": "desc_estructura"},
            inplace=True,
        )
        await self.estructuras_repo.save_all(df.to_dict(orient="records"))

        # Subprogramas
        df = self.from_sql("SUBPROGRAMAS")
        df.rename(
            columns={
                "Subprograma": "estructura",
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
                "Proyecto": "estructura",
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
                "Actividad": "estructura",
                "DescAct": "desc_estructura",
            },
            inplace=True,
        )
        df.drop(["Proyecto"], axis=1, inplace=True)
        await self.estructuras_repo.save_all(df.to_dict(orient="records"))

    # --------------------------------------------------
    async def migrate_ctas_ctes(self):
        df = self.from_sql("CUENTASBANCARIAS")
        df.rename(
            columns={
                "CuentaAnterior": "cta_cte_anterior",
                "Cuenta": "cta_cte",
                "Descripcion": "desc_cta_cte",
                "Banco": "banco",
            },
            inplace=True,
        )
        await self.ctas_ctes_repo.delete_all()
        await self.ctas_ctes_repo.save_all(df.to_dict(orient="records"))

    # --------------------------------------------------
    async def migrate_fuentes(self):
        df = self.from_sql("FUENTES")
        df.rename(
            columns={
                "Fuente": "fuente",
                "Descripcion": "desc_fuente",
                "Abreviatura": "abreviatura",
            },
            inplace=True,
        )
        await self.fuentes_repo.delete_all()
        await self.fuentes_repo.save_all(df.to_dict(orient="records"))

    # --------------------------------------------------
    async def migrate_partidas(self):
        df = self.from_sql("PARTIDAS")
        df.rename(
            columns={
                "Grupo": "grupo",
                "DescGrupo": "desc_grupo",
                "PartidaParcial": "partida_parcial",
                "DescPartidaParcial": "desc_partida_parcial",
                "Partida": "partida",
                "DescPartida": "desc_partida",
            },
            inplace=True,
        )
        await self.partidas_repo.delete_all()
        await self.partidas_repo.save_all(df.to_dict(orient="records"))

    # --------------------------------------------------
    async def migrate_proveedores(self):
        df = self.from_sql("PROVEEDORES")
        df.rename(
            columns={
                "Codigo": "codigo",
                "Descripcion": "desc_proveedor",
                "Domicilio": "domicilio",
                "Localidad": "localidad",
                "Telefono": "telefono",
                "CUIT": "cuit",
                "CondicionIVA": "condicion_iva",
            },
            inplace=True,
        )
        await self.proveedores_repo.delete_all()
        await self.proveedores_repo.save_all(df.to_dict(orient="records"))

    # --------------------------------------------------
    async def migrate_obras(self):
        df = self.from_sql("OBRAS")
        df.rename(
            columns={
                "Localidad": "localidad",
                "CUIT": "cuit",
                "Imputacion": "actividad",
                "Partida": "partida",
                "Fuente": "fuente",
                "MontoDeContrato": "monto_contrato",
                "Adicional": "monto_adicional",
                "Cuenta": "cta_cte",
                "NormaLegal": "norma_legal",
                "Descripcion": "desc_obra",
                "InformacionAdicional": "info_adicional",
            },
            inplace=True,
        )
        await self.obras_repo.delete_all()
        await self.obras_repo.save_all(df.to_dict(orient="records"))

    # --------------------------------------------------
    async def migrate_carga(self) -> RouteReturnSchema:
        df = self.from_sql("CARGA")
        df.rename(
            columns={
                "Fecha": "fecha",
                "Fuente": "fuente",
                "CUIT": "cuit",
                "Importe": "importe",
                "FondoDeReparo": "fondo_reparo",
                "Cuenta": "cta_cte",
                "Avance": "avance",
                "Certificado": "nro_certificado",
                "Comprobante": "nro_comprobante",
                "Obra": "desc_obra",
                "Origen": "origen",
                "Tipo": "tipo",
                "Imputacion": "actividad",
                "Partida": "partida",
            },
            inplace=True,
        )
        df = df.loc[~df.actividad.isnull()]
        df["fecha"] = pd.to_timedelta(df["fecha"], unit="D") + pd.Timestamp(
            "1970-01-01"
        )
        df["id_carga"] = df["nro_comprobante"] + "C"
        df.loc[df["tipo"] == "PA6", "id_carga"] = df["nro_comprobante"] + "F"
        df["ejercicio"] = df["fecha"].dt.year
        df["mes"] = (
            df["fecha"].dt.month.astype(str).str.zfill(2)
            + "/"
            + df["ejercicio"].astype(str)
        )
        # Validar datos usando Pydantic
        validate_and_errors = validate_and_extract_data_from_df(
            dataframe=df, model=CargaReport, field_id="id_carga"
        )
        # await self.carga_repo.delete_all()
        # await self.carga_repo.save_all(df.to_dict(orient="records"))
        return await sync_validated_to_repository(
            repository=self.carga_repo,
            validation=validate_and_errors,
            delete_filter=None,
            title="ICARO Carga Migration",
            logger=logger,
            label="Tabla CARGA de ICARO",
        )

    # --------------------------------------------------
    async def migrate_retenciones(self):
        df = self.from_sql("RETENCIONES")
        df.rename(
            columns={
                "Codigo": "codigo",
                "Importe": "importe",
                "Comprobante": "nro_comprobante",
                "Tipo": "tipo",
            },
            inplace=True,
        )
        df["id_carga"] = df["nro_comprobante"] + "C"
        df.loc[df["tipo"] == "PA6", "id_carga"] = df["nro_comprobante"] + "F"
        df.drop(["nro_comprobante", "tipo"], axis=1, inplace=True)
        await self.retenciones_repo.delete_all()
        await self.retenciones_repo.save_all(df.to_dict(orient="records"))

    # --------------------------------------------------
    async def migrate_certificados(self):
        df = self.from_sql("CERTIFICADOS")
        df.rename(
            columns={
                "NroComprobanteSIIF": "nro_comprobante",
                "TipoComprobanteSIIF": "tipo",
                "Origen": "origen",
                "Periodo": "ejercicio",
                "Beneficiario": "beneficiario",
                "Obra": "desc_obra",
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
        df["ejercicio"] = pd.to_numeric(df["ejercicio"], errors="coerce")
        df["otras_retenciones"] = 0
        df["cod_obra"] = df["desc_obra"].str.split(" ", n=1).str[0]
        df.loc[df["nro_comprobante"] != "", "id_carga"] = df["nro_comprobante"] + "C"
        df.loc[df["tipo"] == "PA6", "id_carga"] = df["nro_comprobante"] + "F"
        df.drop(["nro_comprobante", "tipo"], axis=1, inplace=True)
        await self.certificados_repo.delete_all()
        await self.certificados_repo.save_all(df.to_dict(orient="records"))

    # --------------------------------------------------
    async def migrate_resumen_rend_obras(self):
        df = self.from_sql("EPAM")
        df.rename(
            columns={
                "NroComprobanteSIIF": "nro_comprobante",
                "TipoComprobanteSIIF": "tipo",
                "Origen": "origen",
                "Obra": "desc_obra",
                "Periodo": "ejercicio",
                "Beneficiario": "beneficiario",
                "LibramientoSGF": "nro_libramiento_sgf",
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
        df["destino"] = ""
        df["movimiento"] = ""
        df["seguro"] = 0
        df["salud"] = 0
        df["mutual"] = 0
        df["cod_obra"] = df["desc_obra"].str.split("-", n=1).str[0]
        df["fecha"] = pd.to_timedelta(df["fecha"], unit="D") + pd.Timestamp(
            "1970-01-01"
        )
        df["ejercicio"] = df["fecha"].dt.year
        df["mes"] = (
            df["fecha"].dt.month.astype(str).str.zfill(2)
            + "/"
            + df["ejercicio"].astype(str)
        )
        df.loc[df["nro_comprobante"] != "", "id_carga"] = df["nro_comprobante"] + "C"
        df.loc[df["tipo"] == "PA6", "id_carga"] = df["nro_comprobante"] + "F"
        df.drop(["nro_comprobante", "tipo"], axis=1, inplace=True)
        await self.resumen_rend_obras_repo.delete_all()
        await self.resumen_rend_obras_repo.save_all(df.to_dict(orient="records"))

    # --------------------------------------------------
    async def migrate_all(self):
        await self.migrate_programas()
        await self.migrate_subprogramas()
        await self.migrate_proyectos()
        await self.migrate_actividades()
        await self.migrate_estructuras()
        await self.migrate_ctas_ctes()
        await self.migrate_fuentes()
        await self.migrate_partidas()
        await self.migrate_proveedores()
        await self.migrate_obras()
        await self.migrate_carga()
        await self.migrate_retenciones()
        await self.migrate_certificados()
        await self.migrate_resumen_rend_obras()


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

    migrator = IcaroMongoMigrator(
        sqlite_path=args.file,
    )

    await migrator.migrate_all()


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.icaro.handlers.migrate_icaro
