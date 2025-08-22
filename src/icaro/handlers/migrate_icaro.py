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
from typing import List

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
from ..schemas import (
    ActividadesReport,
    CargaReport,
    CertificadosReport,
    CtasCtesReport,
    EstructurasReport,
    FuentesReport,
    ObrasReport,
    PartidasReport,
    ProgramasReport,
    ProveedoresReport,
    ProyectosReport,
    ResumenRendObrasReport,
    RetencionesReport,
    SubprogramasReport,
)


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
    async def migrate_programas(self) -> RouteReturnSchema:
        df = self.from_sql("PROGRAMAS")
        df.rename(
            columns={"Programa": "programa", "DescProg": "desc_programa"}, inplace=True
        )

        # Validar datos usando Pydantic
        validate_and_errors = validate_and_extract_data_from_df(
            dataframe=df, model=ProgramasReport, field_id="programa"
        )
        # await self.programas_repo.delete_all()
        # await self.programas_repo.save_all(df.to_dict(orient="records"))
        return await sync_validated_to_repository(
            repository=self.programas_repo,
            validation=validate_and_errors,
            delete_filter=None,
            title="ICARO Programas Migration",
            logger=logger,
            label="Tabla Programas de ICARO",
        )

    # --------------------------------------------------
    async def migrate_subprogramas(self) -> RouteReturnSchema:
        df = self.from_sql("SUBPROGRAMAS")
        df.rename(
            columns={
                "Programa": "programa",
                "Subprograma": "subprograma",
                "DescSubprog": "desc_subprograma",
            },
            inplace=True,
        )

        # Validar datos usando Pydantic
        validate_and_errors = validate_and_extract_data_from_df(
            dataframe=df, model=SubprogramasReport, field_id="subprograma"
        )
        # await self.subprogramas_repo.delete_all()
        # await self.subprogramas_repo.save_all(df.to_dict(orient="records"))
        return await sync_validated_to_repository(
            repository=self.subprogramas_repo,
            validation=validate_and_errors,
            delete_filter=None,
            title="ICARO Subprogramas Migration",
            logger=logger,
            label="Tabla Subprogramas de ICARO",
        )

    # --------------------------------------------------
    async def migrate_proyectos(self) -> RouteReturnSchema:
        df = self.from_sql("PROYECTOS")
        df.rename(
            columns={
                "Subprograma": "subprograma",
                "Proyecto": "proyecto",
                "DescProy": "desc_proyecto",
            },
            inplace=True,
        )

        # Validar datos usando Pydantic
        validate_and_errors = validate_and_extract_data_from_df(
            dataframe=df, model=ProyectosReport, field_id="proyecto"
        )
        # await self.proyectos_repo.delete_all()
        # await self.proyectos_repo.save_all(df.to_dict(orient="records"))
        return await sync_validated_to_repository(
            repository=self.proyectos_repo,
            validation=validate_and_errors,
            delete_filter=None,
            title="ICARO Proyectos Migration",
            logger=logger,
            label="Tabla Proyectos de ICARO",
        )

    # --------------------------------------------------
    async def migrate_actividades(self) -> RouteReturnSchema:
        df = self.from_sql("ACTIVIDADES")
        df.rename(
            columns={
                "Proyecto": "proyecto",
                "Actividad": "actividad",
                "DescAct": "desc_actividad",
            },
            inplace=True,
        )

        # Validar datos usando Pydantic
        validate_and_errors = validate_and_extract_data_from_df(
            dataframe=df, model=ActividadesReport, field_id="actividad"
        )
        # await self.actividades_repo.delete_all()
        # await self.actividades_repo.save_all(df.to_dict(orient="records"))
        return await sync_validated_to_repository(
            repository=self.actividades_repo,
            validation=validate_and_errors,
            delete_filter=None,
            title="ICARO Actividades Migration",
            logger=logger,
            label="Tabla Actividades de ICARO",
        )

    # --------------------------------------------------
    async def migrate_estructuras(self) -> RouteReturnSchema:
        # await self.estructuras_repo.delete_all()
        # Programas
        df = self.from_sql("PROGRAMAS")
        df.rename(
            columns={"Programa": "estructura", "DescProg": "desc_estructura"},
            inplace=True,
        )
        # await self.estructuras_repo.save_all(df.to_dict(orient="records"))

        # Subprogramas
        df_aux = self.from_sql("SUBPROGRAMAS")
        df_aux.rename(
            columns={
                "Subprograma": "estructura",
                "DescSubprog": "desc_estructura",
            },
            inplace=True,
        )
        df_aux.drop(["Programa"], axis=1, inplace=True)
        df = pd.concat([df, df_aux], ignore_index=True)
        # await self.estructuras_repo.save_all(df.to_dict(orient="records"))

        # Proyectos
        df_aux = self.from_sql("PROYECTOS")
        df_aux.rename(
            columns={
                "Proyecto": "estructura",
                "DescProy": "desc_estructura",
            },
            inplace=True,
        )
        df_aux.drop(["Subprograma"], axis=1, inplace=True)
        df = pd.concat([df, df_aux], ignore_index=True)
        # await self.estructuras_repo.save_all(df.to_dict(orient="records"))

        # Actividades
        df_aux = self.from_sql("ACTIVIDADES")
        df_aux.rename(
            columns={
                "Actividad": "estructura",
                "DescAct": "desc_estructura",
            },
            inplace=True,
        )
        df_aux.drop(["Proyecto"], axis=1, inplace=True)
        df = pd.concat([df, df_aux], ignore_index=True)
        # Validar datos usando Pydantic
        validate_and_errors = validate_and_extract_data_from_df(
            dataframe=df, model=EstructurasReport, field_id="estructura"
        )
        return await sync_validated_to_repository(
            repository=self.estructuras_repo,
            validation=validate_and_errors,
            delete_filter=None,
            title="ICARO Estructuras Migration",
            logger=logger,
            label="Tabla Estructuras de ICARO",
        )
        # await self.estructuras_repo.save_all(df.to_dict(orient="records"))

    # --------------------------------------------------
    async def migrate_ctas_ctes(self) -> RouteReturnSchema:
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

        # Validar datos usando Pydantic
        validate_and_errors = validate_and_extract_data_from_df(
            dataframe=df, model=CtasCtesReport, field_id="cta_cte"
        )
        # await self.ctas_ctes_repo.delete_all()
        # await self.ctas_ctes_repo.save_all(df.to_dict(orient="records"))
        return await sync_validated_to_repository(
            repository=self.ctas_ctes_repo,
            validation=validate_and_errors,
            delete_filter=None,
            title="ICARO Ctas Ctes Migration",
            logger=logger,
            label="Tabla Ctas Ctes de ICARO",
        )

    # --------------------------------------------------
    async def migrate_fuentes(self) -> RouteReturnSchema:
        df = self.from_sql("FUENTES")
        df.rename(
            columns={
                "Fuente": "fuente",
                "Descripcion": "desc_fuente",
                "Abreviatura": "abreviatura",
            },
            inplace=True,
        )

        # Validar datos usando Pydantic
        validate_and_errors = validate_and_extract_data_from_df(
            dataframe=df, model=FuentesReport, field_id="fuente"
        )
        # await self.fuentes_repo.delete_all()
        # await self.fuentes_repo.save_all(df.to_dict(orient="records"))
        return await sync_validated_to_repository(
            repository=self.fuentes_repo,
            validation=validate_and_errors,
            delete_filter=None,
            title="ICARO Fuentes Migration",
            logger=logger,
            label="Tabla Fuentes de ICARO",
        )

    # --------------------------------------------------
    async def migrate_partidas(self) -> RouteReturnSchema:
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

        # Validar datos usando Pydantic
        validate_and_errors = validate_and_extract_data_from_df(
            dataframe=df, model=PartidasReport, field_id="partida"
        )
        # await self.partidas_repo.delete_all()
        # await self.partidas_repo.save_all(df.to_dict(orient="records"))
        return await sync_validated_to_repository(
            repository=self.partidas_repo,
            validation=validate_and_errors,
            delete_filter=None,
            title="ICARO Partidas Migration",
            logger=logger,
            label="Tabla Partidas de ICARO",
        )

    # --------------------------------------------------
    async def migrate_proveedores(self) -> RouteReturnSchema:
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
        # Validar datos usando Pydantic
        validate_and_errors = validate_and_extract_data_from_df(
            dataframe=df, model=ProveedoresReport, field_id="codigo"
        )

        # await self.proveedores_repo.delete_all()
        # await self.proveedores_repo.save_all(df.to_dict(orient="records"))
        return await sync_validated_to_repository(
            repository=self.proveedores_repo,
            validation=validate_and_errors,
            delete_filter=None,
            title="ICARO Proveedores Migration",
            logger=logger,
            label="Tabla Proveedores de ICARO",
        )

    # --------------------------------------------------
    async def migrate_obras(self) -> RouteReturnSchema:
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
        # Validar datos usando Pydantic
        validate_and_errors = validate_and_extract_data_from_df(
            dataframe=df, model=ObrasReport, field_id="actividad"
        )
        # await self.obras_repo.delete_all()
        # await self.obras_repo.save_all(df.to_dict(orient="records"))
        return await sync_validated_to_repository(
            repository=self.obras_repo,
            validation=validate_and_errors,
            delete_filter=None,
            title="ICARO Obras Migration",
            logger=logger,
            label="Tabla OBRAS de ICARO",
        )

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
        # df["fecha"] = df["fecha"].apply(
        #     lambda x: x.to_pydatetime() if pd.notnull(x) else None
        # )

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
    async def migrate_retenciones(self) -> RouteReturnSchema:
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

        # Validar datos usando Pydantic
        validate_and_errors = validate_and_extract_data_from_df(
            dataframe=df, model=RetencionesReport, field_id="id_carga"
        )
        # await self.retenciones_repo.delete_all()
        # await self.retenciones_repo.save_all(df.to_dict(orient="records"))
        return await sync_validated_to_repository(
            repository=self.retenciones_repo,
            validation=validate_and_errors,
            delete_filter=None,
            title="ICARO Retenciones Migration",
            logger=logger,
            label="Tabla Retenciones de ICARO",
        )

    # --------------------------------------------------
    async def migrate_certificados(self) -> RouteReturnSchema:
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

        # Validar datos usando Pydantic
        validate_and_errors = validate_and_extract_data_from_df(
            dataframe=df, model=CertificadosReport, field_id="id_carga"
        )
        # await self.certificados_repo.delete_all()
        # await self.certificados_repo.save_all(df.to_dict(orient="records"))
        return await sync_validated_to_repository(
            repository=self.certificados_repo,
            validation=validate_and_errors,
            delete_filter=None,
            title="ICARO Certificados Migration",
            logger=logger,
            label="Tabla Certificados de ICARO",
        )

    # --------------------------------------------------
    async def migrate_resumen_rend_obras(self) -> RouteReturnSchema:
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
        # df["fecha"] = df["fecha"].apply(
        #     lambda x: x.to_pydatetime() if pd.notnull(x) else None
        # )

        # Validar datos usando Pydantic
        validate_and_errors = validate_and_extract_data_from_df(
            dataframe=df, model=ResumenRendObrasReport, field_id="id_carga"
        )
        logger.info(validate_and_errors)
        # await self.resumen_rend_obras_repo.delete_all()
        # await self.resumen_rend_obras_repo.save_all(df.to_dict(orient="records"))
        return await sync_validated_to_repository(
            repository=self.resumen_rend_obras_repo,
            validation=validate_and_errors,
            delete_filter=None,
            title="ICARO Resumen Rend Obras Migration",
            logger=logger,
            label="Tabla Resumen Rend Obras de ICARO",
        )

    # --------------------------------------------------
    async def migrate_all(self) -> List[RouteReturnSchema]:
        return_schema = []
        return_schema.append(await self.migrate_programas())
        return_schema.append(await self.migrate_subprogramas())
        return_schema.append(await self.migrate_proyectos())
        return_schema.append(await self.migrate_actividades())
        return_schema.append(await self.migrate_estructuras())
        return_schema.append(await self.migrate_ctas_ctes())
        return_schema.append(await self.migrate_fuentes())
        return_schema.append(await self.migrate_partidas())
        return_schema.append(await self.migrate_proveedores())
        return_schema.append(await self.migrate_obras())
        return_schema.append(await self.migrate_carga())
        return_schema.append(await self.migrate_retenciones())
        return_schema.append(await self.migrate_certificados())
        return_schema.append(await self.migrate_resumen_rend_obras())
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

    migrator = IcaroMongoMigrator(
        sqlite_path=args.file,
    )

    await migrator.migrate_proveedores()


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.icaro.handlers.migrate_icaro
