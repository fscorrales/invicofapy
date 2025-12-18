#!/usr/bin/env python3

"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Control Banco SIIF vs SSCC (Banco Real)
Data required:
    - SIIF rci02
    - SIIF rf602
    - SIIF rf610
    - SIIF rcg01_uejp
    - SIIF gto_rpa03g
    - SIIF rfondos04 (PA3 y REV)
    - SIIF rfondo07tp
    - SIIF rdeu012
    - SIIF rvicon03
    - SIIF rcocc31
    - SGF Resumen de Rendiciones
    - SSCC Resumen General de Movimientos
    - SSCC ctas_ctes (manual data)
    - Icaro (Carga)
    - SGF Listado Proveedores (POR LE MONENTO USO PROVEEDORES DE ICARO)
    - SLAVE
"""

__all__ = ["ControlCompletoService", "ControlCompletoServiceDependency"]

import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, List

from dateutil.relativedelta import relativedelta
from fastapi import Depends, HTTPException
from playwright.async_api import async_playwright
from pydantic import ValidationError

from ...config import logger
from ...icaro.handlers import IcaroMongoMigrator
from ...sgf.services import ResumenRendProvServiceDependency
from ...siif.handlers import (
    Rcg01Uejp,
    Rci02,
    Rcocc31,
    Rdeu012,
    Rf602,
    Rf610,
    Rfondo07tp,
    Rfondos04,
    Rpa03g,
    Rvicon03,
    login,
    logout,
)
from ...siif.schemas import GrupoPartidaSIIF
from ...slave.handlers import SlaveMongoMigrator
from ...sscc.services import BancoINVICOServiceDependency, CtasCtesServiceDependency
from ...utils import (
    GoogleExportResponse,
    RouteReturnSchema,
    get_r_icaro_path,
)
from ..handlers import (
    get_siif_rvicon03,
)
from ..repositories.control_banco import ControlBancoRepositoryDependency
from ..schemas.control_completo import (
    ControlCompletoParams,
    ControlCompletoSyncParams,
)
from ..services.control_aporte_empresario import (
    ControlAporteEmpresarioServiceDependency,
)
from ..services.control_banco import ControlBancoServiceDependency
from ..services.control_debitos_bancarios import (
    ControlDebitosBancariosServiceDependency,
)
from ..services.control_escribanos import ControlEscribanosServiceDependency
from ..services.control_haberes import ControlHaberesServiceDependency
from ..services.control_honorarios import ControlHonorariosServiceDependency
from ..services.control_icaro_vs_siif import ControlIcaroVsSIIFServiceDependency
from ..services.control_obras import ControlObrasServiceDependency
from ..services.control_recursos import ControlRecursosServiceDependency
from ..services.control_viaticos import ControlViaticosServiceDependency


# --------------------------------------------------
@dataclass
class ControlCompletoService:
    control_banco_repo: ControlBancoRepositoryDependency
    siif_rci02_handler: Rci02 = field(init=False)  # No se pasa como argumento
    siif_rf602_handler: Rf602 = field(init=False)  # No se pasa como argumento
    siif_rf610_handler: Rf610 = field(init=False)  # No se pasa como argumento
    siif_rcg01_uejp_handler: Rcg01Uejp = field(init=False)  # No se pasa como argumento
    siif_rpa03g_handler: Rpa03g = field(init=False)  # No se pasa como argumento
    siif_rdeu012_handler: Rdeu012 = field(init=False)  # No se pasa como argumento
    siif_rfondos04_handler: Rfondos04 = field(init=False)  # No se pasa como argumento
    siif_rfondo07tp_handler: Rfondo07tp = field(init=False)  # No se pasa como argumento
    siif_rvicon03_handler: Rvicon03 = field(init=False)  # No se pasa como argumento
    siif_rcocc31_handler: Rcocc31 = field(init=False)  # No se pasa como argumento
    sgf_resumend_rend_prov_service: ResumenRendProvServiceDependency
    sscc_banco_invico_service: BancoINVICOServiceDependency
    sscc_ctas_ctes_service: CtasCtesServiceDependency
    control_aporte_empresario_service: ControlAporteEmpresarioServiceDependency
    control_banco_service: ControlBancoServiceDependency
    control_debitos_bancarios_service: ControlDebitosBancariosServiceDependency
    control_escribanos_service: ControlEscribanosServiceDependency
    control_haberes_service: ControlHaberesServiceDependency
    control_honorarios_service: ControlHonorariosServiceDependency
    control_icaro_vs_siif_service: ControlIcaroVsSIIFServiceDependency
    control_obras_service: ControlObrasServiceDependency
    control_recursos_service: ControlRecursosServiceDependency
    control_viaticos_service: ControlViaticosServiceDependency

    # -------------------------------------------------
    async def sync_control_completo_from_source(
        self,
        params: ControlCompletoSyncParams = None,
    ) -> List[RouteReturnSchema]:
        """Downloads a report from all sources, processes it, validates the data,
        and stores it in MongoDB if valid.

        Args:
            ejercicio (int, optional): The fiscal year for the report. Defaults to the current year.

        Returns:
            RouteReturnSchema
        """
        if params.siif_username is None or params.siif_password is None:
            raise HTTPException(
                status_code=401,
                detail="Missing username or password",
            )
        return_schema = []
        async with async_playwright() as p:
            connect_siif = await login(
                username=params.siif_username,
                password=params.siif_password,
                playwright=p,
                headless=False,
            )
            try:
                ejercicios = list(
                    range(params.ejercicio_desde, params.ejercicio_hasta + 1)
                )
                # 🔹Rci02
                self.siif_rci02_handler = Rci02(siif=connect_siif)
                await self.siif_rci02_handler.go_to_reports()
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rci02_handler.download_and_sync_validated_to_repository(
                        ejercicio=int(ejercicio),
                    )
                    return_schema.append(partial_schema)

                # 🔹 RF602
                self.siif_rf602_handler = Rf602(siif=connect_siif)
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rf602_handler.download_and_sync_validated_to_repository(
                        ejercicio=int(ejercicio)
                    )
                    return_schema.append(partial_schema)

                # 🔹 RF610
                self.siif_rf610_handler = Rf610(siif=connect_siif)
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rf610_handler.download_and_sync_validated_to_repository(
                        ejercicio=int(ejercicio)
                    )
                    return_schema.append(partial_schema)

                # 🔹 Rcg01Uejp
                self.siif_rcg01_uejp_handler = Rcg01Uejp(siif=connect_siif)
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rcg01_uejp_handler.download_and_sync_validated_to_repository(
                        ejercicio=int(ejercicio)
                    )
                    return_schema.append(partial_schema)

                # 🔹 Rpa03g
                self.siif_rpa03g_handler = Rpa03g(siif=connect_siif)
                for ejercicio in ejercicios:
                    for grupo in [g.value for g in GrupoPartidaSIIF]:
                        partial_schema = await self.siif_rpa03g_handler.download_and_sync_validated_to_repository(
                            ejercicio=int(ejercicio), grupo_partida=grupo
                        )
                        return_schema.append(partial_schema)

                # 🔹 Rfondos04
                self.siif_rfondos04_handler = Rfondos04(siif=connect_siif)
                for ejercicio in ejercicios:
                    for tipo_comprobante in ["PA3", "REV"]:
                        partial_schema = await self.siif_rfondos04_handler.download_and_sync_validated_to_repository(
                            ejercicio=int(ejercicio), tipo_comprobante=tipo_comprobante
                        )
                        return_schema.append(partial_schema)

                # 🔹 Rfondo07tp
                self.siif_rfondo07tp_handler = Rfondo07tp(siif=connect_siif)
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rfondo07tp_handler.download_and_sync_validated_to_repository(
                        ejercicio=int(ejercicio)
                    )
                    return_schema.append(partial_schema)

                # 🔹Rdeu012
                # Obtenemos los meses a descargar
                start = datetime.strptime(str(params.ejercicio_desde), "%Y")
                end = (
                    datetime.strptime("12/" + str(params.ejercicio_hasta), "%m/%Y")
                    if params.ejercicio_hasta < datetime.now().year
                    else datetime.now().replace(day=1)
                )

                meses = []
                current = start
                while current <= end:
                    meses.append(current.strftime("%m/%Y"))
                    current += relativedelta(months=1)

                self.siif_rdeu012_handler = Rdeu012(siif=connect_siif)
                for mes in meses:
                    partial_schema = await self.siif_rdeu012_handler.download_and_sync_validated_to_repository(
                        mes=str(mes)
                    )
                    return_schema.append(partial_schema)

                # 🔹Rvicon03
                self.siif_rvicon03_handler = Rvicon03(siif=connect_siif)
                # await self.siif_rvicon03_handler.go_to_reports()
                for ejercicio in ejercicios:
                    partial_schema = await self.siif_rvicon03_handler.download_and_sync_validated_to_repository(
                        ejercicio=ejercicio,
                    )
                    return_schema.append(partial_schema)

                # 🔹 Rcocc31
                self.siif_rcocc31_handler = Rcocc31(siif=connect_siif)
                for ejercicio in ejercicios:
                    cuentas_contables = await get_siif_rvicon03(ejercicio=ejercicio)
                    cuentas_contables = cuentas_contables["cta_contable"].unique()
                    logger.info(
                        f"Se Bajaran las siguientes cuentas contables: {cuentas_contables}"
                    )
                    for cta_contable in cuentas_contables:
                        partial_schema = await self.siif_rcocc31_handler.download_and_sync_validated_to_repository(
                            ejercicio=ejercicio,
                            cta_contable=cta_contable,
                        )
                        return_schema.append(partial_schema)

                # 🔹Resumen Rendicion Proveedores
                partial_schema = await self.sgf_resumend_rend_prov_service.sync_resumen_rend_prov_from_sgf(
                    username=params.sgf_username,
                    password=params.sgf_password,
                    params=params,
                )
                return_schema.extend(partial_schema)

                # 🔹Banco INVICO
                partial_schema = (
                    await self.sscc_banco_invico_service.sync_banco_invico_from_sscc(
                        username=params.sscc_username,
                        password=params.sscc_password,
                        params=params,
                    )
                )
                return_schema.extend(partial_schema)

                # 🔹Ctas Ctes
                partial_schema = (
                    await self.sscc_ctas_ctes_service.sync_ctas_ctes_from_excel(
                        excel_path=params.ctas_ctes_excel_path,
                    )
                )
                return_schema.append(partial_schema)

                # 🔹 Slave
                migrator = SlaveMongoMigrator(access_path=params.slave_access_path)
                return_schema.extend(await migrator.migrate_all())

                # 🔹 Icaro Carga
                path = os.path.join(get_r_icaro_path(), "ICARO.sqlite")
                migrator = IcaroMongoMigrator(sqlite_path=path)
                return_schema.append(await migrator.migrate_carga())
                return_schema.append(
                    await migrator.migrate_proveedores()
                )  # NO ES LO CORRECTO, debería usar Listado Proveedores SGF

            except ValidationError as e:
                logger.error(f"Validation Error: {e}")
                raise HTTPException(status_code=400, detail="Invalid response format")
            except Exception as e:
                logger.error(f"Error during report processing: {e}")
                raise HTTPException(
                    status_code=401,
                    detail="Invalid credentials or unable to authenticate",
                )
            finally:
                try:
                    await logout(connect=connect_siif)
                except Exception as e:
                    logger.warning(f"Logout falló o browser ya cerrado: {e}")
                return return_schema

    # -------------------------------------------------
    async def compute_all(
        self, params: ControlCompletoParams
    ) -> List[RouteReturnSchema]:
        """
        Compute all controls for the given params.
        """
        return_schema = []
        try:
            # 🔹 Control Recursos
            partial_schema = await self.control_recursos_service.compute_all(
                params=params
            )
            return_schema.extend(partial_schema)

            # 🔹 Aporte Empresario - 3% INVICO
            partial_schema = await self.control_aporte_empresario_service.compute_all(
                params=params
            )
            return_schema.extend(partial_schema)

            # 🔹 Control Icaro vs SIIF
            partial_schema = await self.control_icaro_vs_siif_service.compute_all(
                params=params
            )
            return_schema.extend(partial_schema)

            # 🔹 Control Obras
            partial_schema = await self.control_obras_service.compute_all(params=params)
            return_schema.extend(partial_schema)

            # 🔹 Control Haberes
            partial_schema = await self.control_haberes_service.compute_all(
                params=params
            )
            return_schema.extend(partial_schema)

            # 🔹 Control Honorarios
            partial_schema = await self.control_honorarios_service.compute_all(
                params=params
            )
            return_schema.extend(partial_schema)

            # 🔹 Control Debitos Bancarios
            partial_schema = await self.control_debitos_bancarios_service.compute_all(
                params=params
            )
            return_schema.extend(partial_schema)

            # 🔹 Control Escribanos
            partial_schema = await self.control_escribanos_service.compute_all(
                params=params
            )
            return_schema.extend(partial_schema)

            # 🔹 Control Viaticos
            partial_schema = await self.control_viaticos_service.compute_all(
                params=params
            )
            return_schema.extend(partial_schema)

            # 🔹 Control Banco
            partial_schema = await self.control_banco_service.compute_all(params=params)
            return_schema.extend(partial_schema)

        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid response format from Control de Banco",
            )
        except Exception as e:
            logger.error(f"Error in compute_all: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error in compute_all",
            )
        finally:
            return return_schema

    # -------------------------------------------------
    async def export_all_from_db_to_google(
        self,
        params: ControlCompletoParams = None,
    ) -> List[GoogleExportResponse]:
        """Exports all control dataframes to Google Sheets.

        Args:
            params (ControlCompletoParams, optional): Parameters for filtering data. Defaults to None.

        Returns:
            dict: Summary of the export operation.
        """
        return_schema = []
        try:
            # 🔹 Control Banco
            partial_schema = (
                await self.control_banco_service.export_all_from_db_to_google(
                    params=params
                )
            )
            return_schema.append(partial_schema)

            # 🔹 Aporte Empresario - 3% INVICO
            partial_schema = await self.control_aporte_empresario_service.export_all_from_db_to_google(
                params=params
            )
            return_schema.append(partial_schema)

            # 🔹 Debitos Bancarios
            partial_schema = await self.control_debitos_bancarios_service.export_all_from_db_to_google(
                params=params
            )
            return_schema.append(partial_schema)

            # 🔹 Escribanos
            partial_schema = (
                await self.control_escribanos_service.export_all_from_db_to_google(
                    params=params
                )
            )
            return_schema.append(partial_schema)

            # 🔹 Haberes
            partial_schema = (
                await self.control_haberes_service.export_all_from_db_to_google(
                    params=params
                )
            )
            return_schema.append(partial_schema)

            # 🔹 Honorarios
            partial_schema = (
                await self.control_honorarios_service.export_all_from_db_to_google(
                    params=params
                )
            )
            return_schema.append(partial_schema)

            # 🔹 Icaro vs SIIF
            # partial_schema = await self.control_icaro_vs_siif_service.export_all_from_db_to_google(
            #     params=params
            # )
            # return_schema.append(partial_schema)

            # 🔹 Obras
            partial_schema = (
                await self.control_obras_service.export_all_from_db_to_google(
                    params=params
                )
            )
            return_schema.append(partial_schema)

            # 🔹 Recursos
            partial_schema = (
                await self.control_recursos_service.export_all_from_db_to_google(
                    params=params
                )
            )
            return_schema.append(partial_schema)

        except Exception as e:
            logger.error(f"Error in export_all_from_db_to_google: {e}")
            raise HTTPException(
                status_code=500,
                detail="Error in export_all_from_db_to_google",
            )
        finally:
            return return_schema


ControlCompletoServiceDependency = Annotated[ControlCompletoService, Depends()]
