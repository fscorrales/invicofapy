#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Icaro vs SIIF budget execution
Data required:
    - Icaro
    - SIIF rf602
    - SIIF rf610
    - SIIF gto_rpa03g
    - SIIF rcg01_uejp
    - SIIF rfondo07tp
    - SSCC ctas_ctes (manual data)
"""


import datetime as dt
import os
from dataclasses import dataclass

import numpy as np
import pandas as pd
from fastapi import Depends, HTTPException
from ...siif.repositories import Rf602RepositoryDependency
from ...siif.handlers import Rf602
from ...icaro.repositories import CargaRepositoryDependency


# --------------------------------------------------
@dataclass
class IcaroVsSIIFService():
    siif_rf602_repo: Rf602RepositoryDependency
    icaro_carga_repo: CargaRepositoryDependency
    siif_rf602_handler: Rf602 = field(init=False)  # No se pasa como argumento
    # update_db:bool = False

    # --------------------------------------------------
    # def __post_init__(self):
    #     if self.db_path == None:
    #         self.get_db_path()
    #     self.import_dfs()

    # --------------------------------------------------
    def update_sql_db(self):
        pass
        # if self.input_path == None:
        #     update_path_input = self.get_update_path_input()
        # else:
        #     update_path_input = self.input_path
        
        # update_siif = update_db.UpdateSIIF(
        #     update_path_input + '/Reportes SIIF', 
        #     self.db_path + '/siif.sqlite')
        # update_siif.update_ppto_gtos_fte_rf602()
        # update_siif.update_comprobantes_gtos_gpo_part_gto_rpa03g()
        # update_siif.update_comprobantes_gtos_rcg01_uejp()
        # update_siif.update_resumen_fdos_rfondo07tp()

        # update_sscc = update_db.UpdateSSCC(
        #     update_path_input + '/Sistema de Seguimiento de Cuentas Corrientes', 
        #     self.db_path + '/sscc.sqlite')
        # update_sscc.update_ctas_ctes()

        # update_icaro = update_db.UpdateIcaro(
        #     os.path.dirname(os.path.dirname(self.db_path))
        #     + '/R Output/SQLite Files/ICARO.sqlite', 
        #     self.db_path + '/icaro.sqlite')
        # update_icaro.migrate_icaro()

    # # --------------------------------------------------
    # def import_dfs(self):
    #     self.import_ctas_ctes()
    #     self.siif_desc_pres = self.import_siif_desc_pres(ejercicio_to=self.ejercicio)
    #     self.import_icaro_carga(self.ejercicio)
    #     self.import_siif_rfondo07tp_pa6(self.ejercicio)
    #     self.import_siif_rf602()
    #     self.import_siif_comprobantes()

    # # --------------------------------------------------
    # def import_siif_rf602(self):
    #     df = super().import_siif_rf602(self.ejercicio)
    #     df = df.loc[
    #         (df['partida'].isin(['421', '422'])) | 
    #         (df['estructura'] == '01-00-00-03-354')
    #     ]
    #     return df

    # # --------------------------------------------------
    # def import_siif_comprobantes(self):
    #     df = super().import_siif_comprobantes(self.ejercicio)
    #     df = df.loc[
    #         (df['partida'].isin(['421', '422'])) |
    #         ((df['partida'] == '354') & (~df['cuit'].isin([
    #             '30500049460', '30632351514', '20231243527'
    #         ])))
    #     ]
    #     return df

    # --------------------------------------------------
    async def control_ejecucion_anual(self):
        group_by = ['ejercicio','estructura', 'fuente']
        icaro = self.icaro_carga_repo.find_by_filter(
            filters={
                "tipo__ne": "PA6",
            }
        )
        icaro['estructura'] = icaro.actividad + '-' + icaro.partida
        icaro = icaro.groupby(group_by)['importe'].sum()
        icaro = icaro.reset_index(drop=False)
        icaro = icaro.rename(columns={'importe':'ejecucion_icaro'})
        siif = self.siif_rf602_repo().find_by_filter(
            filters={
                    "$or": [
                        {"nro_partida": {"$in": ["421", "422"]}},
                        {
                            "$and": [
                                {"partida": "354"},
                                {"CUIT": {"$nin": ["30500049460", "30632351514", "20231243527"]}}
                            ]
                        }
                    ]
                }
        )
        siif = siif.loc[:, group_by + ['ordenado']]
        siif = siif.rename(columns={'ordenado':'ejecucion_siif'})
        df = pd.merge(siif, icaro, how='outer', on = group_by, copy=False)
        df = df.fillna(0)
        df['diferencia'] = df['ejecucion_siif'] - df['ejecucion_icaro']

        #NECESITO EL RF610 antes de seguir
        df = df.merge(self.siif_desc_pres, how='left', on='estructura', copy=False)
        df = df.loc[(df['diferencia'] < -0.1) | (df['diferencia'] > 0.1)]
        df = df.reset_index(drop=True)
        return df

    # # --------------------------------------------------
    # def control_comprobantes(self):
    #     select = [
    #         'ejercicio', 'nro_comprobante', 'fuente', 'importe',
    #         'mes', 'cta_cte', 'cuit', 'partida'
    #     ]
    #     siif = self.import_siif_comprobantes().copy()
    #     # En ICARO limito los REG para regularizaciones de PA6
    #     siif.loc[(siif.clase_reg == 'REG') & (siif.nro_fondo.isnull()), 'clase_reg'] = 'CYO'
    #     siif = siif.loc[:, select + ['clase_reg']]
    #     siif = siif.rename(columns={
    #             'nro_comprobante':'siif_nro',
    #             'clase_reg':'siif_tipo',
    #             'fuente':'siif_fuente',
    #             'importe':'siif_importe',
    #             'mes':'siif_mes',
    #             'cta_cte':'siif_cta_cte',
    #             'cuit':'siif_cuit',
    #             'partida':'siif_partida'
    #     })
    #     icaro = self.icaro_carga.copy()
    #     icaro = icaro.loc[:, select + ['tipo']]
    #     icaro = icaro.loc[icaro['tipo'] != 'PA6']
    #     icaro = icaro.rename(columns={
    #             'nro_comprobante':'icaro_nro',
    #             'tipo':'icaro_tipo',
    #             'fuente':'icaro_fuente',
    #             'importe':'icaro_importe',
    #             'mes':'icaro_mes',
    #             'cta_cte':'icaro_cta_cte',
    #             'cuit':'icaro_cuit',
    #             'partida':'icaro_partida'
    #     })
    #     df = pd.merge(
    #         siif, icaro, how='outer', 
    #         left_on = ['ejercicio', 'siif_nro'], 
    #         right_on = ['ejercicio', 'icaro_nro']
    #     )
    #     df['err_nro'] = df.siif_nro != df.icaro_nro
    #     df['err_tipo'] = df.siif_tipo != df.icaro_tipo
    #     df['err_mes'] = df.siif_mes != df.icaro_mes
    #     df['err_partida'] = df.siif_partida != df.icaro_partida
    #     df['err_fuente'] = df.siif_fuente != df.icaro_fuente
    #     df['siif_importe'] = df['siif_importe'].fillna(0)
    #     df['icaro_importe'] = df['icaro_importe'].fillna(0)
    #     df['err_importe'] = (df.siif_importe - df.icaro_importe).abs()
    #     df['err_importe'] = (df['err_importe'] > 0.1)
    #     df['err_cta_cte'] = df.siif_cta_cte != df.icaro_cta_cte
    #     df['err_cuit'] = df.siif_cuit != df.icaro_cuit
    #     df = df.loc[(
    #         df.err_nro + df.err_tipo + df.err_mes + df.err_partida + 
    #         df.err_fuente + df.err_importe + df.err_cta_cte + df.err_cuit
    #     ) > 0]
    #     df = df.loc[:, ['ejercicio',
    #         'siif_nro', 'icaro_nro', 'err_nro',
    #         'siif_tipo', 'icaro_tipo', 'err_tipo',
    #         'siif_fuente', 'icaro_fuente', 'err_fuente',
    #         'siif_importe', 'icaro_importe', 'err_importe',
    #         'siif_mes', 'icaro_mes', 'err_mes',
    #         'siif_cta_cte', 'icaro_cta_cte', 'err_cta_cte',
    #         'siif_cuit', 'icaro_cuit', 'err_cuit',
    #         'siif_partida', 'icaro_partida', 'err_partida']
    #     ]
    #     # comprobantes.sort_values(
    #     #     by=['err_nro', 'err_fuente', 'err_importe', 
    #     #     'err_cta_cte', 'err_cuit', 'err_partida', 
    #     #     'err_mes'], ascending=False,
    #     #     inplace=True)

    #     return df

    # # --------------------------------------------------
    # def control_pa6(self):
    #     siif_fdos = self.siif_rfondo07tp.copy()
    #     siif_fdos = siif_fdos.loc[
    #         :, ['ejercicio', 'nro_fondo', 'mes', 'ingresos', 'saldo']
    #     ]
    #     siif_fdos['nro_fondo'] = siif_fdos['nro_fondo'].str.zfill(5) + '/' + siif_fdos.ejercicio.str[-2:]
    #     siif_fdos = siif_fdos.rename(columns={
    #         'nro_fondo':'siif_nro_fondo', 
    #         'mes':'siif_mes_pa6', 
    #         'ingresos':'siif_importe_pa6', 
    #         'saldo':'siif_saldo_pa6'
    #     })
        
    #     select = [
    #         'ejercicio','nro_comprobante', 'fuente', 'importe',
    #         'mes', 'cta_cte', 'cuit'
    #     ]

    #     siif_gtos = self.import_siif_comprobantes().copy()
    #     siif_gtos = siif_gtos.loc[siif_gtos['clase_reg'] == 'REG']
    #     siif_gtos = siif_gtos.loc[:, select + ['nro_fondo', 'clase_reg']]
    #     siif_gtos['nro_fondo'] = siif_gtos['nro_fondo'].str.zfill(5) + '/' + siif_gtos.ejercicio.str[-2:]
    #     siif_gtos = siif_gtos.rename(columns={
    #             'nro_fondo':'siif_nro_fondo',
    #             'cta_cte':'siif_cta_cte', 
    #             'cuit':'siif_cuit',
    #             'clase_reg':'siif_tipo', 
    #             'fuente':'siif_fuente', 
    #             'nro_comprobante':'siif_nro_reg', 
    #             'importe':'siif_importe_reg', 
    #             'mes':'siif_mes_reg',
    #     })
        
    #     icaro = self.icaro_carga.copy()
    #     icaro = icaro.loc[:, select + ['tipo']]
    #     icaro = icaro.rename(columns={
    #             'mes':'icaro_mes',
    #             'nro_comprobante':'icaro_nro', 
    #             'tipo':'icaro_tipo', 
    #             'importe':'icaro_importe',
    #             'cuit':'icaro_cuit',
    #             'cta_cte':'icaro_cta_cte',
    #             'fuente':'icaro_fuente'
    #     })
        
    #     icaro_pa6 = icaro.loc[icaro['icaro_tipo'] == 'PA6']
    #     icaro_pa6 = icaro_pa6.loc[:, ['icaro_mes', 'icaro_nro', 'icaro_importe']]
    #     icaro_pa6 = icaro_pa6.rename(columns={
    #             'icaro_mes':'icaro_mes_pa6',
    #             'icaro_nro':'icaro_nro_fondo', 
    #             'icaro_importe':'icaro_importe_pa6',
    #     })

    #     icaro_reg = icaro.loc[icaro['icaro_tipo'] != 'PA6']
    #     icaro_reg = icaro_reg.rename(columns={
    #             'icaro_mes':'icaro_mes_reg',
    #             'icaro_nro':'icaro_nro_reg', 
    #             'icaro_importe':'icaro_importe_reg',
    #     })

    #     df = pd.merge(
    #         siif_fdos, siif_gtos, how='left', 
    #         on=['ejercicio','siif_nro_fondo'], copy=False
    #     )
    #     df = pd.merge(
    #         df, icaro_pa6, how='outer', 
    #         left_on = 'siif_nro_fondo', 
    #         right_on = 'icaro_nro_fondo'
    #     )
    #     df = pd.merge(
    #         df, icaro_reg, how='left', 
    #         left_on = ['ejercicio', 'siif_nro_reg'], 
    #         right_on = ['ejercicio', 'icaro_nro_reg']
    #     )
    #     df = df.fillna(0)
    #     df['err_nro_fondo'] = df.siif_nro_fondo != df.icaro_nro_fondo
    #     df['err_mes_pa6'] = df.siif_mes_pa6 != df.icaro_mes_pa6
    #     df['siif_importe_pa6'] = df['siif_importe_pa6'].fillna(0)
    #     df['icaro_importe_pa6'] = df['icaro_importe_pa6'].fillna(0)
    #     df['err_importe_pa6'] = (df.siif_importe_pa6 - df.icaro_importe_pa6).abs()
    #     df['err_importe_pa6'] = (df['err_importe_pa6'] > 0.1)
    #     # df['err_importe_pa6'] = ~np.isclose((df.siif_importe_pa6 - df.icaro_importe_pa6), 0)
    #     df['err_nro_reg'] = df.siif_nro_reg != df.icaro_nro_reg
    #     df['err_mes_reg'] = df.siif_mes_reg != df.icaro_mes_reg
    #     df['siif_importe_reg'] = df['siif_importe_reg'].fillna(0)
    #     df['icaro_importe_reg'] = df['icaro_importe_reg'].fillna(0)
    #     df['err_importe_reg'] = (df.siif_importe_reg - df.icaro_importe_reg).abs()
    #     df['err_importe_reg'] = (df['err_importe_reg'] > 0.1)        
    #     #df['err_importe_reg'] = ~np.isclose((df.siif_importe_reg - df.icaro_importe_reg), 0)
    #     df['err_tipo'] = df.siif_tipo != df.icaro_tipo
    #     df['err_fuente'] = df.siif_fuente != df.icaro_fuente
    #     df['err_cta_cte'] = df.siif_cta_cte != df.icaro_cta_cte
    #     df['err_cuit'] = df.siif_cuit != df.icaro_cuit
    #     df = df.loc[:, ['ejercicio',
    #         'siif_nro_fondo', 'icaro_nro_fondo', 'err_nro_fondo',
    #         'siif_mes_pa6', 'icaro_mes_pa6', 'err_mes_pa6',
    #         'siif_importe_pa6', 'icaro_importe_pa6', 'err_importe_pa6',
    #         'siif_nro_reg', 'icaro_nro_reg', 'err_nro_reg',
    #         'siif_mes_reg', 'icaro_mes_reg', 'err_mes_reg',
    #         'siif_importe_reg', 'icaro_importe_reg', 'err_importe_reg',
    #         'siif_tipo', 'icaro_tipo', 'err_tipo',
    #         'siif_fuente', 'icaro_fuente', 'err_fuente',
    #         'siif_cta_cte', 'icaro_cta_cte', 'err_cta_cte',
    #         'siif_cuit', 'icaro_cuit', 'err_cuit'
    #     ]]
    #     df = df.loc[(
    #         df.err_nro_fondo + df.err_mes_pa6 + df.err_importe_pa6 + 
    #         df.err_nro_reg + df.err_mes_reg + df.err_importe_reg + 
    #         df.err_fuente + df.err_tipo + df.err_cta_cte +
    #         df.err_cuit
    #     ) > 0]
    #     df = df.sort_values(
    #         by=['err_nro_fondo','err_importe_pa6', 
    #         'err_nro_reg', 'err_importe_reg', 
    #         'err_fuente', 'err_cta_cte', 'err_cuit', 
    #         'err_tipo', 'err_mes_pa6', 'err_mes_reg'], 
    #         ascending=False
    #     )
    #     return df

IcaroVsSIIFServiceDependency = Annotated[IcaroVsSIIFService, Depends()]