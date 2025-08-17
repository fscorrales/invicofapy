__all__ = [
    "EjercicioSIIF",
    "TipoComprobanteSIIF",
    "GrupoPartidaSIIF",
    "GrupoControlFinancieroSIIF",
    "FuenteFinanciamientoSIIF",
]

from datetime import date
from enum import Enum

from pydantic import BaseModel, Field, field_validator


# -------------------------------------------------
class EjercicioSIIF(BaseModel):
    """
    Representa el año fiscal del SIIF. Debe ser un año entre 2010 y el actual.
    """

    value: int = Field(
        default_factory=lambda: date.today().year,
        alias="ejercicio",
        description="Año del ejercicio fiscal (entre 2010 y el año actual)",
        example=2025,
    )

    @field_validator("value")
    @classmethod
    def validate_value(cls, v):
        current_year = date.today().year
        if not (2010 <= v <= current_year):
            raise ValueError(f"Ejercicio debe estar entre 2010 y {current_year}")
        return v

    def __int__(self):
        return self.value


# -------------------------------------------------
class PartidaPrincipalSIIF(str, Enum):
    """
    Enum para representar las Partidas Principales del SIIF.
    """

    gastos_en_personal = "100"
    bienes_de_consumo = "200"
    servicios_no_personales = "300"
    bienes_de_uso = "400"


# -------------------------------------------------
class GrupoPartidaSIIF(int, Enum):
    """
    Enum para representar los grupos de partidas del SIIF.
    """

    sueldos = 1
    bienes_consumo = 2
    servicios = 3
    bienes_capital = 4


# -------------------------------------------------
class TipoComprobanteSIIF(str, Enum):
    adelanto_contratista = "PA6"
    anticipo_viatico = "PA3"


# -------------------------------------------------
class GrupoControlFinancieroSIIF(str, Enum):
    """
    Enum para representar los Grupos de Control Financiero del SIIF.
    """

    gastos_de_personal = "1"  # CONTROL DEL GRUPO DE GASTOS 100
    bienes_serv_inversion = "2"  # CONTROL GRUPO DE GASTOS 200, 300, 400
    transf_act_fin_deuda_publica = "3"  # CONTROL GRUPO DE GASTOS: 500, 600, 700
    gastos_figurativos = "4"  # CONTROL GRUPO DE GASTOS: 900
    gpo_gral_sin_control = "9"  # GRUPO SIN CONTROL FINANCIERO : GRUPO: 800


# -------------------------------------------------
class FuenteFinanciamientoSIIF(str, Enum):
    """
    Enum para representar las fuentes de financiamiento del SIIF.
    """

    recursos_tesoro_gral_prov = "10"
    recursos_propios = "11"
    financiamiento_interno = "12"
    transf_nac_con_afect_especifica = "13"
    transf_prov_con_afect_especifica = "14"
    transf_ext_con_afect_especifica = "15"
