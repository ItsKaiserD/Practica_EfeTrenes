from datetime import date
from typing import Optional

from pydantic import BaseModel, Field

class Indicador(BaseModel):
    fecha: date
    filial_code: str = Field(min_length=1)
    servicio_code: str = Field(min_length=1)
    tipo_indicador: str
    valor: float
    fuente: str
    tramo_id: Optional[str] = Field(default=None, min_length=1)
