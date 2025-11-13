from pydantic import BaseModel, Field
from datetime import date

class Indicador(BaseModel):
    fecha: date
    filial_code: str = Field(min_length=1)
    servicio_code: str = Field(min_length=1)
    tipo_indicador: str
    valor: float
    fuente: str
