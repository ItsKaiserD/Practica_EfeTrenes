from datetime import date
from typing import Optional
from pydantic import BaseModel, Field
from sqlalchemy import Column, Integer, String, Date, Float, TIMESTAMP, func
from .database import Base

class Indicador(BaseModel):
    fecha: date
    filial_code: str = Field(min_length=1)
    servicio_code: str = Field(min_length=1)
    tipo_indicador: str
    valor: float
    fuente: str
    tramo_id: Optional[str] = Field(default=None, min_length=1)

class IndicadorSchema(Base):
    __tablename__ = "indicadores"
    id = Column(Integer, primary_key=True, index=True)
    fecha = Column(Date, index=True)
    filial_code = Column(String, index=True)
    servicio_code = Column(String, index=True)
    tramo_id = Column(String, nullable=True, index=True)
    tipo_indicador = Column(String, index=True)
    valor = Column(Float)
    fuente = Column(String)
    ingest_timestamp = Column(TIMESTAMP, server_default=func.now(), nullable=False)
