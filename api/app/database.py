from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# PUSE SQLITE POR PONER ALGO
DATABASE_URL = "sqlite:///./data/mcp.db"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False}  # Necesario solo para SQLite
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# Base para los modelos SQLAlchemy
Base = declarative_base()
