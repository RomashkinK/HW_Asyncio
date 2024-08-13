import os

from sqlalchemy import JSON, Integer, String
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "swapi")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRS_PORT = os.getenv("POSTGRES_PORT", "5431")


PG_DSN = (
    f"postgresql+asyncpg://"
    f"{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
    f"{POSTGRES_HOST}:{POSTGRS_PORT}/"
    f"{POSTGRES_DB}"
)

engine = create_async_engine(PG_DSN)
Session = async_sessionmaker(bind=engine, expire_on_commit=False)


class Base(DeclarativeBase, AsyncAttrs):
    pass


class SwapiPeople(Base):

    __tablename__ = "swapi"

    id: Mapped[int] = mapped_column(primary_key=True)
    birth_year: Mapped[str] = mapped_column(String(50))
    eye_color: Mapped[str] = mapped_column(String(50))
    films: Mapped[str] = mapped_column(String(500))
    gender: Mapped[str] = mapped_column(String(100))
    hair_color: Mapped[str] = mapped_column(String(50))
    height: Mapped[int] = mapped_column(String(50))
    homeworld: Mapped[str] = mapped_column(String(200))
    mass: Mapped[int] = mapped_column(String(50))
    name: Mapped[str] = mapped_column(String(200))
    skin_color: Mapped[str] = mapped_column(String(50))
    species: Mapped[str] = mapped_column(String(500))
    starships: Mapped[str] = mapped_column(String(500))
    vehicles: Mapped[str] = mapped_column(String(500))


async def init_orm():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
