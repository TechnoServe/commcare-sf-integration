import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

DATABASE_URL = os.getenv("PG_PROD_DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not set in environment")

# SQLAlchemy engine and session
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)

# Import models here to register tables
from .models import Base
from utils.postgres import engine
# from utils.models import Base

# Base.metadata.create_all(engine)


def init_db():
    pass
    # for table in reversed(Base.metadata.sorted_tables):
    #     table.drop(engine, checkfirst=True)

    # Base.metadata.create_all(bind=engine)
