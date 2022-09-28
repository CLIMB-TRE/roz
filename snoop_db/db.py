from sqlmodel import SQLModel, create_engine
from .models import (
    new_file_table,
    matched_triplet_table,
    validation_result_table,
    new_artifact_table,
)
import os


def make_engine():
    DB_ENV_VAR = "SNOOP_DB_URL"
    DB_URL = os.getenv(DB_ENV_VAR)
    if not DB_URL:
        raise Exception(f"Environment variable '{DB_ENV_VAR}' was not found.")
    engine = create_engine(DB_URL)
    return engine


def make_db_and_tables(engine):
    # Takes engine, creates database and tables that have been registered in the MetaData class
    # A table is registered in the MetaData class if table=True
    SQLModel.metadata.create_all(engine)

def main():
    engine = make_engine()
    make_db_and_tables(engine)
    yield engine

if __name__ == "__main__":
    main()
