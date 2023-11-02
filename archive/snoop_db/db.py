from sqlmodel import SQLModel, create_engine
from snoop_db.models import (
    inbound_s3_table,
    inbound_validated_table,
    inbound_artifacts_table,
    inbound_matched_table,
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


if __name__ == "__main__":
    main()
