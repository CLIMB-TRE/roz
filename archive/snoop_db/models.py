from sqlmodel import Field, SQLModel
import sqlalchemy.types as types


class inbound_s3_table(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)

    bucket: str = Field(index=True)

    uploader: str = Field(index=True)

    payload: str = Field()


class inbound_matched_table(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)

    uuid: str = Field(index=True)

    timestamp: int = Field(index=True)

    site: str = Field(index=True)

    project: str = Field(index=True)

    uploaders: list = Field(index=True)

    platform: str = Field(index=True)

    artifact: str = Field(index=True)

    sample_id: str = Field(index=True)

    run_name: str = Field(index=True)

    files: str = Field()

    local_paths: str = Field()

    payload: str = Field()


class inbound_to_validate_table(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)

    uuid: int = Field(index=True)

    timestamp: int = Field(index=True)

    site: str = Field(index=True)

    project: str = Field(index=True)

    platform: str = Field(index=True)

    artifact: str = Field(index=True)

    sample_id: str = Field(index=True)

    run_name: str = Field(index=True)

    files: str = Field()

    local_paths: str = Field()

    onyx_test_status_code: int = Field(index=True)

    onyx_test_errors: str = Field()

    onyx_test_status: bool = Field(index=True)

    payload: str = Field()


class inbound_validated_table(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)

    uuid: int = Field(index=True)

    timestamp: int = Field(index=True)

    site: str = Field(index=True)

    project: str = Field(index=True)

    platform: str = Field(index=True)

    artifact: str = Field(index=True)

    sample_id: str = Field(index=True)

    run_name: str = Field(index=True)

    files: str = Field()

    local_paths: str = Field()

    onyx_status_code: int = Field(index=True)

    onyx_errors: str = Field()

    onyx_status: bool = Field(index=True)

    ingest_status: bool = Field(index=True)

    ingest_errors: str = Field()

    payload: str = Field()


class inbound_artifacts_table(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)

    uuid: str = Field(index=True)

    cid: str = Field(index=True)

    timestamp: int = Field(index=True)

    site_code: str = Field(index=True)

    pathogen_code: str = Field(index=True)

    artifact: str = Field(index=True)

    fasta_url: str = Field()

    bam_url: str = Field()

    payload: str = Field()
