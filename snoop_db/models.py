from sqlmodel import Field, SQLModel
import sqlalchemy.types as types


class inbound_s3_table(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)

    payload: str = Field()


class inbound_matched_table(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)

    timestamp: int = Field(index=True)

    site_code: str = Field(index=True)

    pathogen_code: str = Field(index=True)

    csv_url: str = Field()

    csv_etag: str = Field()

    fasta_url: str = Field()

    fasta_etag: str = Field()

    bam_url: str = Field()

    bam_etag: str = Field()

    artifact: str = Field(index=True)

    payload: str = Field()


class inbound_validated_table(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)

    timestamp: int = Field(index=True)

    site_code: str = Field(index=True)

    pathogen_code: str = Field(index=True)

    artifact: str = Field(index=True)

    triplet_result: bool = Field(index=True)

    csv_result: bool = Field(index=True)

    csv_messages: str = Field()

    fasta_result: bool = Field(index=True)

    fasta_messages: str = Field()

    bam_result: bool = Field(index=True)

    bam_messages: str = Field()

    payload: str = Field()


class inbound_artifacts_table(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)

    cid: str = Field(index=True, unique=True)

    timestamp: int = Field(index=True)

    site_code: str = Field(index=True)

    pathogen_code: str = Field(index=True)

    artifact: str = Field(index=True)

    fasta_url: str = Field()

    bam_url: str = Field()

    payload: str = Field()
