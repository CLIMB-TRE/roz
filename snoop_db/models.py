from sqlmodel import Field, SQLModel
import datetime
import types


class EpochTime(types.TypeDecorator):
    impl = types.Integer

    epoch = datetime.date(1970, 1, 1)

    def process_bind_param(self, value, dialect):
        return (value - self.epoch).days

    def process_result_value(self, value, dialect):
        return self.epoch + datetime.timedelta(days=value)


class new_file_table(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)

    timestamp: EpochTime = Field(index=True)

    site_code: str = Field(index=True)

    pathogen_code: str = Field(index=True)

    payload: str = Field()


class matched_triplet_table(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)

    timestamp: EpochTime = Field(index=True)

    site_code: str = Field(index=True)

    pathogen_code: str = Field(index=True)

    csv_url: str = Field()

    csv_md5: str = Field()

    fasta_url: str = Field()

    fasta_md5: str = Field()

    bam_url: str = Field()

    bam_md5: str = Field()

    artifact: str = Field(index=True)

    payload: str = Field()


class validation_result_table(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)

    timestamp: EpochTime = Field(index=True)

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


class new_artifact_table(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)

    cid: str = Field(index=True, unique=True)

    timestamp: EpochTime = Field(index=True)

    site_code: str = Field(index=True)

    pathogen_code: str = Field(index=True)

    artifact: str = Field(index=True)

    csv_url: str = Field()

    csv_md5: str = Field()

    fasta_url: str = Field()

    fasta_md5: str = Field()

    bam_url: str = Field()

    bam_md5: str = Field()

    payload: str = Field()

    