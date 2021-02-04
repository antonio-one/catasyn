import requests
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datcat.domain import model as dc_model
from catasyn.domain import model as cs_model
from urllib.parse import parse_qs, urlunsplit
import logging

from catasyn.settings import (
    DATCAT_SCHEME, DATCAT_HOST, DATCAT_PORT,
    CLOUD_PROJECT_ID, LOCATION, GOOGLE_APPLICATION_CREDENTIALS,
    SCHEDULER_LOG_FILENAME
)
from google.oauth2 import service_account

FORMAT = "%(asctime)s %(levelname)s %(message)s"
filename = SCHEDULER_LOG_FILENAME
logging.basicConfig(filename=filename, level=logging.INFO, format=FORMAT)

DATCAT_NETLOC = f"{DATCAT_HOST}:{DATCAT_PORT}"


# TODO: break out to separate module once we have more custom exceptions
class UnexpectedSchema(Exception):
    pass


class TableSynchroniser:
    """
    The logic of the schema/table synchronisation is the following:
    If the table does not exist then create it
    If the table exists check the schema of the catalogue is identical to the schema of the table
    If the schema is different raise an error
    If it's the same do nothing
    TODO: maybe save the state somewhere so table existence does not have to be checked all the time.
    """
    def __init__(self, table_id: cs_model.TableId):
        # TODO: make this a table_or_tables: typing.Union[cs_model.TableId, cs_model.ListOfTables]
        self.credentials = service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS)
        self.client = bigquery.Client(project=CLOUD_PROJECT_ID, credentials=self.credentials, location=LOCATION)
        self.table_id = table_id

    def create_table(self) -> bool:
        # TODO: make this create_table_or_tables
        """
        :links
            https://cloud.google.com/bigquery/docs/tables#python
        """
        table = bigquery.Table(self.table_id, schema=self.schema_from_catalogue())
        self.client.create_table(table)
        return self.table_exists

    def schema_from_catalogue(self) -> dc_model.SchemaDefinition:
        """
        TODO: This needs to be integration tested
        :return:
            Schema definition in json. E.g see tests/schema/schema_one_v1.json
        """
        url = urlunsplit((DATCAT_SCHEME, DATCAT_NETLOC, "/schemas/search_by_key", "", ""))
        params = {"schema_name": self.table_name, "schema_version": self.table_version, "refresh": True}
        response = requests.get(url=url, params=params)

        response.raise_for_status()

        return response.json()

    def schema_from_dataset(self) -> dc_model.SchemaDefinition:
        # TODO: CONTINUE THIS!
        schema: dc_model.SchemaDefinition = []
        # field: dc_model.SchemaField = {}
        table = bigquery.Table(self.table_id, schema=self.schema_from_catalogue())
        for field in table.to_api_repr()["schema"]["fields"]:
            sorted_fields = dict(sorted(field.items(), key=lambda k: k[0]))
            schema.append(sorted_fields)
        return schema

    @property
    def schema_is_identical(self) -> bool:
        return self.schema_from_catalogue() == self.schema_from_dataset()

    def synchronise(self) -> bool:
        """
        :return:
            error: if the table exists and the target schema is different
            pass: if the table exists and the target schema is identical
            create: if the table does not exist
        """
        is_created: bool = False
        if not self.table_exists:
            is_created = self.create_table()
        elif not self.schema_is_identical:
            message: str = f"The catalogue/dataset schemas of {self.table_id} are not the same!"
            logging.error(message)
        else:
            pass
        return is_created

    @property
    def table_exists(self) -> bool:
        """
        :return:
            True if table exists | False if table does not exist
        :see
            https://cloud.google.com/bigquery/docs/samples/bigquery-get-table#bigquery_get_table-python
            https://cloud.google.com/bigquery/docs/samples/bigquery-table-exists
        """
        try:
            self.client.get_table(self.table_id)
        except NotFound:
            return False
        return True

    @property
    def table_name(self) -> str:
        tn: str
        tn = self.table_id.rpartition("_")[0]
        tn = tn.rpartition(".")[-1]
        return tn

    @property
    def table_name_version(self) -> str:
        tnv: str
        tnv = self.table_id.rpartition(".")[-1]
        return tnv

    @property
    def table_version(self) -> int:
        tv: str
        tv = self.table_id.rpartition("_")[-1]
        tv = tv.rpartition(".")[-1]
        tv = tv.replace("v", "")
        return int(tv)


class TopicSynchroniser:
    """
    This synchronises topics and subscribers
    """
    def __init__(self):
        self.credentials = service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS)
        self.client = bigquery.Client(project=CLOUD_PROJECT_ID, credentials=self.credentials, location=LOCATION)

    def create_topic(self):
        pass

    def check_topic_exists(self, collection_type: str) -> bool:
        pass

    def topic_config(self):
        pass

    @property
    def topic_name(self):
        pass
