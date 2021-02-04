import typing

import requests
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datcat.domain import model as dc_model
from catasyn.domain import model as cs_model
from urllib.parse import parse_qs, urlunsplit

from catasyn.settings import DATCAT_SCHEME, DATCAT_HOST, DATCAT_PORT
from catasyn.settings import CLOUD_PROJECT_ID, LOCATION, GOOGLE_APPLICATION_CREDENTIALS
from google.oauth2 import service_account


# TODO: break out to separate module once we have more custom exceptions
class UnexpectedSchema(Exception):
    pass


class Synchroniser:
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
        table = bigquery.Table(self.table_id, schema=self.schema_from_catalogue)
        self.client.create_table(table)
        return self.table_exists

    @property
    def schema_from_catalogue(self) -> dc_model.SchemaDefinition:
        """
        TODO: This needs to be integration tested
        :return:
            Schema definition in json. E.g see tests/schema/schema_one_v1.json
        """
        # TODO: put this somewhere else
        query: typing.Dict = parse_qs("")
        query["schema_name"] = self.table_name
        query["schema_version"] = self.table_version
        query["refresh"] = True

        url = urlunsplit((DATCAT_SCHEME, f"{DATCAT_HOST}:{DATCAT_PORT}", "/schemas/search_by_key", "", ""))
        response = requests.get(url=url, params=query)

        response.raise_for_status()

        return response.json()

    @property
    def schema_from_dataset(self) -> dc_model.SchemaDefinition:
        # TODO: CONTINUE THIS!
        schema: dc_model.SchemaDefinition = []
        field: dc_model.SchemaField = {}
        table = bigquery.Table(self.table_id, schema=self.schema_from_catalogue)
        for field in table.to_api_repr()["schema"]["fields"]:
            sorted_fields = dict(sorted(field.items(), key=lambda k: k[0]))
            schema.append(sorted_fields)
        return schema

    @property
    def schema_is_identical(self) -> bool:
        return self.schema_from_catalogue == self.schema_from_dataset

    def synchronise(self) -> bool:
        """
        :param table_exists:
        :return:
            error: if the table exists and the target schema is different
            pass: if the table exists and the target schema is identical
            create: if the table does not exist
        """
        is_created: bool = False
        if not self.table_exists:
            is_created = self.create_table()
        elif not self.schema_is_identical:
            raise UnexpectedSchema(f"The catalogue/dataset schemas of {self.table_id} are not the same!")
        else:
            pass
        return is_created

    @property
    def table_exists(self) -> bool:
        """
        :param table_id:
            Expected id format is "project_id.dataset_id.table_name"
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
