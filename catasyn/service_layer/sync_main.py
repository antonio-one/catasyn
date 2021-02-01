import json
import typing

import requests
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datcat.domain import model as cat_model
from catasyn.domain import model as sync_model
from catasyn.helpers import catalogue_url


# TODO: break out to separate module once we have more custom exceptions
class UnexpectedSchema(Exception):
    pass


class Synchroniser:
    def __init__(self, table_id: sync_model.TableId):
        # TODO: make this a table_or_tables: typing.Union[sync_model.TableId, sync_model.ListOfTables]
        self.client = bigquery.Client()
        self.table_id = table_id

    def create_table(
        self, table_id: str, schema_definition: cat_model.SchemaDefinition
    ) -> None:
        # TODO: make this create_table_or_tables
        """
        :param table_id:
            Expected id format is "project_id.dataset_id.table_name"
        :param schema_definition:
            Schema definition in json. E.g see tests/schema/schema_one_v1.json
        :return:
            None
        :links
            https://cloud.google.com/bigquery/docs/tables#python
        """
        schema = json.loads(schema_definition)
        table = bigquery.Table(table_ref=table_id, schema=schema)
        table = self.client.create_table(table)

    @property
    def schema_from_catalogue(
        self, schema_format: cat_model.SchemaFormat
    ) -> cat_model.SchemaDefinition:
        """
        TODO: This needs to be integration tested
        :param schema_format:
            See SchemaFormat in data_catalogue/domain/model
        :return:
            Schema definition in json. E.g see tests/schema/schema_one_v1.json
        """
        response = requests.get(
            catalogue_url(route="search_by_key", refresh=True),
            params=schema_format.params,
        )
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise e

        return response.json()

    @property
    def schema_from_dataset(self) -> cat_model.SchemaDefinition:
        schema: cat_model.SchemaDefinition = []
        field: cat_model.SchemaField = {}
        for field in self.table.to_api_repr()["schema"]["fields"]:
            schema.append(dict(field.items(), key=lambda k: k[0]))
        return json.dumps(schema, separators=(",", ":"), indent=None)

    @property
    def schema_is_identical(self):
        # TODO: Integration test pending
        return self.schema_from_catalogue == self.schema_from_dataset

    @property
    def sync_type(self) -> str:
        """
        :param table_exists:
        :return:
            error: if the table exists and the target schema is different
            pass: if the table exists and the target schema is identical
            create: if the table does not exist
        """
        action: str = "create"
        if self.table_exists:
            if not self.schema_is_identical:
                action = "error"
            else:
                action = "pass"
        return action

    @property
    def table(self):
        """
        :param table_id:
            Expected id format is "project_id.dataset_id.table_name"
        :return:
            True if table exists | False if table does not exist
        :see
            https://cloud.google.com/bigquery/docs/samples/bigquery-get-table#bigquery_get_table-python
            https://cloud.google.com/bigquery/docs/samples/bigquery-table-exists
        """
        table: typing.Any = None
        try:
            table = self.client.get_table(self.table_id)
        except NotFound:
            pass
        return table
