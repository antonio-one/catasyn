import json
import logging
from os import environ
from urllib.parse import urlunsplit

import requests
from datcat.domain import model as dc_model
from google.cloud import bigquery, pubsub_v1
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from contextlib import suppress
from catasyn.domain import model as cs_model
from catasyn.settings import (
    CLOUD_PROJECT_ID,
    DATCAT_HOST,
    DATCAT_PORT,
    DATCAT_SCHEME,
    GOOGLE_APPLICATION_CREDENTIALS,
    LOCATION,
)

DATCAT_NETLOC = f"{DATCAT_HOST}:{DATCAT_PORT}"
PROJECT_PATH = f"projects/{CLOUD_PROJECT_ID}"

# pubsub clients are suspected to only read from the env variable
environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS


class UnexpectedSchema(Exception):
    pass


class TableSynchroniser():
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
        self.credentials = service_account.Credentials.from_service_account_file(
            GOOGLE_APPLICATION_CREDENTIALS
        )
        self.client = bigquery.Client(
            project=CLOUD_PROJECT_ID, credentials=self.credentials, location=LOCATION
        )
        self.table_id = table_id

    def create_table(self) -> bool:
        """
        :links
            https://cloud.google.com/bigquery/docs/tables#python
        """
        table = bigquery.Table(self.table_id, schema=self.schema_from_catalogue())
        if self.partition_column():
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=self.partition_column()
            )

        self.client.create_table(table)
        return self.table_exists

    def partition_column(self) -> str:
        for column in self.schema_from_catalogue():
            with suppress(json.decoder.JSONDecodeError, KeyError):
                cd = column["description"]
                cd = json.loads(cd)
                if cd["partition"] is True:
                    return column["name"]

    def schema_from_catalogue(self) -> dc_model.SchemaDefinition:
        """
        TODO: This needs to be integration tested
        :return:
            Schema definition in json. E.g see tests/schema/schema_one_v1.json
        """
        url = urlunsplit(
            (DATCAT_SCHEME, DATCAT_NETLOC, "/schemas/search_by_key", "", "")
        )
        params = {
            "schema_class_name": self.table_name,
            "schema_version": self.table_version,
            "refresh": True,
        }
        response = requests.get(url=url, params=params)
        response.raise_for_status()

        return response.json()

    def schema_from_dataset(self) -> dc_model.SchemaDefinition:
        # TODO: CONTINUE THIS!
        schema: dc_model.SchemaDefinition = []
        table = self.client.get_table(self.table_id)
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
        if not self.table_exists:
            message: str = f"`{self.table_id}` does not exist. Creating..."
            logging.info(message)
            return self.create_table()

        elif self.table_exists and not self.schema_is_identical:
            message: str = f"The catalogue and dataset schemas of {self.table_id} are not the same!"
            logging.error(message)
            return False

        else:
            message: str = f"`{self.table_id} exists. Schema is valid. Doing nothing."
            logging.info(message)
            return None

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

    def __init__(self, topic_path: str):
        super().__init__()
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = topic_path

    def create_topic(self) -> bool:
        self.publisher.create_topic(request={"name": self.topic_path})
        return self.topic_exists

    @property
    def topic_exists(self) -> bool:
        topics = self.publisher.list_topics(request={"project": PROJECT_PATH})
        return any(topic.name == self.topic_path for topic in topics)

    def synchronise(self) -> bool:
        if not self.topic_exists:
            message: str = f"`{self.topic_path}` does not exist. Creating..."
            logging.info(message)
            return self.create_topic()
        return False


class SubscriptionSynchroniser:
    """
    This synchronises topics and subscribers
    """

    def __init__(self, subscription_path: str, topic_path: str):
        super().__init__()
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = subscription_path
        self.topic_path = topic_path

    def create_subscription(self) -> bool:
        self.subscriber.create_subscription(
            request={"name": self.subscription_path, "topic": self.topic_path}
        )
        return self.subscription_exists

    @property
    def subscription_exists(self) -> bool:
        subscriptions = self.subscriber.list_subscriptions(request={"project": PROJECT_PATH})
        return any(subscription.name == self.subscription_path for subscription in subscriptions)

    def synchronise(self) -> bool:
        if not self.subscription_exists:
            message: str = f"`{self.subscription_path}` does not exist. Creating..."
            logging.info(message)
            return self.create_subscription()
        return False
