import typing
from os import getcwd, getenv, path
from pathlib import Path
from uuid import uuid1

import pytest
from consumer_generator.adapters import repository as c_repository
from consumer_generator.domain import model as c_model
from google.cloud import bigquery, storage
from data_catalogue.adapters import repository
from data_catalogue.entrypoints import flask_app

from settings import CONSUMER_REPOSITORY_PATH, CLOUD_PROJECT_ID, LOCATION

@pytest.fixture(scope="session")
def schema_registry_path():
    return Path(getcwd()) / "schemas"


@pytest.fixture(scope="session")
def schema_repository(schema_registry_path) -> typing.Dict:
    sr = repository.SchemaRepository()
    sr.load(schema_registry_path)
    return sr.in_memory_schema_repository


@pytest.fixture(scope="session")
def client():
    flask_app.app.config["TESTING"] = True
    with flask_app.app.test_client() as client:
        yield client


@pytest.fixture(scope="session")
def consumer_repository_path():
    return CONSUMER_REPOSITORY_PATH


@pytest.fixture(scope="session")
def consumer_repository(
    consumer_repository_path,
) -> c_repository.FileSystemConsumerRepository:
    return c_repository.FileSystemConsumerRepository(
        repository_path=consumer_repository_path
    )


@pytest.fixture(scope="session")
def consumer(consumer_repository_path) -> c_model.Consumer:
    new_consumer = c_model.Consumer()
    new_consumer.file_path = path.join(
        consumer_repository_path, f'consumer_{str(uuid1()).partition("-")[0]}.py'
    )
    new_consumer.field_list = ["DUMMY_FIELD_1:INTEGER", "DUMMY_FIELD_2:INTEGER"]
    new_consumer.row_parser = (
        '"DUMMY_FIELD_1": row["DUMMY_FIELD_1"],"DUMMY_FIELD_2": row["DUMMY_FIELD_2"]'
    )
    new_consumer.payload = (
        "# TODO: test the payload separately\n# This file can be safely deleted"
    )
    return new_consumer


@pytest.fixture(scope="session")
def cloud_project_id():
    return CLOUD_PROJECT_ID


@pytest.fixture(scope="session")
def location():
    # https://cloud.google.com/bigquery/docs/locations
    return LOCATION


@pytest.fixture(scope="module")
def bucket_name(location) -> str:
    bucket_name = f'bucket_{str(uuid1()).partition("-")[0]}'

    # https://cloud.google.com/storage/docs/creating-buckets#storage-create-bucket-python
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name=bucket_name)
    # https://cloud.google.com/storage/docs/storage-classes
    bucket.storage_class = "COLDLINE"
    created_bucket = storage_client.create_bucket(bucket, location=location)

    yield created_bucket

    # https://cloud.google.com/storage/docs/deleting-buckets#storage-delete-bucket-python
    bucket.delete()


@pytest.fixture(scope="module")
def bucket_uri(bucket_name) -> str:
    return f"gs://{bucket_name}"


@pytest.fixture(scope="function")
def dataset_name() -> str:
    return f'dataset_{str(uuid1()).partition("-")[0]}'


@pytest.fixture(scope="function")
def dataset_id(cloud_project_id, dataset_name, location):
    dataset_id = f"{cloud_project_id}.{dataset_name}"
    # https://cloud.google.com/bigquery/docs/datasets#python
    client = bigquery.Client(location=location)
    client.create_dataset(dataset=dataset_id)

    yield dataset_id
    # https://cloud.google.com/bigquery/docs/samples/bigquery-delete-dataset#bigquery_delete_dataset-python
    client.delete_dataset(dataset=dataset_id, delete_contents=True, not_found_ok=True)
