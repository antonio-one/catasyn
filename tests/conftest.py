from urllib.parse import urlunsplit
from uuid import uuid1

import pytest
import requests
from google.cloud import bigquery

from catasyn.entrypoints import flask_app
from catasyn.settings import (
    CLOUD_PROJECT_ID,
    DATCAT_HOST,
    DATCAT_PORT,
    DATCAT_SCHEME,
    LOCATION,
)

DATCAT_NETLOC = f"{DATCAT_HOST}:{DATCAT_PORT}"
DATCAT_ROUTE = "search_by_key"


@pytest.fixture(scope="session")
def client():
    flask_app.app.config["TESTING"] = True
    with flask_app.app.test_client() as client:
        yield client


@pytest.fixture(scope="session")
def datcat_url():
    return urlunsplit((DATCAT_SCHEME, DATCAT_NETLOC, "", "", ""))


@pytest.fixture(scope="session")
def schemas_from_catalogue(datcat_url):
    response = requests.get(datcat_url)
    return response.json()


@pytest.fixture(scope="session")
def cloud_project_id():
    return CLOUD_PROJECT_ID


@pytest.fixture(scope="session")
def location():
    # https://cloud.google.com/bigquery/docs/locations
    return LOCATION


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
