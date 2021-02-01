import pytest
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datcat.domain import model
from catasyn.service_layer import sync_main


@pytest.mark.parametrize(
    "client, schema_name, schema_version, refresh, dataset_id, table_name",
    [
        ("client", "schema_one", 1, True, "dataset_id", "schema_one_v1"),
        ("client", "schema_two", 1, True, "dataset_id", "schema_two_v1"),
    ],
    indirect=["client", "dataset_id"],
)
def test_on_demand_table_is_created(
    client, schema_name, schema_version, refresh, dataset_id, table_name
):
    table_id = f"{dataset_id}.{table_name}"
    sod = sync_main.Synchroniser(table_id)
    schema_format = model.SchemaFormat(
        schema_name=schema_name, schema_version=schema_version, refresh=refresh
    )

    # TODO: Whenever we are ready to do an integration test
    """scheme = os.getenv("CATALOGUE_SCHEME")
    netloc = f'{os.getenv("CATALOGUE_HOST")}:{os.getenv("CATALOGUE_PORT")}'
    route = "search_by_key"
    url = urlunsplit((scheme, netloc, route, "", ""))
    sfc = sod.schema_from_catalogue(url=url, schema_format=schema_format)"""
    """*** requests.exceptions.ConnectionError: HTTPSConnectionPool(host='127.0.0.1', port=8080):
    Max retries exceeded with url: /search_by_key?schema_name=schema_two&schema_version=1&refresh=True
    (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x7fe8ab810040>:
    Failed to establish a new connection: [Errno 61] Connection refused'))"""

    response = client.get(
        "/search_by_key",
        query_string=schema_format.params,
    )
    schema_definition = response.data
    sod.create_table(table_id=table_id, schema_definition=schema_definition)
    client = bigquery.Client()
    table_exists: bool = True
    try:
        client.get_table(table_id)
    except NotFound:
        table_exists = False

    assert table_exists is True
    assert sod.table is not None
    # TODO: find something more elegant here perhaps?
    schema_definition = schema_definition.decode("utf-8").strip()
    assert sod.schema_from_dataset == schema_definition
    # The below will fail as there it can only be integration tested
    # assert sod.schema_is_identical is True


@pytest.mark.skip(reason="work in progress")
def test_existing_on_demand_table_is_not_recreated():
    pass


@pytest.mark.skip(reason="work in progress")
def test_on_demand_table_schema_is_correct():
    pass


@pytest.mark.skip(reason="work in progress")
def test_scheduler_intervals():
    pass


@pytest.mark.skip(reason="work in progress")
def test_scheduler_table_is_created():
    pass


@pytest.mark.skip(reason="work in progress")
def test_existing_scheduler_table_is_not_recreated():
    pass


@pytest.mark.skip(reason="work in progress")
def test_scheduler_table_schema_is_correct():
    pass


@pytest.mark.skip(reason="work in progress")
def test_scheduler_multiple_tables_are_created():
    pass


@pytest.mark.skip(reason="work in progress")
def test_existing_scheduler_multiple_tables_are_not_recreated():
    pass


@pytest.mark.skip(reason="work in progress")
def test_scheduler_multiple_table_schemas_are_correct():
    pass
