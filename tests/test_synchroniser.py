from catasyn.service_layer.synchroniser import TableSynchroniser


def test_tables_are_created(dataset_id, schemas_from_catalogue):

    for schema in schemas_from_catalogue:

        table_id = f"{dataset_id}.{schema}"
        syn = TableSynchroniser(table_id=table_id)
        syn.synchronise()

        assert syn.table_exists
        assert syn.schema_is_identical
