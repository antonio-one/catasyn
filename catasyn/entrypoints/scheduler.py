import schedule
import logging
import typing
from time import sleep

from catasyn.settings import (SCHEDULE_INTERVAL_SECONDS, SCHEDULE_SLEEP_SECONDS,
                              DATCAT_SCHEME, DATCAT_HOST, DATCAT_PORT, DATASET,
                              CLOUD_PROJECT_ID)
from catasyn.service_layer.synchroniser import TableSynchroniser
import requests
from urllib.parse import urlunsplit


def synchronise_all_schemas():
    components: typing.Tuple = (DATCAT_SCHEME, f"{DATCAT_HOST}:{DATCAT_PORT}", "", "", "")
    url = urlunsplit(components)

    # schemas.raise_for_status()
    # don't kill the service if there is a network error
    try:
        schemas = requests.get(url=url)
        schemas.raise_for_status()
    except requests.exceptions.ConnectionError:
        message = f"message here"
        logging.exception(message)
        return
    else:
        message = f"{url} returned status {schemas.status_code}"
        logging.info(message)

    for schema in schemas.json():
        table_id = f"{CLOUD_PROJECT_ID}.{DATASET}.{schema}"
        syn = TableSynchroniser(table_id=table_id)
        synchronised_status: bool = syn.synchronise()
        message = f"`{table_id}` {synchronised_status=}"
        logging.debug(message)


def main():
    schedule.every(SCHEDULE_INTERVAL_SECONDS).seconds.do(synchronise_all_schemas)
    while True:
        schedule.run_pending()
        sleep(SCHEDULE_SLEEP_SECONDS)


if __name__ == "__main__":
    main()
