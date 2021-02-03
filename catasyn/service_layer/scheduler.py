import schedule
import logging
import typing
from time import sleep

from catasyn.settings import SCHEDULE_INTERVAL_SECONDS, SCHEDULE_SLEEP_SECONDS, SCHEDULER_LOG_ROOT, DATCAT_SCHEME, DATCAT_HOST, DATCAT_PORT, DATASET, CLOUD_PROJECT_ID
from catasyn.service_layer.synchroniser import Synchroniser
import requests
from urllib.parse import urlunsplit
from pathlib import Path

FORMAT = "%(asctime)s %(levelname)s %(message)s"
filename = Path(SCHEDULER_LOG_ROOT) / "schedule.log"
logging.basicConfig(filename=filename, level=logging.INFO, format=FORMAT)


def synchronise_all_schemas():

    components: typing.Tuple = (DATCAT_SCHEME, f"{DATCAT_HOST}:{DATCAT_PORT}", "", "", "")
    url = urlunsplit(components)
    schemas = requests.get(url=url)

    for schema in schemas.json():
        table_id = f"{CLOUD_PROJECT_ID}.{DATASET}.{schema}"
        syn = Synchroniser(table_id=table_id)
        synchronised_status: bool = syn.synchronise()
        if synchronised_status:
            message = f"Creating `{table_id}` {synchronised_status=}."
        else:
            message = f"Doing nothing. Table `{table_id}` has already been created and has identical schema."
        logging.info(message)


def main():
    schedule.every(SCHEDULE_INTERVAL_SECONDS).seconds.do(synchronise_all_schemas)
    while True:
        schedule.run_pending()
        sleep(SCHEDULE_SLEEP_SECONDS)


if __name__ == "__main__":
    main()
