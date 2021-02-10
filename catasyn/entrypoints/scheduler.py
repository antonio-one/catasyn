import schedule
import logging
import typing
from time import sleep

from catasyn.settings import (SCHEDULE_INTERVAL_SECONDS, SCHEDULE_SLEEP_SECONDS,
                              DATCAT_SCHEME, DATCAT_HOST, DATCAT_PORT, DATASET,
                              CLOUD_PROJECT_ID)
from catasyn.service_layer.synchroniser import TableSynchroniser, TopicSynchroniser, SubscriptionSynchroniser
import requests
from urllib.parse import urlunsplit


def synchronise_all_schemas():
    url_components: typing.Tuple = (DATCAT_SCHEME, f"{DATCAT_HOST}:{DATCAT_PORT}", "", "", "")
    url = urlunsplit(url_components)

    # schemas.raise_for_status()
    # don't kill the service if there is a network error
    try:
        schemas = requests.get(url=url)
        schemas.raise_for_status()
    except requests.exceptions.ConnectionError:
        logging.exception()
        return
    else:
        message = f"{url} returned status {schemas.status_code}"
        logging.info(message)

    for schema in schemas.json():
        table_id = f"{CLOUD_PROJECT_ID}.{DATASET}.{schema}"
        syn = TableSynchroniser(table_id=table_id)
        synchronised_status: bool = syn.synchronise()
        message = f"{table_id=}, {synchronised_status=}"
        logging.debug(message)


def synchronise_all_topics():
    url_components: typing.Tuple = (DATCAT_SCHEME, f"{DATCAT_HOST}:{DATCAT_PORT}", "mappings", "refresh=True", "")
    url = urlunsplit(url_components)

    try:
        mappings = requests.get(url=url)
        mappings.raise_for_status()
    except requests.exceptions.ConnectionError:
        logging.exception()
        return
    else:
        message = f"{url} returned status {mappings.status_code}"
        logging.info(message)

    for schema_name, mappings in mappings.json().items():

        topic_name = mappings["topic_name"]
        topic_path = f"projects/{CLOUD_PROJECT_ID}/topics/{topic_name}"

        subscription_name = mappings["subscription_name"]
        subscription_path = f"projects/{CLOUD_PROJECT_ID}/subscriptions/{subscription_name}"

        syn = TopicSynchroniser(topic_path=topic_path)
        synchronised_status: bool = syn.synchronise()
        message = f"{topic_path=}, {synchronised_status=}"
        logging.debug(message)

        syn = SubscriptionSynchroniser(subscription_path=subscription_path, topic_path=topic_path)
        synchronised_status: bool = syn.synchronise()
        message = f"{subscription_path=}, {synchronised_status=}"
        logging.debug(message)


def synchronise_everything():
    synchronise_all_schemas()
    synchronise_all_topics()


def main():
    schedule.every(SCHEDULE_INTERVAL_SECONDS).seconds.do(synchronise_everything)
    while True:
        schedule.run_pending()
        sleep(SCHEDULE_SLEEP_SECONDS)


if __name__ == "__main__":
    main()
