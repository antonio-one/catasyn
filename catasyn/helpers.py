import typing
from urllib.parse import urlunsplit

from settings import CATALOGUE_SCHEME, CATALOGUE_HOST, CATALOGUE_PORT


def catalogue_url(route: str, refresh: bool = False):
    return urlunsplit((
        CATALOGUE_SCHEME,
        f"{CATALOGUE_HOST}:{CATALOGUE_PORT}",
        route,
        f"refresh={refresh}",
        "",
    ))


def coalesce(*values):
    output: typing.List = [a for a in values if a is not None]
    return output[0]
