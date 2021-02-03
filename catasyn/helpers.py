import typing
from urllib.parse import urlunsplit


def catalogue_url(scheme: str, host: str, port: str, route: str, query: typing.Dict[str, typing.Any] = {}):
    """

    :param scheme: e.g "http"
    :param host: e.g "0.0.0.0"
    :param port: e.g "80"
    :param route: e.g "schemas"
    :param query: e.g refresh=True, or nothing if you are passing this as a barebone url to response.get and have already params=
    :return:
    """
    return urlunsplit((
        scheme,
        f"{host}:{port}",
        route,
        query,
        "",     # fragment. Leaving it empty at the moment
    ))


def coalesce(*values):
    output: typing.List = [a for a in values if a is not None]
    return output[0]
