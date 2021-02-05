import typing

from flask_api import FlaskAPI
from flask import redirect
from catasyn.settings import CATASYN_HOST, CATASYN_PORT, CATASYN_DEBUG

app = FlaskAPI(__name__)


@app.route("/")
def default():
    return redirect("/synchronise", code=302)


@app.route("/synchronise/", methods=["GET"])
def synchronise():
    return "all schemas: synchronise/schemas\n" \
           "all versions of a schema: synchronise/schema/{}\n" \
           "schema: synchronise/schema/{}/version/{}\n" \
           "all metadata: synchronise/metadata\n" \
           "all topics: synchronise/metadata/topic\n" \
           "one topic: synchronise/metadata/topic/{}\n" \
           "all subscriptions: synchronise/metadata/subscriptions\n" \
           "one subscription: synchronise/metadata/subscription/{}\n"


def main() -> None:
    app.run(host=CATASYN_HOST, port=CATASYN_PORT, debug=CATASYN_DEBUG)


if __name__ == "__main__":
    main()
