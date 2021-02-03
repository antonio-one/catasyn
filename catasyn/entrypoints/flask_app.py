from flask_api import FlaskAPI
from flask import jsonify, redirect
from catasyn.settings import CATASYN_HOST, CATASYN_PORT, CATASYN_DEBUG

app = FlaskAPI(__name__)


@app.route("/")
def default():
    return redirect("/synchronise/all", code=302)


# @app.route("/synchronise/one", methods=["GET"])
@app.route("/synchronise/all", methods=["GET"])
def view_synchronisation_status():
    # TODO: how to run a scheduler asynchronously to a flask page as part of the same container?
    with open("../schedule.log", "r") as sl:
        return jsonify(sl.read())


def main() -> None:
    app.run(host=CATASYN_HOST, port=CATASYN_PORT, debug=CATASYN_DEBUG)


if __name__ == "__main__":
    main()
