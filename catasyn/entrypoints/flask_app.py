from decouple import config
from flask_api import FlaskAPI

app = FlaskAPI(__name__)


@app.route("/status", methods=["GET"])
def view_synchronisation_status():
    return "This will display the current synchronisation status"


@app.route("/history", methods=["GET"])
def view_synchronisation_history():
    return "This will display the synchronisation history"


def _run() -> None:
    app.run(host=config("HOST"), port=config("HOST"), debug=config("DEBUG"))
