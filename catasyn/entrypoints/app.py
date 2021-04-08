from fastapi import FastAPI, status
from fastapi.responses import RedirectResponse
from catasyn.settings import SCHEDULE_INTERVAL_SECONDS

ROOT = "/v1/catasyn"

app = FastAPI(
    title="catasyn",
    description="A simple way to synchronise your DATa CATalogue with Bigquery",
    version="0.1.0"
)


@app.get("/")
async def default():
    return RedirectResponse("/docs", status_code=302)


@app.get("/{ROOT:path}/synchronise/every/{every}/seconds")
async def synchronise(schedule_interval_seconds: int = SCHEDULE_INTERVAL_SECONDS):
    return f"will eventually run the sync every {schedule_interval_seconds} seconds.\n" \
           f"for now run: python -m catasyn.entrypoints.scheduler"
