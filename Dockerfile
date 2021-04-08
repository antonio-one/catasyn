# stage 1
FROM python:3.8-slim as os-base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update; \
    apt-get install -y build-essential \
    procps \
    iputils-ping

EXPOSE ${CATASYN_PORT}

# stage 2
FROM os-base as app-base

ARG APPDIR=/catasyn
WORKDIR ${APPDIR}
COPY catasyn ./catasyn
COPY dist ./dist
ADD .secrets ./.secrets
RUN pip3 install ${APPDIR}/dist/*.whl

#ENTRYPOINT ["tail", "-f", "/dev/null"]
CMD uvicorn catasyn.entrypoints.app:app --host=0.0.0.0 --port=${CATASYN_PORT}