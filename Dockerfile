# stage 1
FROM python:3.8-slim as python-base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# stage 2
FROM python-base as dependency-base

RUN apt-get update; \
    apt-get install -y build-essential \
    procps \
    iputils-ping

EXPOSE ${CATASYN_PORT}

# stage 3
FROM dependency-base as development-base

ARG WHEEL=catasyn-0.1.0-py3-none-any.whl
ARG APPDIR=/catasyn

WORKDIR ${APPDIR}/
ADD catasyn/ ./
ADD dist/${WHEEL} ./
ADD .env ./
ADD .secrets/ ./.secrets

RUN pip3 install ${APPDIR}/${WHEEL}

ENTRYPOINT ["tail", "-f", "/dev/null"]
#ENTRYPOINT ["bash", "./entrypoints/multi.sh"]
