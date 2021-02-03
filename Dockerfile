# stage 1
FROM python:3.8-slim as python-base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=${PYTHONPATH}:/catasyn

# stage 2
FROM python-base as dependency-base

RUN apt-get update; \
    apt-get install -y build-essential \
    iputils-ping

EXPOSE ${CATASYN_PORT}

# stage 3
FROM dependency-base as development-base

WORKDIR /catasyn/
ADD .env ./.env
ADD catasyn/service_layer/ ./service_layer/
ADD dist/ ./dist/
ADD catasyn/domain/ ./domain/
ADD catasyn/entrypoints/ ./entrypoints/
ADD catasyn/settings.py ./settings.py


RUN pip3 install $(find dist/ -name *whl)

ENTRYPOINT ["tail", "-f", "/dev/null"]

#CMD ["python", "service_layer/scheduler.py"]
#ENTRYPOINT ["python", "entrypoints/flask_app.py"]