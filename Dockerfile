FROM python:3.6-alpine

ARG PROJECT_NAME=taxi
ARG SOURCE_DIR=$PROJECT_NAME
WORKDIR /opt/$PROJECT_NAME

ENV PYLIBS=/opt/pylibs
ENV PYTHONPATH=$PYLIBS:$PYTHONPATH

COPY requirements.core .
COPY requirements.test .
RUN pip install -r requirements.core
RUN pip install -r requirements.test

COPY $SOURCE_DIR $PYLIBS/$PROJECT_NAME

ENV TAXI_CONFIG=/opt/taxi/config/default.yaml

EXPOSE 4442
