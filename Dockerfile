#-*- mode: dockerfile -*-
FROM c12e/alpine-miniconda

ARG PROJECT_NAME=taxi
ARG SOURCE_DIR=$PROJECT_NAME
WORKDIR /opt/$PROJECT_NAME

RUN conda install -y -c conda-forge av

COPY requirements.core .
COPY requirements.test .
COPY requirements.tools .
RUN pip install -r requirements.core
RUN pip install -r requirements.test
RUN pip install -r requirements.tools

COPY setup.py .
COPY $SOURCE_DIR ./$SOURCE_DIR
RUN python setup.py develop

ENV TAXI_CONFIG=/opt/taxi/config/default.yaml
ENTRYPOINT ["taxi", "start"]

EXPOSE 4442
