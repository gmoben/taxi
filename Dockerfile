#-*- mode: dockerfile -*-
FROM c12e/alpine-miniconda

ARG PROJECT_NAME=taxi
ARG SOURCE_DIR=$PROJECT_NAME
WORKDIR /opt/$PROJECT_NAME

RUN conda install -y -c conda-forge av
RUN conda install -y gcc_linux-64

RUN ln -s /opt/conda/bin/x86_64-conda_cos6-linux-gnu-gcc /usr/bin/gcc

COPY requirements.txt .
RUN pip install --ignore-installed -r requirements.txt

COPY setup.py .
COPY MANIFEST.in .
COPY $SOURCE_DIR ./$SOURCE_DIR
RUN python setup.py develop

ENTRYPOINT ["taxi", "start"]

EXPOSE 4442
