FROM continuumio/miniconda

ENV PYTHONPATH=/opt/pylibs/:$PYTHONPATH

RUN pip install \
futures==3.0.5 \
pexpect==4.2.1 \
PyYAML==3.12

# RUN pip install -r /opt/pylibs/bus/requirements.txt

EXPOSE 4222
