FROM python:3.6-alpine

ENV PYTHONPATH=/opt/pylibs/:$PYTHONPATH

RUN pip install \
pexpect==4.2.1 \
PyYAML==3.12 \
pytest \
mock \
six

# RUN pip install -r /opt/pylibs/taxi/requirements.txt

EXPOSE 4222
