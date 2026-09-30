FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONPATH=/opt/grid/src TZ=UTC
WORKDIR /opt/grid

COPY requirements/python.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY src /opt/grid/src
