# A rez client for the end-to-end test: real rez, a tiny package repository and
# context tracking pointed at the compose stack's RabbitMQ.
FROM python:3.12-slim

ARG REZ_VERSION=3.5.0
RUN pip install --no-cache-dir "rez==${REZ_VERSION}"

COPY packages /packages
COPY rezconfig.py /etc/rez/rezconfig.py
COPY run.sh /usr/local/bin/run-e2e
ENV REZ_CONFIG_FILE=/etc/rez/rezconfig.py

RUN useradd --create-home artist
USER artist
WORKDIR /home/artist
ENTRYPOINT ["bash", "/usr/local/bin/run-e2e"]
