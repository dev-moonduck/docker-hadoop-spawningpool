FROM python:3.9.1-slim

RUN apt update && DEBIAN_FRONTEND=noninteractive apt install -y --no-install-recommends \
      netcat \
      curl \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir /scripts

COPY run.sh /scripts

ENTRYPOINT ["scripts/run.sh"]
