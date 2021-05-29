FROM debian:10.9-slim

RUN apt update && DEBIAN_FRONTEND=noninteractive apt install -y --no-install-recommends \
      netcat \
      curl \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir /scripts

ENTRYPOINT ["scripts/run.sh"]
