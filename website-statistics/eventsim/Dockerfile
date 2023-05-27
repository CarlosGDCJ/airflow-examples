FROM adoptopenjdk:11.0.8_10-jre-hotspot-bionic@sha256:24864d2d79437f775c70fd368c0272a1579a45a81c965e5fdcf0de699c15a054

RUN set -ex; \
  export DEBIAN_FRONTEND=noninteractive; \
  apt-get update; \
  mkdir -p /opt/eventsim

WORKDIR /opt/eventsim

COPY eventsim.sh /opt/eventsim/eventsim.sh
COPY examples /opt/eventsim/examples
COPY data /opt/eventsim/data
COPY eventsim-assembly-2.0.jar /opt/eventsim/eventsim-assembly-2.0.jar
CMD ./eventsim.sh -c examples/example-config.json --nouseAvro -n 200 --start-time 2023-03-25T00:00 \
--end-time 2023-07-25T00:00 --growth-rate 0.8 --userid 1000000 generated_data