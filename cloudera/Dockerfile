FROM docker-private.infra.cloudera.com/cloudera_base/openjdk:11.0.13-jdk-slim-buster

ENV SCHEMA_REGISTRY_PORT=9090

RUN mkdir -p /opt/schema-registry /etc/schema-registry
COPY schema-registry.tgz /tmp/schema-registry.tgz

RUN tar xfz /tmp/schema-registry.tgz -C /opt/schema-registry --strip-components=1
RUN rm -f /tmp/schema-registry.tgz

RUN apt update && apt install -y \
    krb5-user \
    vim \
    bash \
    openssl \
    wget

ENV PATH=$PATH:/opt/schema-registry/bin
WORKDIR /opt/schema-registry

EXPOSE $SCHEMA_REGISTRY_PORT

ADD docker-entrypoint.sh /opt/schema-registry
ENTRYPOINT ["/opt/schema-registry/docker-entrypoint.sh"]