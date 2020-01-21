FROM openjdk:8-jre-alpine
RUN apk add --no-cache bash krb5 openssl
#ADD http://s3.amazonaws.com/dev.hortonworks.com/CSP/centos7/99.x/BUILDS/99.0.0.0-186/tars/registry/hortonworks-registry-0.8.1.99.0.0.0-186.tar.gz /opt
ARG binary=registry-dist/target/hortonworks-registry-0.8.1-SNAPSHOT.tar.gz
ENV REGISTRY_HOME /opt/registry
ADD ${binary} /opt/
RUN ln -s /opt/hortonworks-registry* /opt/registry
COPY ci/docker/schema-registry/entrypoint.sh /opt/registry/bin/
ENTRYPOINT ["/opt/registry/bin/entrypoint.sh"]
CMD ["/opt/registry/conf/registry.yaml"]
