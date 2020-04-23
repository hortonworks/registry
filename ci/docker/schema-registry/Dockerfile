FROM openjdk:8-jre-alpine
RUN apk add --no-cache bash krb5 openssl wget
#ADD http://s3.amazonaws.com/dev.hortonworks.com/CSP/centos7/99.x/BUILDS/99.0.0.0-186/tars/registry/hortonworks-registry-0.8.1.99.0.0.0-186.tar.gz /opt
ARG binary=registry-dist/target/schemaregistry*.tar.gz
ENV REGISTRY_HOME /opt/registry
ADD ${binary} /opt/
RUN ln -s /opt/schemaregistry* /opt/registry
RUN wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/5.1.48/mysql-connector-java-5.1.48.jar -q -O /opt/registry/libs/mysql-connector-java-5.1.48.jar
RUN ln -s /opt/registry/libs/mysql-connector-java-5.1.48.jar /opt/registry/bootstrap/lib/mysql-connector-java-5.1.48.jar
COPY ci/docker/schema-registry/entrypoint.sh /opt/registry/bin/
ENTRYPOINT ["/opt/registry/bin/entrypoint.sh"]
CMD ["/opt/registry/conf/registry.yaml"]
