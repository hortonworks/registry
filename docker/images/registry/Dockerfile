# Copyright 2016 Hortonworks.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

#FROM centos:7.2.1511
#
#ARG REGISTRY_VERSION
#ENV REG_VER $REGISTRY_VERSION
#
#ENV JAVA_HOME /opt/jdk1.8.0_141/
#
## Add Hortonworks Registry binary. Maven profile "-Pdocker" will place the binary in the correct location
#ADD hortonworks-registry-$REGISTRY_VERSION.tar.gz /
#
## Install system dependencies
#RUN yum install -y wget
#
## Install Java
#RUN wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u141-b15/336fa29ff2bb4ef291e347e091f7f4a7/jdk-8u141-linux-x64.tar.gz" \
#    && tar xzf jdk-8u141-linux-x64.tar.gz \
#    && mv jdk1.8.0_141 /opt \
#    && cd /opt/jdk1.8.0_141/ \
#    && alternatives --install /usr/bin/java java /opt/jdk1.8.0_141/bin/java 1
#
#CMD hortonworks-registry-$REG_VER/bin/registry-server-start.sh hortonworks-registry-$REG_VER/conf/registry-dev.yaml

FROM openjdk:8

ENV TZ="/usr/share/zoneinfo/Asia/Kolkata"

ARG REGISTRY_VERSION
ARG VERSION_SUFFIX="${REGISTRY_VERSION}"

# Do not ask for confirmations when running apt-get, etc.
ENV DEBIAN_FRONTEND noninteractive

# update linux and install neccessary utilities.
RUN apt update && apt install -y sudo netcat iptables rsync unzip wget curl jq coreutils openssh-server net-tools vim \
python-pip python-dev libffi-dev libssl-dev cmake pkg-config libfuse-dev bc && apt-get -y clean

RUN mkdir /registryapp

ARG REGISTRY_MIRROR="https://github.com/hortonworks/registry/releases/download"
ARG URL="${REGISTRY_MIRROR}"/v"${REGISTRY_VERSION}"/hortonworks-registry-"${VERSION_SUFFIX}".tar.gz

RUN echo "Downloading Schema Registry from URL :: " ${URL}
RUN mkdir -p "/registryapp" && wget -q "${URL}" && tar xvzf hortonworks-registry-"${VERSION_SUFFIX}".tar.gz -C "/registryapp"

ENV REGISTRY_HOME /registryapp/hortonworks-registry-$REGISTRY_VERSION

COPY start-registry.sh /registryapp/hortonworks-registry-$REGISTRY_VERSION/bin/

WORKDIR /registryapp/hortonworks-registry-$REGISTRY_VERSION

RUN chmod a+x bin/*.sh bootstrap/bootstrap-storage.sh

ENTRYPOINT ["bin/start-registry.sh"]
