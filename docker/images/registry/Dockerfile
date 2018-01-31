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

FROM openjdk:8

ENV TZ="/usr/share/zoneinfo/Asia/Kolkata"

ARG REGISTRY_VERSION
ARG VERSION_SUFFIX="${REGISTRY_VERSION}"

# Do not ask for confirmations when running apt-get, etc.
ENV DEBIAN_FRONTEND noninteractive

# update linux and install neccessary utilities.
RUN apt update && apt install -y sudo netcat iptables rsync unzip wget curl jq coreutils openssh-server net-tools vim \
less python-pip python-dev libffi-dev libssl-dev cmake pkg-config libfuse-dev bc && apt-get -y clean

ARG REGISTRY_MIRROR="https://github.com/hortonworks/registry/releases/download"
ARG URL="${REGISTRY_MIRROR}"/v"${REGISTRY_VERSION}"/hortonworks-registry-"${VERSION_SUFFIX}".tar.gz

RUN echo "Downloading Schema Registry from URL :: " ${URL} \
&& wget "${URL}" && tar xvzf hortonworks-registry-"${VERSION_SUFFIX}".tar.gz -C "/opt" \
&& rm hortonworks-registry-"${VERSION_SUFFIX}".tar.gz

ENV REGISTRY_HOME /opt/hortonworks-registry-$REGISTRY_VERSION

COPY start-registry.sh /opt/hortonworks-registry-$REGISTRY_VERSION/bin/

WORKDIR /opt/hortonworks-registry-$REGISTRY_VERSION

RUN chmod a+x bin/*.sh bootstrap/bootstrap-storage.sh

ENTRYPOINT ["bin/start-registry.sh"]
