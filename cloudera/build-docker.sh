#!/bin/bash

#
# Copyright (c) 2023 Cloudera, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

set -ex

SCHEMA_REGISTRY_TAR_LOCATION="${SCHEMA_REGISTRY_TAR_LOCATION:-__SCHEMA_REGISTRY_TAR_LOCATION__}"

rm -rf docker-tmp
mkdir docker-tmp

cp "${SCHEMA_REGISTRY_TAR_LOCATION}" docker-tmp/schema-registry.tgz
cp cloudera/docker-entrypoint.sh docker-tmp/docker-entrypoint.sh

docker_file="cloudera/Dockerfile"
image_name="schema-registry"

: ${REGISTRY:="docker-private.infra.cloudera.com/cloudera"}
: ${TAGS:="latest"}

tagging=""
for tag in ${TAGS}; do
  tagging="${tagging} -t ${REGISTRY}/${image_name}:${tag}"
done

docker build -f "${docker_file}" ${tagging} ./docker-tmp

rm -rf docker-tmp