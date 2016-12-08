#!/usr/bin/env bash
# Copyright 2016 Hortonworks.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Resolve links - $0 may be a softlink
PRG="${0}"

while [ -h "${PRG}" ]; do
  ls=`ls -ld "${PRG}"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "${PRG}"`/"$link"
  fi
done

BOOTSTRAP_DIR=`dirname ${PRG}`
CONFIG_FILE_PATH=${BOOTSTRAP_DIR}/../conf/registry.yaml

# Which java to use
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

CONF_READER_MAIN_CLASS=com.hortonworks.registries.storage.tool.StorageProviderConfigurationReader
SCRIPT_RUNNER_MAIN_CLASS=com.hortonworks.registries.storage.tool.SQLScriptRunner
CLASSPATH=${BOOTSTRAP_DIR}/lib/storage-tool-*.jar

echo "Configuration file: ${CONFIG_FILE_PATH}"

CONF_READ_OUTPUT=`exec $JAVA -cp $CLASSPATH $CONF_READER_MAIN_CLASS $CONFIG_FILE_PATH`

# if it doesn't exit with code 0, just give up
if [ $? -ne 0 ]; then
  exit 1
fi

echo "JDBC connection informations: ${CONF_READ_OUTPUT}"

declare -a OUTPUT_ARRAY=(${CONF_READ_OUTPUT})

DB_TYPE=${OUTPUT_ARRAY[0]}
JDBC_DRIVER_CLASS=${OUTPUT_ARRAY[1]}
JDBC_URL=${OUTPUT_ARRAY[2]}
JDBC_USER=${OUTPUT_ARRAY[3]}
JDBC_PASSWORD=${OUTPUT_ARRAY[4]}

echo "DB TYPE: ${DB_TYPE}"
echo "JDBC_DRIVER_CLASS: ${JDBC_DRIVER_CLASS}"
echo "JDBC_URL: ${JDBC_URL}"
echo "JDBC_USER: ${JDBC_USER}"
echo "JDBC_PASSWORD: ${JDBC_PASSWORD}"

if [ $DB_TYPE -eq "phoenix" ];
then
  DELIM="\n"
else
  DELIM=";"
fi

echo "Script delimiter: ${DELIM}"

SCRIPT_DIR="${BOOTSTRAP_DIR}/sql/${DB_TYPE}"
FILE_OPT="-f ${SCRIPT_DIR}/drop_tables.sql -f ${SCRIPT_DIR}/create_tables.sql"

echo "Script files option: $FILE_OPT"

exec $JAVA -cp $CLASSPATH $SCRIPT_RUNNER_MAIN_CLASS -c $JDBC_DRIVER_CLASS -u $JDBC_URL -l $JDBC_USER -p $JDBC_PASSWORD -d $DELIM $FILE_OPT
