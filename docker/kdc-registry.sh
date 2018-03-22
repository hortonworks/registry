#!/usr/bin/env bash
#
# Docker Containerized Schema Registry application.
#
# Portions of this code is borrowed / inspired from https://github.com/tillt/docker-kdc
#

machine_name="hwx-machine"

# Image name variables
registry_image="schema-registry"
kdc_image="docker-kdc:0.1"
mysql_image="mysql:5.7"
oracle_image="oracle-hwx-xe-12c:0.1"
postgres_image="postgres:10"
kafka_image="apache-kafka:1.0.0"
minimal_ubuntu_image="minimal-ubuntu:0.1"

# Container name variables
mysql_container_name="hwx-mysql"
oracle_container_name="hwx-oracle"
postgres_container_name="hwx-postgres"
kdc_container_name="hwx-kdc"
zk_container_name="hwx-zk"
kafka_container_name="hwx-kafka-"

# Beware before changing the registry container name, it's used in the internal scripts to find out the instance number.
registry_container_name="hwx-schema-registry-"
network_name="hwx-net"

broker_nodes=${broker_nodes:-3}
registry_nodes=${registry_nodes:-2}
schema_registry_download_url=${schema_registry_download_url:-''}
db_type=""
sasl_secrets_dir=${sasl_secrets_dir:-"$(pwd)/secrets"}

# Standard output variable
std_output="/dev/null"

# KDC variables
# KDC hostname.
KDC_HOST_NAME=${kdc_container_name}
# External KDC port.
KDC_PORT=${KDC_PORT:-'48088'}
# Config file.
KDC_CONFIG=${KDC_CONFIG:-'images/kdc/kdc.json'}
# Templates source dir.
KDC_TEMPLATES_DIR=${KDC_TEMPLATES_DIR:-'templates'}

# Default principal.
KDC_PRINCIPAL=${KDC_PRINCIPAL:-'admin'}
# Default password.
KDC_PASSWORD=${KDC_PASSWORD:-'admin'}
# Kerberos domain name.
# Default derived from fully qualified domain-name of the host.
# Using backtick operator to prevent sublime's highlighting from freaking out.
DEFAULT=`hostname -f | awk -F. '{$1="";OFS="." ; print $0 ; OFS=""}' | sed 's/^.//'`
KDC_DOMAIN_NAME=${KDC_DOMAIN_NAME:-$DEFAULT}

function readKdcConfig {
	status=$(cat ${KDC_CONFIG} | jq 'has("port")')
	if [[ ${status} == "true" ]]; then
		KDC_PORT=$(cat ${KDC_CONFIG} | jq '.port' | sed -e 's/^"//'  -e 's/"$//')
	fi

	status=$(cat ${KDC_CONFIG} | jq 'has("domain")')
	if [[ ${status} == "true" ]]; then
		KDC_DOMAIN_NAME=$(cat ${KDC_CONFIG} | jq '.domain' | sed -e 's/^"//'  -e 's/"$//')
	fi

	status=$(cat ${KDC_CONFIG} | jq 'has("realm")')
	if [[ ${status} == "true" ]]; then
		KDC_REALM_NAME=$(cat ${KDC_CONFIG} | jq '.realm' | sed -e 's/^"//'  -e 's/"$//')
	fi

	status=$(cat ${KDC_CONFIG} | jq 'has("principals")')
	if [[ ${status} == "true" ]]; then
		OLD_IFS=$IFS
		IFS=$'\n'
		KDC_PRINCIPALS=($(cat ${KDC_CONFIG} | jq '.principals[] | .id+" "+.password+" "+.keytab'))
		IFS=${OLD_IFS}
	fi
}

# Try read and parse the setup from a JSON file.
if [ -e ${KDC_CONFIG} ]; then
	readKdcConfig
fi

DEFAULT=`echo ${KDC_DOMAIN_NAME} | awk '{print toupper($0)}'`
# Kerberos realm name.
# Default derived from KDC_DOMAIN_NAME.
KDC_REALM_NAME=${KDC_REALM_NAME:-$DEFAULT}

# For OSX, starts docker-machine if not running
function startMachine {
    # Adjust container in case of OSX.
	if [[ $OSTYPE =~ darwin.+ ]]; then
	    docker-machine create --driver virtualbox --virtualbox-memory 4096 "${machine_name}"
		docker-machine start "${machine_name}"
		docker-machine env "${machine_name}"
	fi
}

function stopMachine {
    if [[ $OSTYPE =~ darwin.+ ]]; then
        docker-machine stop "${machine_name}"
    fi
}

function buildKdc {
    pushd -- images/kdc &> ${std_output}
    local RENDER_PRINCIPAL="RUN kadmin -l add --password=PASSWORD --use-defaults PRINCIPAL"
	local EXPORT_KEYTAB="RUN kadmin -l ext_keytab -k /etc/security/keytabs/KEYTABNAME PRINCIPAL"

	# Use a temporary file for the add principal directives.
	local TEMP_PRINCIPALS="_principals.txt"
	local TEMP_KEYTABS="_keytabs.txt"
	for principal in "${KDC_PRINCIPALS[@]}"
	do
		principal=$(echo ${principal} | sed -e 's=^"=='  -e 's="$==')
		IFS=' ' read -ra principal <<< "$principal"
		IFS=''

		echo $(echo ${RENDER_PRINCIPAL} |		 	\
		sed -e "s=PRINCIPAL=${principal[0]}=g"		\
		-e "s=PASSWORD=${principal[1]}=g")		\
		>>${TEMP_PRINCIPALS}

		echo $(echo ${EXPORT_KEYTAB} | sed -e "s=KEYTABNAME=${principal[2]}=g" \
		    -e "s=PRINCIPAL=${principal[0]}=g") >>${TEMP_KEYTABS}
	done

	local DOCKER_FILE=$(<${KDC_TEMPLATES_DIR}/Dockerfile)
	local ADD_PRINCIPALS=$(<${TEMP_PRINCIPALS})
	local ADD_KEYTABS=$(<${TEMP_KEYTABS})

	DOCKER_FILE=$(echo "${DOCKER_FILE//PRINCIPALS/$ADD_PRINCIPALS}")
	DOCKER_FILE=$(echo "${DOCKER_FILE//REALM/$KDC_REALM_NAME}")
	DOCKER_FILE=$(echo "${DOCKER_FILE//EXPORT_KEYTAB/$ADD_KEYTABS}")
	echo ${DOCKER_FILE} > Dockerfile

	rm -f ${TEMP_PRINCIPALS} ${TEMP_KEYTABS}

	sed -e "s=HOST_NAME=$KDC_HOST_NAME=g" 				\
		-e "s=DOMAIN_NAME=$KDC_DOMAIN_NAME=g" 			\
		-e "s=REALM_NAME=$KDC_REALM_NAME=g"			\
		"$KDC_TEMPLATES_DIR/krb5.conf" >krb5.conf

	docker build -t ${kdc_image} .
	rm -f Dockerfile
	rm -f krb5.conf

	popd &> ${std_output}
}

function registryVersion {
    if [[ -z ${schema_registry_download_url} ]]; then
        rversion=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version|grep -Ev '(^\[|Download\w+:)')
    else
        filename=$(echo ${schema_registry_download_url} | cut -d '/' -f9)
        rversion=$(echo ${filename} | awk -F "hortonworks-registry-" '{print $2}' | awk -F ".tar.gz" '{print $1}')
    fi
    echo "${rversion}"
}

function buildSchemaRegistry {
    rversion=$(registryVersion)
    if [[ -z ${schema_registry_download_url} ]]; then
        if [[ "$(docker images -q ${registry_image}:${rversion} 2> ${std_output})" == "" ]]; then
            echo "Building Schema Registry distribution from the master branch."
            pushd -- "../" &> ${std_output}
            mvn clean package -DskipTests
            popd &> ${std_output}
            mvn clean package -Pdocker
        else
            echo "Schema registry image ${registry_image}:${rversion} already available, build skipped" \
                "If you want to re-build, remove the existing image and build again"
        fi
    else
        if [[ "$(docker images -q ${registry_image}:${rversion} 2> ${std_output})" == "" ]]; then
            echo "Downloading Schema Registry distribution from URL :: " ${schema_registry_download_url}
            wget -q --show-progress "${schema_registry_download_url}"
            mv ${filename} images/registry/
            docker build -t ${registry_image}:${rversion} images/registry --build-arg "REGISTRY_VERSION=${rversion}"
        else
            echo "Schema registry image ${registry_image}:${rversion} already available, build skipped" \
                "If you want to re-build, remove the existing image and build again"
        fi
    fi
}

function buildDocker {
    echo "Building Minimal Ubuntu Image"
    docker build -t ${minimal_ubuntu_image} images/minimal-ubuntu

    echo "Building Schema Registry Image"
    buildSchemaRegistry

    echo "Building KDC Server Image"
    buildKdc

    echo "Building Apache Zookeeper and Kafka Image"
    docker build -t ${kafka_image} images/kafka

    echo "Pulling official ${mysql_image} image from docker store"
    docker pull ${mysql_image}

    echo "Building Oracle image"
    docker build -t ${oracle_image} images/oracle

    echo "Pulling official ${postgres_image} image from docker store"
    docker pull ${postgres_image}
}

function startDocker {
    local containers=()
    local is_secured="${1}"
    shift
    j=0
    for service in "${@}"
    do
        case "${service}" in
             "${kdc_container_name}")
                if [[ ${is_secured} == "yes" ]]; then
                    startKdc
                    containers[${j}]=${kdc_container_name}
                    j=$((j+1))
                fi
                ;;
             "${zk_container_name}")
                startZookeeper ${is_secured}
                containers[${j}]=${zk_container_name}
                j=$((j+1))
                ;;
             "${kafka_container_name}")
                for ((i=0; i<${broker_nodes}; i++))
                do
                    cname=${kafka_container_name}${i}
                    startKafka ${i} ${cname} ${is_secured}
                    containers[${j}]=${cname}
                    j=$((j+1))
                    # Just providing enough time for the inited instance to bootstrap and start properly..
                    if [[ $i -ne $(echo ${broker_nodes}-1 | bc) ]]; then
                        sleep 2
                    fi
                done
                ;;
             # MySQL, Oracle and Postgres don't need entry in "/etc/hosts" so skipping those.
             "${mysql_container_name}"|mysql)
                startMySQL
                ;;
             "${oracle_container_name}"|oracle)
                startOracle
                ;;
             "${postgres_container_name}"|postgresql)
                startPostgres
                ;;
             "${registry_container_name}")
                for ((i=0; i<${registry_nodes}; i++))
                do
                    cname=${registry_container_name}${i}
                    startSchemaRegistry ${i} ${cname} ${db_type} ${is_secured}
                    containers[${j}]=${cname}
                    j=$((j+1))
                    # Just providing enough time for the inited instance to bootstrap and start properly..
                    if [[ $i -ne $(echo ${registry_nodes}-1 | bc) ]]; then
                        sleep 2
                    fi
                done
                ;;
             *)
                echo "Invalid container name : ${service}"
                ;;
        esac
    done

    echo "# Add the following entries in your \"/etc/hosts\" file to access the containers"
    local tmp_hosts="_hosts.txt"
    for service in "${containers[@]}"
    do
        echo "$(docker exec "${service}" ifconfig | grep -v "127.0.0.1" | grep inet | awk '{print $2}' | cut -d ':' -f2)\t${service}" >>${tmp_hosts}
    done

    local container_hosts=$(<${tmp_hosts})
    for service in "${containers[@]}"
    do
        docker exec -it ${service} /bin/bash -c "sudo echo \"${container_hosts}\" >> /etc/hosts"
    done
    echo "${container_hosts}"
    rm -f "${tmp_hosts}"

    if [[ $OSTYPE =~ darwin.+ ]]; then
        ip_prefix=$(docker exec "${containers[0]}" ifconfig | grep -v "127.0.0.1" | grep inet | awk '{print $2}' | cut -d ':' -f2 | cut -d "." -f1 -f2)
        echo "# Run this command to connect to the container"
        echo "sudo route add -net ${ip_prefix}.0.0/16 $(docker-machine ip ${machine_name})"
    fi
}

function stopDocker {
    if [[ $# -ne 0 ]]; then
        docker container stop "${@}"
        exit 0
    fi

    ask_yes_no "Do you want to stop all the (hwx-*) docker containers? [Y/n]: "
    if [[ "${_return}" -eq 0 ]]; then
      exit 0
    fi
    container_ids=$(docker container ps -f name=hwx-* -q)
    if [[ -z "${container_ids}" ]]; then
        echo "No containers to stop"
    else
        docker container stop ${container_ids}
    fi
}

function cleanDocker {
    if [[ $# -ne 0 ]]; then
        docker container rm --force "${@}"
        exit 0
    fi

    ask_yes_no "Do you want to remove the (hwx-*) containers? [Y/n]: "
    if [[ "${_return}" -eq 0 ]]; then
      exit 0
    fi
    echo "=== Removing the containers ==="
    container_ids=$(docker container ps -a -f name=hwx-* -q)
    if [[ -z "${container_ids}" ]]; then
        echo "No containers to remove"
    else
        docker container rm --force ${container_ids}
    fi

    echo "Removing the krb5.conf and keytabs files"
    rm -rvf "${sasl_secrets_dir}"

#    ask_yes_no "Do you want to prune all the stopped containers? [Y/n]: "
#    if [[ "${_return}" -eq 1 ]]; then
#        echo 'y' | docker container prune
#    fi

    ask_yes_no "Do you want to remove the docker images? [Y/n]: "
    if [[ "${_return}" -eq 0 ]]; then
        exit 0
    fi
    echo "=== Removing the docker images ==="
    image_names=("${kdc_image}" "${oracle_image}" "${kafka_image}" "${registry_image}":"$(registryVersion)")
    docker rmi ${image_names[@]}

    ask_yes_no "Do you want to remove the dangling docker images? [Y/n]: "
    if [[ "${_return}" -eq 0 ]]; then
      exit 0
    fi
    echo "=== Removing dangling docker images ==="
    image_ids=$(docker images -f "dangling=true" -q)
    if [[ -z "${image_ids}" ]]; then
        echo "No dangling images to remove"
    else
        docker rmi ${image_ids}
    fi

    ask_yes_no "Do you want to remove services network? [Y/n]: "
    if [[ "${_return}" -eq 0 ]]; then
      exit 0
    fi
    echo "=== Remove docker network ==="
    network_id=$(docker network ls --filter "name=${network_name}" -q)
    if [[ -z "${network_id}" ]]; then
      echo "No one network to remove"
    else
      docker network rm ${network_id}
    fi

    echo "You may additionally want to remove the pulled ${minimal_ubuntu_image}, ${mysql_image}, sath89/oracle-12c and" \
         "${postgres_image} images by yourself using (docker rmi \$IMAGE_ID) command..."
}

function createUserNetwork {
    echo "Creating docker network ${network_name}"

    docker network ls | awk '{print $2}' | grep ^${network_name}$ &> ${std_output}
    if [[ $? -eq 0 ]]; then
        echo "Docker network '${network_name}' already exists"
        return 0
    fi

    docker network create ${network_name}
}

function isContainerExists() {
    local cname="${1}"
    local component="${2}"

    container_id=$(docker container ps -f name=${cname} -q)
    if [[ -n ${container_id} ]]; then
        echo "${component} docker container '${cname}' already started"
        return 1
    fi

    container_id=$(docker container ps -a -f name=${cname} -q)
    if [[ -n ${container_id} ]]; then
        echo "Restarting the existing ${component} docker container '${cname}' with id : '${container_id}'"
        docker container start ${cname}
        return 1
    fi

    return 0
}

function checkStatus {
    local status="${1}"
    local component="${2}"

    if [[ ${status} -ne 0 ]]; then
        echo "ERROR: Unable to start / load the ${component} container."
        exit 1
    fi
}

function startMySQL {
    isContainerExists ${mysql_container_name} "MySQL"
    if [[ $? -eq 1 ]]; then
        return 0;
    fi

    local root_pwd="password"
    local user="registry_user"
    local pwd="password"
    local db="schema_registry"

    SECONDS=0
    echo "Starting MySQL server from image : ${mysql_image}"
    docker run --name ${mysql_container_name} \
        -h ${mysql_container_name} \
        -e MYSQL_ROOT_PASSWORD=${root_pwd} \
        -e MYSQL_DATABASE=${db} \
        -e MYSQL_USER=${user} \
        -e MYSQL_PASSWORD=${pwd} \
        -p 3308:3306 \
        --network ${network_name} \
        -d ${mysql_image}

    checkStatus $? "MySQL"

    echo "MySQL health check"
    while :
    do
        docker exec -it ${mysql_container_name} mysqladmin ping --silent &> ${std_output}
        if [[ $? -eq 0 ]]; then
            echo "MySQL server started successfully! Time taken : ${SECONDS}s"
            break
        else
            echo "MySQL Server is not Ready. Retrying to connect to it..."
            sleep 2
        fi
    done
}

function startOracle {
    isContainerExists ${oracle_container_name} "Oracle"
    if [[ $? -eq 1 ]]; then
        return 0;
    fi

    SECONDS=0
    echo "Starting Oracle Database from image : ${oracle_image}"
    docker run --name ${oracle_container_name} \
        -h ${oracle_container_name} \
        -e DBCA_TOTAL_MEMORY=1024 \
        -p 1521:1521 \
        -p 8080:8080 \
        --network ${network_name} \
        -d ${oracle_image}

    checkStatus $? "Oracle"

    echo "Oracle health check"
    retry=0
    while :
    do
        # check whether the oracle Apex is started!!
        nc -z localhost 8080 &> ${std_output}
        if [[ $? -eq 0 ]]; then
            # Adding a sleep for 20 seconds to provide enough time for Oracle to initializes the startup sql / scripts if any..
            sleep 20
            echo "Oracle database started successfully! Retried : ${retry}. Time taken : ${SECONDS}s"
            break
        else
            retry=$((retry+1))
            echo "${retry}: Oracle database is not Ready. Retrying to connect to it..."
            sleep 10
        fi
    done
}

function startPostgres {
    isContainerExists ${postgres_container_name} "Postgres"
    if [[ $? -eq 1 ]]; then
        return 0;
    fi

    local user="registry_user"
    local pwd="password"
    local db="schema_registry"

    SECONDS=0
    echo "Starting Postgres server from image : ${postgres_image}"
    docker run --name ${postgres_container_name} \
        -h ${postgres_container_name} \
        -e POSTGRES_DB=${db} \
        -e POSTGRES_USER=${user} \
        -e POSTGRES_PASSWORD=${pwd} \
        -p 5432:5432 \
        --network ${network_name} \
        -d ${postgres_image}

    checkStatus $? "Postgres"

    echo "Postgres health check"
    while :
    do
        docker exec -it ${postgres_container_name} pg_isready -U${user} &> ${std_output}
        if [[ $? -eq 0 ]]; then
            echo "Postgres server started successfully! Time taken : ${SECONDS}s"
            break
        else
            echo "Postgres Server is not Ready. Retrying to connect to it..."
            sleep 2
        fi
    done
}

function startKdc {
    # For logs, check /var/log/heimdal-kdc.log
    isContainerExists ${kdc_container_name} "KDC"
    if [[ $? -eq 1 ]]; then
        return 0;
    fi

    SECONDS=0
    echo "Kerberos KDC container starting..."
    docker run --name ${kdc_container_name} \
	    -h ${KDC_HOST_NAME}	\
		-p ${KDC_PORT}:88 \
		-p ${KDC_PORT}:88/udp \
		--network ${network_name} \
		-d ${kdc_image}

    checkStatus $? "Kerberos"
	echo "Kerberos KDC now reachable at '$(hostname):$KDC_PORT'. Time taken : ${SECONDS}s"

    # Note the KDC containers are connected to the same user network. So they can directly contact each other over any port.
	sed -e "s/HOST_NAME/$KDC_HOST_NAME:88/g"			\
		-e "s/DOMAIN_NAME/$KDC_DOMAIN_NAME/g" 			\
		-e "s/REALM_NAME/$KDC_REALM_NAME/g"			\
		"images/kdc/$KDC_TEMPLATES_DIR/krb5.conf" >"${sasl_secrets_dir}"/krb5.conf

    for kt in $(docker exec ${kdc_container_name} find /etc/security/keytabs/ -type f); do
        docker cp ${KDC_HOST_NAME}:${kt} "${sasl_secrets_dir}"/
    done
}

function startZookeeper {
    # For data, check /data/zk-data
    isContainerExists ${zk_container_name} "Zookeeper"
    if [[ $? -eq 1 ]]; then
        return 0;
    fi

    local is_secured="${1}"
    if [[ "${is_secured}" == "yes" ]]; then
        KRB_OPTS="-Djava.security.auth.login.config=/opt/kafka/config/zookeeper_jaas.conf
            -Djava.security.krb5.conf=/etc/registry/secrets/krb5.conf
            -Dsun.security.krb5.debug=true"
    fi

    SECONDS=0
    echo "Starting Apache Zookeeper container"
    docker run --name ${zk_container_name} \
        -h ${zk_container_name} \
        -p 2181:2181 \
        -v ${sasl_secrets_dir}:/etc/registry/secrets \
        -e KAFKA_HEAP_OPTS="-Xmx512M -Xms512M ${KRB_OPTS}" \
        --network ${network_name} \
        -d ${kafka_image} \
        bin/zookeeper-server-start.sh config/zookeeper.properties

    checkStatus $? "Zookeeper"
    echo "Apache Zookeeper started successfully. Time taken : ${SECONDS}s"
}

function startKafka {
    # For data, check /data/ak-data
    local brokerId="${1}"
    local cname="${2}"
    local is_secured="${3}"
    isContainerExists ${cname} "Kafka"
    if [[ $? -eq 1 ]]; then
        return 0;
    fi

    if [[ "${is_secured}" == "yes" ]]; then
        KRB_OPTS="-Djava.security.auth.login.config=/opt/kafka/config/kafka_jaas.conf
            -Djava.security.krb5.conf=/etc/registry/secrets/krb5.conf
            -Dsun.security.krb5.debug=true"
    fi

    SECONDS=0
    echo "Starting Apache Kafka container : ${brokerId}"
    hwx_zk_ip=$(docker exec ${zk_container_name} ifconfig | grep -v 127.0.0.1 | grep inet | awk '{print $2}' | cut -d ":" -f2)
    docker run --name ${cname} \
        -h ${cname} \
        -p 9092 \
        -p 9991 \
        -v ${sasl_secrets_dir}:/etc/registry/secrets \
        -e ZK_CONNECT="${zk_container_name}":2181 \
        -e BROKER_ID="${brokerId}" \
        -e KAFKA_HEAP_OPTS="-Xmx1G -Xms1G ${KRB_OPTS}" \
        -e IS_SECURED="${is_secured}" \
        --network ${network_name} \
        --add-host=${zk_container_name}:${hwx_zk_ip} \
        -d ${kafka_image}

    checkStatus $? "Kafka"
    echo "Apache Kafka started successfully. Time taken : ${SECONDS}s"
}

function startSchemaRegistry {
    local sid="${1}"
    local container_name="${2}"
    local is_secured="${4}"

    isContainerExists ${container_name} "Schema Registry"
    if [[ $? -eq 1 ]]; then
        return 0;
    fi

    local db_type="${3}"
    local db_name="schema_registry"
    local user="registry_user"
    local pwd="password"
    local class_name=""
    local url=""

    case "${db_type}" in
        mysql)
            classname="com.mysql.jdbc.jdbc2.optional.MysqlDataSource"
            url="jdbc:mysql://${mysql_container_name}/${db_name}"
            ;;
        oracle)
            classname="oracle.jdbc.pool.OracleDataSource"
            url="jdbc:oracle:thin:@${oracle_container_name}:1521:xe"
            ;;
        postgresql)
            classname="org.postgresql.ds.PGSimpleDataSource"
            url="jdbc:postgresql://${postgres_container_name}/${db_name}"
            ;;
        *)
            echo "Invalid db type : ${db_type} not supported"
            exit 1
    esac

    if [[ "${is_secured}" == "yes" ]]; then
        KRB_OPTS="-Djava.security.krb5.conf=/etc/registry/secrets/krb5.conf -Dsun.security.krb5.debug=true"
    fi

    SECONDS=0
    echo "Starting Schema Registry ${sid}"
    docker run --name ${container_name} \
        -h ${container_name} \
        -e DB_TYPE=${db_type} \
        -e DATA_SRC_CLASS_NAME=${classname} \
        -e DB_URL=${url} \
        -e DB_USER=${user} \
        -e DB_PASSWORD=${pwd} \
        -p 9010-9020:9090 \
        -p 9030-9040:9091 \
        --network ${network_name} \
        -v ${sasl_secrets_dir}:/etc/registry/secrets \
        -e REGISTRY_HEAP_OPTS="-Xmx1G -Xms1G ${KRB_OPTS}" \
        -e IS_SECURED="${is_secured}" \
        -d ${registry_image}:$(registryVersion)

    checkStatus $? "Schema Registry"
    echo "Schema Registry started successfully. Time taken : ${SECONDS}s"

    echo "Connect with the below ports to start the registry client"
    docker port ${container_name}
}

ask_yes_no() {
    local prompt="${1}"
    while true; do
        read -r -p "${prompt} " response
        case "${response}" in
            [yY]|[yY][eE][sS]) _return=1; return;;
            [nN]|[nN][oO]) _return=0; return;;
            *);;
        esac
        echo "Please respond 'yes' or 'no'."
        echo
    done
}

ask_db_type() {
    read -p "Which underlying db type to use ?
            1. mysql
            2. oracle
            3. postgres
        > " answer

        case "${answer}" in
            1|m|mysql) echo "mysql" ;;
            2|o|oracle) echo "oracle" ;;
            3|p|postgres|postgresql) echo "postgresql";;
            *) echo "Invalid db type : ${answer}"; exit 1;;
        esac
}

ask_cluster_security() {
    read -p "Do you want Kerberos secured cluster ?
            1. yes
            2. no
        > " answer

        case "${answer}" in
            1|y|[yY][eE][sS]) echo "yes" ;;
            2|n|[nN][oO]) echo "no" ;;
            *) echo "Invalid option : ${answer}"; exit 1;;
        esac
}

usage() {
    local exit_status="${1}"
    cat <<EOF
$0: a tool for running Schema Registry tests inside Docker images.

Usage: $0 [command] [options]

help|-h|--help
    Display this help message

start-machine
    Starts a ${machine_name} Linux virtual machine and installs the Docker Engine on top of it.
    This VM is used as Docker host machine as there are known problems in Docker Engine when
    running it on Mac and Windows OS.

    NOTE: Once the machine started, you should set the env variables which configures the shell
    to execute the docker commands (docker / docker-compose) inside the ${machine_name} VM.

build
    Builds the KDC, Zookeeper, Kafka and Schema Registry images.
    Pulls the community image of MySQL, Oracle and Postgres from the docker store.

    To run registry application with Oracle db, user needs to manually download the ojdbc.jar
    from the Oracle website and copy it to extlibs directory before building the image.

    To build Schema Registry from specific tag release, export the "schema_registry_download_url"
    variable before building the image. (Only tar file supported)

start
    Starts Schema Registry application with all the dependent services (KDC, ZK, AK and DB)
    Asks user which database to use to store the data. All the containers are connected with
    the private ${network_name} network.

    To connect with the schema registry app, copy the krb5.conf and keytabs from the "${sasl_secrets_dir}"
    directory and paste it to respective directories [OR] point the files using the System property.
    (-Djava.security.auth.login.config, -Djava.security.krb5.conf)

    One can also be able to start a single service / container.
    (eg) To start KDC server alone, you would run:
        $0 start ${kdc_container_name}

stop
    Stops all the running containers that are connected with the ${network_name} network.

    One can also be able to stop a single service / container.
    (eg) To stop KDC container alone, you would run:
        $0 stop ${kdc_container_name}

clean
    Removes all the stopped containers that are connected with the ${network_name} network.

    This will also remove the images, dangling images and network created by $0. This will
    free disk space.

    One can also be able to remove a single service / container.
    (eg) To remove KDC server alone, you would run:
        $0 clean ${kdc_container_name}

stop-machine
    This will power-off the ${machine_name} Linux Virtual machine.

ps
    Lists all the active containers that are connected with the ${network_name} network.

ps-all
    Lists all the containers that are connected with the ${network_name} network.

shell
    Login into the container and provides a Shell to the user.
    (eg) $0 shell ${kdc_container_name}

logs
    Shows the logs from the container.
    (eg) $0 logs ${kdc_container_name}

port
    Shows the ports that are exposed from the container to the host machine.
    (eg) $0 port ${kdc_container_name}

EOF
    exit "${exit_status}"
}

option="${1}"
shift
case "${option}" in
    h|-h|--help|help)
        usage 0
        ;;
    start-machine)
        startMachine
        ;;
    build)
        buildDocker
        ;;
    start)
        mkdir -p "${sasl_secrets_dir}"
        db_type=$(ask_db_type)

        createUserNetwork
        if [[ $# -eq 0 ]]; then
            startDocker "$(ask_cluster_security)" "${kdc_container_name}" "${zk_container_name}" "${kafka_container_name}" "${db_type}" "${registry_container_name}"
        else
            startDocker "$(ask_cluster_security)" "${@}"
        fi
        ;;
    stop)
        stopDocker "${@}"
        ;;
    clean)
        cleanDocker "${@}"
        ;;
    stop-machine)
        stopMachine
        ;;
    ps)
        docker ps -f name=hwx-*
        ;;
    ps-all)
        docker ps -a -f name=hwx-*
        ;;
    shell)
        if [[ $# -ne 1 ]]; then
            echo "Usage: $0 shell CONTAINER_NAME"
            exit 1
        fi
        docker exec -it "${1}" /bin/bash -c "export COLUMNS=`tput cols`; export LINES=`tput lines`; exec bash"
        ;;
    logs)
        if [[ $# -ne 1 ]]; then
            echo "Usage: $0 logs CONTAINER_NAME"
            exit 1
        fi
        docker logs -f "${1}"
        ;;
    port)
        if [[ $# -ne 1 ]]; then
            echo "Usage: $0 shell CONTAINER_NAME"
            exit 1
        fi
        docker port "${1}"
        ;;
    *)
        usage 0
        ;;
esac
