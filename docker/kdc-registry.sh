#!/usr/bin/env bash

# Image name variables
registry_image="schema-registry"
kdc_image="docker-kdc"
mysql_image="mysql:5.7"
oracle_image="oracle-hwx-xe-12c"
postgres_image="postgres:10"
kafka_image="hwx-kafka"
minimal_ubuntu_image="minimal-ubuntu"

# Container name variables
mysql_container_name="hwx-mysql"
mysql_container_ip="10.5.0.5"
oracle_container_name="hwx-oracle"
oracle_container_ip="10.5.0.6"
postgres_container_name="hwx-postgres"
postgres_container_ip="10.5.0.7"
kdc_container_name="hwx-kdc"
kdc_container_ip="10.5.0.8"
kafka_container_name="hwx-kafka"
kafka_container_ip="10.5.0.9"
registry_container_name="hwx-schema-registry."
registry_container_ip="10.5.0.10"
network_name="hwx-net"

scale=${scale:-2}
counter=0
download_url=${download_url:-''}

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

function buildSchemaRegistry {
    if [[ -z ${download_url} ]]; then
        rversion=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version|grep -Ev '(^\[|Download\w+:)')
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
        filename=$(echo ${download_url} | cut -d '/' -f9)
        rversion=$(echo ${filename} | awk -F "hortonworks-registry-" '{print $2}' | awk -F ".tar.gz" '{print $1}')

        if [[ "$(docker images -q ${registry_image}:${rversion} 2> ${std_output})" == "" ]]; then
            echo "Downloading Schema Registry distribution from URL :: " ${download_url}
            curl -O -C - "${download_url}"
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

function stopDocker {
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

    echo "Removing the temp krb5.conf and keytab files from the host machine"
    rm -rvf tmp/krb5.conf tmp/keytabs/*.keytab

#    ask_yes_no "Do you want to prune all the stopped containers? [Y/n]: "
#    if [[ "${_return}" -eq 1 ]]; then
#        echo 'y' | docker container prune
#    fi

    ask_yes_no "Do you want to remove the docker images? [Y/n]: "
    if [[ "${_return}" -eq 0 ]]; then
        exit 0
    fi
    echo "=== Removing the docker images ==="
    image_names=("${kdc_image}" "${oracle_image}" "${kafka_image}")
    if [[ -z ${download_url} ]]; then
        rversion=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version|grep -Ev '(^\[|Download\w+:)')
        image_names[3]="${registry_image}":"${rversion}"
    else
        filename=$(echo ${download_url} | cut -d '/' -f9)
        rversion=$(echo ${filename} | awk -F "hortonworks-registry-" '{print $2}' | awk -F ".tar.gz" '{print $1}')
        image_names[3]="${registry_image}":"${rversion}"
    fi
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

function startMySQL {
    local root_pwd="password"
    local user="registry_user"
    local pwd="password"
    local db="schema_registry"

    container_id=$(docker container ps -f name=${mysql_container_name} -q)
    if [[ -n ${container_id} ]]; then
        echo "MySQL docker container '${mysql_container_name}' already started"
        return 0
    fi

    container_id=$(docker container ps -a -f name=${mysql_container_name} -q)
    if [[ -n ${container_id} ]]; then
        echo "Restarting the existing MySQL docker container '${mysql_container_name}' with id : '${container_id}'"
        docker container start ${mysql_container_name}
        return 0
    fi

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

    if [[ $? -ne 0 ]]; then
        echo "ERROR: Unable to start / load the MySQL image from the docker repository."
        exit 1
    fi

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
#    NOTE: To run the Schema Registry with the ORACLE database. Download the latest `ojdbc.jar` for the corresponding oracle version
#    from `oracle technetwork <http://www.oracle.com/technetwork/database/features/jdbc/jdbc-drivers-12c-download-1958347.html>`_ (12c)
#    and copy it to `extlibs` directory before building the registry image.

    container_id=$(docker container ps -f name=${oracle_container_name} -q)
    if [[ -n ${container_id} ]]; then
        echo "Oracle docker container '${oracle_container_name}' already started"
        return 0
    fi

    container_id=$(docker container ps -a -f name=${oracle_container_name} -q)
    if [[ -n ${container_id} ]]; then
        echo "Restarting the existing Oracle docker container '${oracle_container_name}' with id : '${container_id}'"
        docker container start ${oracle_container_name}
        return 0
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

    if [[ $? -ne 0 ]]; then
        echo "ERROR: Unable to start / load the oracle image from the docker repository."
        exit 1
    fi

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
    local user="registry_user"
    local pwd="password"
    local db="schema_registry"

    container_id=$(docker container ps -f name=${postgres_container_name} -q)
    if [[ -n ${container_id} ]]; then
        echo "Postgres docker container '${postgres_container_name}' already started"
        return 0
    fi

    container_id=$(docker container ps -a -f name=${postgres_container_name} -q)
    if [[ -n ${container_id} ]]; then
        echo "Restarting the existing Postgres docker container '${postgres_container_name}' with id : '${container_id}'"
        docker container start ${postgres_container_name}
        return 0
    fi

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

    if [[ $? -ne 0 ]]; then
        echo "ERROR: Unable to start / load the Postgres image from the docker repository."
        exit 1
    fi

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
    container_id=$(docker container ps -f name=${kdc_container_name} -q)
    if [[ -n ${container_id} ]]; then
        echo "KDC docker container '${kdc_container_name}' already started"
        return 0
    fi

    container_id=$(docker container ps -a -f name=${kdc_container_name} -q)
    if [[ -n ${container_id} ]]; then
        echo "Restarting the existing KDC docker container '${kdc_container_name}' with id : '${container_id}'"
        docker container start ${kdc_container_name}
        return 0
    fi

    SECONDS=0
    echo "Kerberos KDC container starting..."
    docker run --name ${kdc_container_name} \
	    -h ${KDC_HOST_NAME}	\
		-p ${KDC_PORT}:88 \
		-p ${KDC_PORT}:88/udp \
		--network ${network_name} \
		-d ${kdc_image}

	echo "Kerberos KDC now reachable at '$(hostname):$KDC_PORT'. Time taken : ${SECONDS}s"

    # Note the KDC containers are connected to the same user network. So they can directly contact each other over any port.
	sed -e "s/HOST_NAME/$KDC_HOST_NAME:88/g"			\
		-e "s/DOMAIN_NAME/$KDC_DOMAIN_NAME/g" 			\
		-e "s/REALM_NAME/$KDC_REALM_NAME/g"			\
		"images/kdc/$KDC_TEMPLATES_DIR/krb5.conf" >/tmp/kdc-registry/krb5.conf

    for kt in $(docker exec ${kdc_container_name} find /etc/security/keytabs/ -type f); do
        docker cp ${KDC_HOST_NAME}:${kt} /tmp/kdc-registry/keytabs/
    done
}

function startKafka {
    container_id=$(docker container ps -f name=${kafka_container_name} -q)
    if [[ -n ${container_id} ]]; then
        echo "Apache Kafka docker container '${kafka_container_name}' already started"
        return 0
    fi

    container_id=$(docker container ps -a -f name=${kafka_container_name} -q)
    if [[ -n ${container_id} ]]; then
        echo "Restarting the existing Kafka docker container '${kafka_container_name}' with id : '${container_id}'"
        docker container start ${kafka_container_name}
        return 0
    fi

    SECONDS=0
    echo "Creating Apache Zookeeper and Kafka containers"
    docker create --name ${kafka_container_name} \
        -h ${kafka_container_name} \
        -p 9092:9092 \
        -p 9093:9093 \
        -p 2181:2181 \
        -p 1099:1099 \
        --network ${network_name} \
        ${kafka_image}

    echo "Copying krb5 configuration and keytabs from KDC Server"
    docker cp /tmp/kdc-registry/krb5.conf ${kafka_container_name}:/etc/
    for kt in `find /tmp/kdc-registry/keytabs -type f`; do
        docker cp ${kt} ${kafka_container_name}:/etc/security/keytabs/
    done

    docker start ${kafka_container_name}

    if [[ $? -ne 0 ]]; then
        echo "ERROR: Unable to start / load the Kafka image from the docker repository."
        exit 1
    else
        echo "ZK and AK started successfully. Time taken : ${SECONDS}s"
    fi
}

function startSchemaRegistry {
    local db_type="${1}"
    local container_name="${registry_container_name}${2}"
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

    container_id=$(docker container ps -f name=${container_name} -q)
    if [[ -n ${container_id} ]]; then
        echo "Schema Registry container '${container_name}' already started"
        return 0
    fi

    container_id=$(docker container ps -a -f name=${container_name} -q)
    if [[ -n ${container_id} ]]; then
        echo "Restarting the existing Schema Registry docker container '${container_name}' with id : '${container_id}'"
        docker container start ${container_name}
        return 0
    fi

    SECONDS=0
    echo "Starting Schema Registry"
    docker create --name ${container_name} \
        -h ${container_name} \
        -e DB_TYPE=${db_type} \
        -e DATA_SRC_CLASS_NAME=${classname} \
        -e DB_URL=${url} \
        -e DB_USER=${user} \
        -e DB_PASSWORD=${pwd} \
        -p 9010-9020:9090 \
        -p 9030-9040:9091 \
        --network ${network_name} \
        ${registry_image}

    echo "Copying krb5 configuration and keytabs from KDC Server"
    docker cp /tmp/kdc-registry/krb5.conf ${container_name}:/etc/
    for kt in `find /tmp/kdc-registry/keytabs -type f`; do
        docker cp ${kt} ${container_name}:/etc/security/keytabs/
    done

    docker start ${container_name}

    if [[ $? -ne 0 ]]; then
        echo "ERROR: Unable to start / load the Schema Registry image from the docker repository."
        exit 1
    else
        echo "Schema Registry started successfully. Time taken : ${SECONDS}s"
    fi

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

option="${1}"
shift
case "${option}" in
    build)
        buildDocker
        ;;
    start)
        read -p "Which underlying db type to use ?
            1. mysql
            2. oracle
            3. postgres
        > " answer
        case "${answer}" in
            m|o|p|mysql|oracle|postgres|postgresql) ;;
            *) echo "Invalid db type : ${answer}"; exit;;
        esac

        mkdir -p /tmp/kdc-registry/keytabs/
        createUserNetwork
        startKdc
        startKafka

        db_type="${answer}"
        case "${answer}" in
            mysql|m)
                db_type="mysql"
                startMySQL
                ;;
            oracle|o)
                db_type="oracle"
                startOracle
                ;;
            postgres|postgresql|p)
                db_type="postgresql"
                startPostgres
                ;;
        esac

        for ((i=1; i<=$scale; i++))
        do
            startSchemaRegistry ${db_type} $i
            # Just providing enough time for the inited instance to bootstrap and start properly..
            if [[ $i -ne $scale ]]; then
                sleep 5
            fi
        done
        ;;
    stop)
        stopDocker
        ;;
    clean)
        cleanDocker
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
    list)
        # TODO, list all the container names. Provide a facility to start / stop a single container
        ;;
    *)
        echo "Usage: $0 build|start|stop|clean|ps|ps-all|shell|logs"
        ;;
esac
