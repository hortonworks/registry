Docker Containerized Schema Registry
====================================

Dockerized Schema Registry application to run System tests. 


# Dependencies

- [Docker CE](https://www.docker.com/community-edition#/download)
- [Virtual Box](https://www.virtualbox.org/wiki/Downloads)
- jq (brew install jq)

# Usage

### Starts the docker machine

```
sh kdc-registry.sh start-machine
```
It starts a Linux virtual machine setup and installs the Docker Engine on top of it.
This VM is used as Docker host machine as there are known problems in Docker Engine when
running it on Mac and Windows OS.

<B>NOTE:</B> Once the machine started, you should set the environmental variables which configures the shell
to execute the docker commands (docker / docker-compose) inside the VM.

``` eval $(docker-machine env sr-host)```

### Build the Registry Image
```
sh kdc-registry.sh build
```

It builds the Key Distribution Center, Apache Zookeeper, Apache Kafka and Schema Registry Images.
And, pulls the official images of MySQL, Oracle and Postgresql database images from the docker store.

To run registry application with Oracle db, user needs to manually download the [ojdbc.jar](http://www.oracle.com/technetwork/database/features/jdbc/jdbc-drivers-12c-download-1958347.html) from the Oracle website and 
copy it to `extlibs` directory before building the image.

### Run the container
```
sh kdc-registry.sh start
```

Starts Schema Registry application with all the dependent services (KDC, ZK, AK and DB). Asks user 
which underlying database to use to store the data. All the containers are connected with the 
private network.

To connect with the schema registry app, copy the `krb5.conf` from `/tmp/kdc-registry` directory and paste it in `/etc` directory in 
your machine. All the keytabs are stored under `/tmp/kdc-registry/keytabs` directory.


### Stop the container
```
sh kdc-registry.sh stop
```
Stops all the running containers that are connected with the private network. 

### Clean
```
sh kdc-registry.sh clean
```
Removes all the stopped containers that are connected with the private network. This will also remove the images, dangling images and network that created. This will
free disk space. This operation will be performed only after taking confirmation from the user.


### Stop the docker machine
```
sh kdc-registry.sh stop-machine
```
This will stop and power-off the Linux Virtual machine.

### Lists the active containers
```
ps
```
Lists all the active containers that are connected with the private network.

### Lists all containers
```
ps-all
```
Lists all the containers that are connected with the private network.

### Container Shell
```
sh kdc-registry.sh shell ${name}
```
Login into the container and provides a Shell to the user.

### Container Logs
```
sh kdc-registry.sh logs ${name}
```
Shows the logs from the container.

### Other docker commands

- `docker port CONTAINER_ID` - displays the ports which are exposed to the outside world
- `docker images` - lists all the available images stored in the local repository
- `docker rmi IMAGE_ID` - to delete a particular image. Use `-f` option to delete the image forcefully
- `docker container ps` - lists all the running containers
- `docker container ps -a` - lists all the running and exited containers
- `docker container start CONTAINER_ID` - starts the container
- `docker container stop CONTAINER_ID` - stops the container
- `docker container rm CONTAINER_ID` - removes the container from the memory
- `docker container prune` - removes all the exited containers from the memory
- `docker exec -it CONTAINER_ID psql -U registry_user -W schema_registry` - to login into the postgres client shell