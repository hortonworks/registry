Docker Containerized Schema Registry
====================================

Dockerized Schema Registry application to run System tests. 


# Dependencies

- [Docker CE](https://www.docker.com/community-edition#/download)
- [Virtual Box](https://www.virtualbox.org/wiki/Downloads)
- jq (brew install jq)

# Usage

### Starts the docker machine

If you're running on windows / Mac OS X, you need to use Docker machine to start the Docker host. Docker runs natively on
Linux, so the Docker host will be your machine. 

```
sh kdc-registry.sh start-machine
```
It starts a Linux virtual machine setup and installs the Docker Engine on top of it.
This VM is used as Docker host machine as there are known problems in Docker Engine when
running it on Mac and Windows OS.

<B>NOTE:</B> Once the machine started, you should configure your terminal window to attach it to your new Docker Machine.

``` eval $(docker-machine env reg_machine)```

### Build the Registry Image
```
sh kdc-registry.sh build
```

It builds the Key Distribution Center, Apache Zookeeper, Apache Kafka and Schema Registry Images.
And, pulls the official images of MySQL, Oracle and Postgresql database images from the docker store.

To run registry application with Oracle db, user needs to manually download the [ojdbc.jar](http://www.oracle.com/technetwork/database/features/jdbc/jdbc-drivers-12c-download-1958347.html) from the Oracle website and 
copy it to `extlibs` directory before building the image.

To build Schema Registry from specific tag release, export this variable before building the image. (Only tar file supported)
```
export schema_registry_download_url="https://github.com/hortonworks/registry/releases/download/v0.5.0/hortonworks-registry-0.5.0.tar.gz"
```

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
Removes all the stopped containers that are connected with the private network. This will also remove the created images, dangling images and network. This will
free disk space. 


### Stop the docker machine
```
sh kdc-registry.sh stop-machine
```
This will stop and power-off the Linux Virtual machine.

### Lists the active containers
```
sh kdc-registry.sh ps
```
Lists all the active containers that are connected with the private network.

### Lists all containers
```
sh kdc-registry.sh ps-all
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

- `docker images` - lists all the available images stored in the local repository
- `docker rmi IMAGE_ID` - to delete a particular image. Use `-f` option to delete the image forcefully
- `docker port CONTAINER_ID` - displays the ports which are exposed to the outside world
- `docker container start CONTAINER_ID` - starts the container
- `docker container stop CONTAINER_ID` - stops the container
- `docker container rm CONTAINER_ID` - removes the container
- `docker container prune` - removes all the stopped containers