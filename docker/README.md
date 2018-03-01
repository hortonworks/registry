Schema Registry cluster deployment using SASL
=============================================

Dockerized Schema Registry application with SASL enabled to run System tests. 


# Dependencies

- [Docker CE](https://www.docker.com/community-edition#/download)
- [Virtual Box](https://www.virtualbox.org/wiki/Downloads)
- jq (brew install jq)

Due to import regulations in some countries, the Oracle implementation limits the strength of cryptographic algorithms 
available by default. If stronger algorithms are needed (for example, AES with 256-bit keys), the [JCE Unlimited Strength
Jurisdiction Policy Files](http://www.oracle.com/technetwork/java/javase/downloads/index.html) must be obtained and 
installed in the JDK/JRE. See the [JCA Providers Documentation](https://docs.oracle.com/javase/8/docs/technotes/guides/security/SunProviders.html) 
for more information.
 
Note that if you are using Oracle Java, you will need to download JCE policy files for your Java version and copy them 
to $JAVA_HOME/jre/lib/security.

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
Always, run the below command whenever opening a new terminal.

``` eval $(docker-machine env hwx-machine)```

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

To connect with the schema registry app, copy the krb5.conf and keytabs from the `$(pwd)/secrets` directory (where pwd 
denotes ~/registry/docker directory) and paste it to respective directories [OR] point those files using the System property.
(-Djava.security.auth.login.config=path/abc_jaas.conf, -Djava.security.krb5.conf=path/krb5.conf)

#### Run a single container
```
sh kdc-registry.sh start ${name}
```
Starts a single container with the specified name.

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

### Container Exposed ports
```
sh kdc-registry.sh port ${name}
```
Shows the exposed ports from the container to the host machine.

### Stop the container
```
sh kdc-registry.sh stop
```
Stops all the running containers that are connected with the private network.

#### Stop a single container
```
sh kdc-registry.sh stop ${name}
```
Stops the container with the specified name

### Clean
```
sh kdc-registry.sh clean
```
Removes all the stopped containers that are connected with the private network. This will also remove the created images, 
dangling images and network. This will free disk space. 

#### Clean a single container
```
sh kdc-registry.sh clean ${name}
```
Removes only the container with the specified name

### Stop the docker machine
```
sh kdc-registry.sh stop-machine
```
This will stop and power-off the Linux Virtual machine.

### Other docker commands

- `docker images` - lists all the available images stored in the local repository
- `docker rmi IMAGE_ID` - to delete a particular image. Use `-f` option to delete the image forcefully
- `docker port CONTAINER_ID` - displays the ports which are exposed to the outside world
- `docker container start CONTAINER_ID` - starts the container
- `docker container stop CONTAINER_ID` - stops the container
- `docker container rm CONTAINER_ID` - removes the container
- `docker container prune` - removes all the stopped containers