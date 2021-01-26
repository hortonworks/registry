Schema Registry cluster deployment using SASL
=============================================

Dockerized Schema Registry application.


# Dependencies

- [Docker CE](https://www.docker.com/community-edition#/download)
- [Virtual Box](https://www.virtualbox.org/wiki/Downloads)
- jq (brew install jq)

Due to import regulations in some countries, the Oracle implementation limits the strength of cryptographic algorithms 
available by default. If stronger algorithms are needed (for example, AES with 256-bit keys), the [JCE Unlimited Strength
Jurisdiction Policy Files](http://www.oracle.com/technetwork/java/javase/downloads/index.html) must be obtained and 
installed in the JDK/JRE. See the [JCA Providers Documentation](https://docs.oracle.com/javase/8/docs/technotes/guides/security/SunProviders.html) 
for more information.
 
- Note that if you are using Oracle Java, you will need to download JCE policy files for your Java version and copy 
  local_policy.jar and US_export_policy.jar to $JAVA_HOME/jre/lib/security. If file already exists, replace both of them.

# Usage

### Starts the docker machine

If you're running on windows / Mac OS X, you need to use Docker machine to start the Docker host. Docker runs natively on
Linux, so the Docker host will be your machine. 


### Build the Registry Image

We use the Jib plugin to build the docker image. Jib uses the Gradle dependency management and eliminates
the need to write a Dockerfile.

There are 3 ways to build a docker image:
* jib - builds an image and uploads it to Docker's registry
* jibDockerBuild - builds and image and pushes it to the local Docker daemon
* jibBuildTar - builds a tarball file which can then be imported into Docker 

Example run:

```
gradle jibDockerBuild -x test -x check
```

The image will contain the Schema Registry service. It doesn't use kerberos authentication and doesn't
support TLS. These can be added optionally by you if you modify the build.gradle file.


### Database connection

By default Schema Registry will use conf/registry.yaml which is set to connect to a database under localhost.
You can use docker-compose to run MySql together with Schema Registry. 
If you're running Schema Registry alone then you need to modify conf/registry.yaml before building the 
docker image. Change the JDBC port and instead of "localhost" write the IP address of the MySql server.

```
dataSource.url: "jdbc:mysql://12.10.7.120/schema_registry"
```


### Run the container
```
docker run schema-registry
```

### Connecting to Schema Registry 

Docker will open ports 9090 and 9091. You need to connect to the docker container's host.
Get the container's id with "docker ps" and then run the below command:

```
docker inspect --format '{{ .NetworkSettings.IPAddress }}' <the-container-id>
```

This outputs the container's IP address.


### Lists the active containers
```
docker ps
```
Lists all the active containers that are connected with the private network.


### Other docker commands

- `docker images` - lists all the available images stored in the local repository
- `docker rmi IMAGE_ID` - to delete a particular image. Use `-f` option to delete the image forcefully
- `docker port CONTAINER_ID` - displays the ports which are exposed to the outside world
- `docker container start CONTAINER_ID` - starts the container
- `docker container stop CONTAINER_ID` - stops the container
- `docker container rm CONTAINER_ID` - removes the container
- `docker container prune` - removes all the stopped containers