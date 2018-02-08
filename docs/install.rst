Installation
============

CentOS/RedHat
-------------

1. Install Java

   ::

       # wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u111-b14/jdk-8u111-linux-x64.tar.gz"
       #  tar xzf jdk-8u111-linux-i586.tar.gz
       # cd /opt/jdk1.8.0_111/
       # alternatives --install /usr/bin/java java /opt/jdk1.8.0_111/bin/java 2
       # alternatives --config java

2. Download latest Schema Registry binaries from here
   https://github.com/hortonworks/registry/releases

3. edit $REGISTRY\_HOME/conf/registry-env.sh, add the following

   ::

           export JAVA_HOME=/opt/jdk1.8.0_111/

4. Setup Database

   .. rubric:: 4.1 Mysql
      :name: mysql

   .. rubric:: Install Mysql
      :name: install-mysql

   ::

       sudo yum install mysql-server
       sudo mysql-server start

   .. rubric:: Configure Mysql
      :name: configure-mysql

   ::

        # set root password
       sudo mysql_secure_installation

   .. rubric:: Setup Database
      :name: setup-database

   ::

       mysql -u root -p
       create database schema_registry;
       CREATE USER 'registry_user'@'localhost' IDENTIFIED BY 'registry_password';
       GRANT ALL PRIVILEGES ON schema_registry.* TO 'registry_user'@'localhost' WITH GRANT OPTION;
       commit;

5. Configure registry.yaml

::

  cp conf/registry-mysql-example.yaml conf/registry.yaml

Edit the following section to add appropriate database and user settings

::

 storageProviderConfiguration:
  providerClass:"com.hortonworks.registries.storage.impl.jdbc.JdbcStorageManager"
    properties:
      db.type: "mysql"
      queryTimeoutInSecs: 30
      db.properties:
        dataSourceClassName: "com.mysql.jdbc.jdbc2.optional.MysqlDataSource"
        dataSource.url: "jdbc:mysql://localhost/schema_registry"
        dataSource.user:"registry_user"
        dataSource.password: "registry_password"


6. Run bootstrap scripts

::

  $REGISTRY_HOME/bootstrap/boostrap-storage.sh


7. Start the registry server

``sudo ./bin/registry start``


OS X
----

1. Download latest Schema Registry binaries from here
   https://github.com/hortonworks/registry/releases

2. edit $REGISTRY\_HOME/conf/registry-env.sh, add the following

   ::

           export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)

3. Setup Database

   .. rubric:: 3.1 Mysql
      :name: mysql-1

   .. rubric:: Install Mysql
      :name: install-mysql-1

   ::

       brew install mysql
       launchctl load ~/Library/LaunchAgents/homebrew.mxcl.mysql.plist
       export MYSQL_PATH=/usr/local/Cellar/mysql/5.6.27
       export PATH=$PATH:$MYSQL_PATH/bin

   .. rubric:: Configure Mysql
      :name: configure-mysql

   ::

       mysqladmin -u root password 'yourpassword'
       mysql -u root -p

   .. rubric:: Setup Database
      :name: setup-database-1

   ::

       mysql -u root -p
       create database schema_registry;
       CREATE USER 'registry_user'@'localhost' IDENTIFIED BY 'registry_password';
       GRANT ALL PRIVILEGES ON schema_registry.* TO 'registry_user'@'localhost' WITH GRANT OPTION;
       commit;

4. Configure registry.yaml

::

  cp conf/registry-mysql-example.yaml conf/registry.yaml

Edit the following section to add appropriate database and user settings

::

 storageProviderConfiguration:
  providerClass:"com.hortonworks.registries.storage.impl.jdbc.JdbcStorageManager"
    properties:
      db.type: "mysql"
      queryTimeoutInSecs: 30
      db.properties:
        dataSourceClassName: "com.mysql.jdbc.jdbc2.optional.MysqlDataSource"
        dataSource.url: "jdbc:mysql://localhost/schema_registry"
        dataSource.user:"registry_user"
        dataSource.password: "registry_password"

5. Run bootstrap scripts

::

  $REGISTRY_HOME/bootstrap/boostrap-storage.sh


6. Start the registry server

``sudo ./bin/registry start``


Running Schema Registry in Docker Mode
--------------------------------------

1. Install and run Docker Engine from https://docs.docker.com/engine/installation/

2. Schema Registry can run with three database types: mysql, oracle and postgres. Use the corresponding docker-compose
   configuration file accordingly to connect with that particular db type. Also, edit this configuration to update the
   ports / db credentials.

    a) docker-compose-mysql.yml
    b) docker-compose-oracle.yml
    c) docker-compose-postgres.yml

3. To create Schema Registry docker image,
::

    cd $REGISTRY_HOME/docker
    docker-compose -f docker-compose-`db_type`.yml build [OR]

    cd $REGISTRY_HOME/docker/images/registry
    docker build -t registry . --build-arg "REGISTRY_VERSION=0.5.0"

3. Use `docker images` command to confirm that the `schema-registry` image is stored in your local repository

4. To run the schema registry from image in detached mode,
::

   docker-compose -f docker-compose-`db_type`.yml up -d registry

5. To stop the schema registry application,
::

   docker-compose -f docker-compose-`db_type.yml` stop [`service_name` (optional)]
   docker-compose -f docker-compose-`db_type.yml` rm [`service-name` (optional)]

6. To view the registry / container logs,
::

    docker container ls
    docker logs -f `CONTAINER_ID`

7. To login into the docker container, (Schema Registry working dir is /opt)
::

    docker exec -it `CONTAINER_ID` /bin/bash [OR]
    docker-compose -f docker-compose-`db_type`.yml exec `service-name` /bin/bash

8. To scale the Schema Registry application. NOTE: One instance of Schema Registry should be up and running
::

    docker-compose -f docker-compose-`db_type`.yml scale registry=5

9. To view client, use "http://localhost:9010" (The app can bind on any available port provided in the allowed range.
   Please check docker-compose-`db-type`.yml file). Use the below command to check the host port which mapped with the
   corresponding container port,
::

    docker port `CONTAINER_ID`

10. Other useful docker commands,
::

    `docker images` - lists all the available images stored in the local repository
    `docker rmi IMAGE_ID` - to delete a particular image. Use `-f` option to delete the image forcefully
    `docker-compose -f docker-compose-`db_type`.yml ps` - to view the running containers

    `docker container ps` - lists all the running containers
    `docker container ps -a` - lists all the running and exited containers
    `docker container stop CONTAINER_ID` - stops the container
    `docker container rm CONTAINER_ID` - removes the container from the memory
    `docker container prune` - removes all the exited containers from the memory

    `docker exec -it `CONTAINER_ID` mysql -uregistry_user -ppassword db_name` - to login into the mysql client shell
    `docker exec -it `CONTAINER_ID` psql -U registry_user -W schema_registry` - to login into the postgres client shell

    `docker-compose -f docker-compose-`db_type`.yml exec psql -U registry_user -W schema_registry` - to login into the postgres client shell
    `docker-compose -f docker-compose-`db_type`.yml exec `service-name` date` - executes the command `date` inside the container and shows the output

11. To run the Schema Registry with the ORACLE database. Download the latest `ojdbc.jar` for the corresponding oracle version
    from `oracle technetwork <http://www.oracle.com/technetwork/database/features/jdbc/jdbc-drivers-12c-download-1958347.html>`_ (12c)
    and copy it to `extlibs` directory before building the registry image.