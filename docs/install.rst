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

1. Install and run Docker Engine from https://docs.docker.com/engine/installation/ and jq (brew install jq)

2. To build Schema Registry and all the dependent services,
::

    sh kdc-registry.sh build

3. If you want to build Schema Registry from a particular tag release,
::

    export download_url="https://github.com/hortonworks/registry/releases/download/v0.5.0/hortonworks-registry-0.5.0.tar.gz"
    sh kdc-registry.sh build

4. To start the application,
::

    sh kdc-registry.sh start

5. To stop the application with all the services,
::

    sh kdc-registry.sh stop

6. To remove the images, user network and stopped containers,
::

    sh kdc-registry.sh clean

7. To login into the container,
::

    sh kdc-registry.sh shell `CONTAINER_NAME`

8. To view the container logs,
::

    sh kdc-registry.sh logs `CONTAINER_NAME`

9. To start the registry application with more than 2 instances, then
::

    export scale=5
    sh kdc-registry.sh start

10. To view the active containers,
::

    sh kdc-registry.sh ps

11. To view all the containers including the stopped ones,
::

    sh kdc-registry.sh ps-all


10. Other useful docker commands,
::

    `docker port CONTAINER_ID` - displays the ports which are exposed to the outside world
    `docker images` - lists all the available images stored in the local repository
    `docker rmi IMAGE_ID` - to delete a particular image. Use `-f` option to delete the image forcefully

    `docker container ps` - lists all the running containers
    `docker container ps -a` - lists all the running and exited containers
    `docker container start CONTAINER_ID` - starts the container
    `docker container stop CONTAINER_ID` - stops the container
    `docker container rm CONTAINER_ID` - removes the container from the memory
    `docker container prune` - removes all the exited containers from the memory

    `docker exec -it `CONTAINER_ID` psql -U registry_user -W schema_registry` - to login into the postgres client shell

11. To run the Schema Registry with the ORACLE database. Download the latest `ojdbc.jar` for the corresponding oracle version
    from `oracle technetwork <http://www.oracle.com/technetwork/database/features/jdbc/jdbc-drivers-12c-download-1958347.html>`_ (12c)
    and copy it to `extlibs` directory before building the registry image.