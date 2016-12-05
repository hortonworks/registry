# Registries
Hortonworks  registries repository

# Build
mvn clean install

cd registry-dist

mvn clean package

cd target

unzip hortonworks-registry-0.1.0.zip

cd hortonworks-registry-0.1.0

./bin/registry-server-start.sh conf/registry-dev.yaml 

# API doc

http://localhost:9090/api/swagger/ ( make sure to change the port if you are using different port in the registry-dev.yaml config)


# to persist schemas across restarts
1. install mysql
2. create database 'schema_registry'
3. give user permissions:
    a. create user ''@'localhost';
    b. GRANT ALL PRIVILEGES ON *.* to ''@'localhost';
4. change from:
# storage provider configuration
storageProviderConfiguration:
 providerClass: "org.apache.registries.storage.impl.memory.InMemoryStorageManager"
to:
# MySQL based jdbc provider configuration is:
storageProviderConfiguration:
providerClass: "org.apache.registries.storage.impl.jdbc.JdbcStorageManager"
    properties:
        db.type: "mysql"
        queryTimeoutInSecs: 30
        db.properties:
            dataSourceClassName: "com.mysql.jdbc.jdbc2.optional.MysqlDataSource"
            dataSource.url: "jdbc:mysql://localhost/schema_registry"
            dataSource.user: ""
            dataSource.password: ""

