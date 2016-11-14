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

