# Registries
Hortonworks  registries repository

# Build
mvn clean install

cd registry-dist

mvn clean package

cd target

unzip hortonworks-registry-0.1.0-SNAPSHOT.zip

cd hortonworks-registry-0.1.0-SNAPSHOT

./bin/registry-server-start.sh conf/registry-dev.yaml 
