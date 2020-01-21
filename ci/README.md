#  CI local execution

Start minikube
```
minikube start --vm-driver=hyperkit --cpus 4 --memory 8192
```
Setup tiller if not yet available:
``` 
helm init --wait
```
## Generate images
```
ansible-playbook -i ci/local_inventory -e @ci/local_vars.yaml ci/build-images.yaml -vvv
```
## Build System under test
```
ansible-playbook -i ci/local_inventory -e @ci/local_vars.yaml ci/gate-sut.yaml  -vvv
```
## Run system tests
``` 
ansible-playbook -i ci/local_inventory -e @ci/local_vars.yaml ci/gate-st.yaml  -vvv
```

## Local variables configuration in ci/local_vars.yaml
### srcdir: "../"
```
srcdir: "../"
```
### namespace: default
```
namespace: default
```
### tiller_namespace: kube-system 
```
tiller_namespace: kube-system 
```
### mit_kerberos
```
mit_kerberos: "/usr/local/opt/krb5/bin/"
```
### patchset
```
patchset: localbuild
```

### docker_host
```
docker_host: 'xxx'
```
Fetch docker_host values from mininkube, use the DOCKER_HOST variable in ci/local_vars.yaml
```
minikube docker-env                                                                                                            

set -gx DOCKER_TLS_VERIFY "1";
set -gx DOCKER_HOST "tcp://192.168.64.30:2376";
set -gx DOCKER_CERT_PATH "/Users/<your user>/.minikube/certs";
set -gx MINIKUBE_ACTIVE_DOCKERD "minikube";
```
### repository
Set empty value to use minikube docker images
```
repository: """
```
### database
inmemory | mysql | postgresql
```
database: inmemory
```
### security
none | kerberos
```
security: none
```
