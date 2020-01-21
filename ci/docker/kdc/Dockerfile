FROM centos:7

# install packages neded
RUN yum update -y && \
    yum install -y \
        rng-tools \
        krb5-server \
        krb5-libs \
        krb5-workstation && \
    yum clean all

# copy krb5.conf
COPY ci/docker/kdc/files/krb5.conf /etc/krb5.conf

# make sure acl is setup correctly
COPY ci/docker/kdc/files/kadm5.acl /var/kerberos/krb5kdc/kadm5.acl

# script that will setup kerberos
COPY ci/docker/kdc/files/kerberos_setup.sh /opt/kerberos_setup.sh

RUN chmod 755 /opt/kerberos_setup.sh

ENV REALM=K8S.COM

EXPOSE 88 464 749
ENTRYPOINT ["/opt/kerberos_setup.sh"]
