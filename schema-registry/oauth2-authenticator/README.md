# OAuth2 in Schema Registry

This module contains the server-side code for OAuth2 _authentication_
for Schema Registry. At the moment we only support JWT tokens and
only support client credentials flow (machine-to-machine).

We expect the client's request to contain a bearer token.

We validate the token. The token must contain a "subject" field. This
subject needs to also exist on the Ranger side, since authorization is 
performed by Ranger. We extract the principal from the subject and use
Ranger to check if the client is permitted to perform the action.


## Generating certificates

For testing we needed to generate private keys and certificates. Here are the
steps:

1. Generate an RSA private key and a signing request (CSR)
```
openssl req -newkey rsa:2048 -nodes -keyout domain.key -out domain.csr
```
2. Convert the key and CSR into a self-signed certificate (CRT)
```
openssl req -newkey rsa:2048 -nodes -keyout domain.key -x509 -days 365 -out domain.crt
```
3. Convert key and CRT into PKCS12 format which is compatible by Java keystore.
Notice the "name". This will be the alias in the Java keystore.
```
openssl pkcs12 -export -in domain.crt -inkey domain.key -name oauth2 -out domain.p12
```
4. Generate an empty keystore
```
keytool -genkeypair -alias boguscert -storepass test123 -keypass test123 -keystore testkeystore.jks -dname "CN=Developer, OU=Department, O=Company, L=City, ST=State, C=CA"
keytool -delete -alias boguscert -storepass test123 -keystore testkeystore.jks
```
5. Import the PKCS12 key into the keystore
```
keytool -importkeystore -deststorepass test123 -destkeystore testkeystore.jks -srckeystore H:/tmp/k2/test.p12 -srcstoretype PKCS12
```
6. We can generate a public key for the private key
```
openssl rsa -in domain.key -out domain.pub -pubout -outform PEM
```
7. Java likes PCKS8 format. This is what we use when storing the key as a property.
```
openssl pkcs8 -topk8 -nocrypt -in domain.key -out domain.key.pcks8
```

We can also import the CRT into the keystore. This is not needed if we have
already imported the PCKS12 key, because it included both the key and the
certificate. From the certificate you can get the public key.

However, if you don't have a private key then this might be a good solution.

```
keytool -import -alias oauth_crt -keystore testkeystore.jks -file "H:/tmp/k2/domain.crt"
```

