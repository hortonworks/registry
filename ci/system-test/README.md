# Schema Registry Test Code

## Introduction
These are tests for Schema Registry. It has been written in Java8. We use surefire & testng for running tests.
The code for this live in [schema-registry-qe](https://github.com/hortonworks/schema-registry-qe) repo.

The tests are backend tests and test the API aspects of schema registry.

## Api tests
These test are strongly tied to the schema registry product and are independent of ui. They should continue to irrespective
of ui changes. In particular in these tests we should not:
 * Fetch and parse html from streamline url
 * Perform operations by running ssh (only exception is HA testing)

For such operations use rest api provided by the product or request one if necessary.

# Test development
## Nightly execution
#Has to be filled once this is completed

## Properties file
All the essential properties such as cluster & credentials details must be picked from
`schema-registry.properties`. This file is populated with right values by certification code before tests are started.

## Running tests
Change following parameter in `streamline-automation.properties` file.
* registry.urls=<registry url. Present all urls in case of an HA cluster>

## License
We need hortonworks license to be present. This is necessary because we also share our test code with our partners.
To enforce presence of license we are using apache-rat-plugin. If the correct license is not found the build will fail.

## Gerrit
All the patches must go through Gerrit. All it does is run `mvn -B clean package -DskipTests` and will give feedback
 within 5 mins. If your code does not pass Gerrit checks it will break nightly runs.