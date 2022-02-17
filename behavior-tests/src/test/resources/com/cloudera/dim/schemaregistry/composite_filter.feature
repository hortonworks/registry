Feature: Composite Filter

  Scenario: No filters
    Given that Schema Registry is running

    When we send an HTTP GET request to "/api/v1/schemaregistry/schemas"
    Then the response code is 200


  Scenario: One auth filter
    Given that Schema Registry is running with the following servlet filters:
      | ClassName                                                   | Param Key | Param Value                                       |
      | com.hortonworks.registries.auth.server.AuthenticationFilter | composite | true                                              |
      | -                                                           | type      | com.cloudera.dim.schemaregistry.auth.FirstFilter  |
      | com.hortonworks.registries.auth.server.AuthTerminatorFilter |           |                                                   |

    When we send an HTTP GET request to "/api/v1/schemaregistry/schemas"
    Then the response code is 403

    When we send an HTTP GET request to "/api/v1/schemaregistry/schemas" with the following headers:
      | Key           | Value |
      | X-Target-Auth | first |
    Then the response code is 200


  Scenario: Two auth filters
    Given that Schema Registry is running with the following servlet filters:
      | ClassName                                                   | Param Key | Param Value                                       |
      | com.hortonworks.registries.auth.server.AuthenticationFilter | composite | true                                              |
      | -                                                           | type      | com.cloudera.dim.schemaregistry.auth.FirstFilter  |
      | com.hortonworks.registries.auth.server.AuthenticationFilter | composite | true                                              |
      | -                                                           | type      | com.cloudera.dim.schemaregistry.auth.SecondFilter |
      | com.hortonworks.registries.auth.server.AuthTerminatorFilter |           |                                                   |

    When we send an HTTP GET request to "/api/v1/schemaregistry/schemas"
    Then the response code is 403

    When we send an HTTP GET request to "/api/v1/schemaregistry/schemas" with the following headers:
      | Key           | Value |
      | X-Target-Auth | first |
    Then the response code is 200

    When we send an HTTP GET request to "/api/v1/schemaregistry/schemas" with the following headers:
      | Key           | Value  |
      | X-Target-Auth | second |
    Then the response code is 200

    When we send an HTTP GET request to "/api/v1/schemaregistry/schemas" with the following headers:
      | Key           | Value |
      | X-Target-Auth | third |
    Then the response code is 403
