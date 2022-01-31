Feature: Authentication with OAuth2

  Background: Schema Registry is running
    Given that an OAuth2 server exists
    And that Schema Registry is running with OAuth2

  Scenario: Basic mechanism
    When we send an HTTP GET request to "/api/v1/schemaregistry/schemas" with the following query parameters:
      | Name           | Value       |
      | name           | NewSchema   |
      | _orderByFields | timestamp,d |
    Then the response code is 403

    When we authenticate with OAuth2
    And we send an HTTP GET request to "/api/v1/schemaregistry/schemas" with the following query parameters:
      | Name           | Value       |
      | name           | NewSchema   |
      | _orderByFields | timestamp,d |
    Then the response code is 200


#  Scenario: Create a new schema
#    When we create a new schema meta "Car" with the following parameters:
#    | Name            | Value          |
#    | type            | avro           |
#    | schemaGroup     | Kafka          |
#    | compatibility   | BACKWARD       |
#    | validationLevel | ALL            |
#    | evolve          | true           |
#    | description     | this is a test |
#    Then the schema is successfully created
#
#    When we create a new version for schema "Car" with the following schema:
#    """
#    {
#      "type": "record",
#      "namespace": "com.cloudera",
#      "name": "Car",
#      "fields": [
#                { "name": "model", "type":  "string" },
#                { "name": "color", "type":  "string", "default": "blue" },
#                { "name": "price", "type":  "string", "default": "0" }
#      ]
#    }
#    """
#    Then the version is successfully created
#
#    When we create a new version for schema "Car" with the following schema:
#    """
#    {
#      "type": "record",
#      "namespace": "com.cloudera",
#      "name": "Car",
#      "fields": [
#                { "name": "model", "type":  "string" },
#                { "name": "color", "type":  "string", "default": "blue" }
#      ]
#    }
#    """
#    Then the version is successfully created
#
#    When we search for aggregated schemas with the following parameters:
#    | Name | Description | OrderBy |
#    | Car  |             |         |
#
#    Then the resulting list size is 1
#    And the resulting list will contain the following 2 versions:
#    | 1    |
#    | 2    |