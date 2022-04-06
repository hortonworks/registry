Feature: Schema Registry REST API

  Scenario: Schema Registry is running with TLS enabled
    Given that Schema Registry is running with 2-way TLS enabled
    When we create a new schema meta "Car" with the following parameters:
      | Name            | Value          |
      | type            | avro           |
      | schemaGroup     | Kafka          |
      | compatibility   | BACKWARD       |
      | validationLevel | ALL            |
      | evolve          | true           |
      | description     | this is a test |
    Then the schema is successfully created

    When we create a new version for schema "Car" with the following schema:
    """
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "Car",
      "fields": [
                { "name": "model", "type":  "string" },
                { "name": "color", "type":  "string", "default": "blue" },
                { "name": "price", "type":  "string", "default": "0" }
      ]
    }
    """
    Then the version is successfully created

    When we create a new version for schema "Car" with the following schema:
    """
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "Car",
      "fields": [
                { "name": "model", "type":  "string" },
                { "name": "color", "type":  "string", "default": "blue" }
      ]
    }
    """
    Then the version is successfully created

    When we search for aggregated schemas with the following parameters:
      | Name | Description | OrderBy |
      | Car  |             |         |

    Then the resulting list size is 1
    And the resulting list will contain the following 2 versions:
      | 1    |
      | 2    |


  Scenario: Schema Registry is running with invalid TLS rule
    Given that Schema Registry is running with 2-way TLS enabled using rules: "RULE:^.*[Cc][Nn]=(invalid).*$/$1/L"
    When we create a new schema meta "Car2" with the following parameters:
      | Name            | Value          |
      | type            | avro           |
      | schemaGroup     | Kafka          |
      | compatibility   | BACKWARD       |
      | validationLevel | ALL            |
      | evolve          | true           |
      | description     | this is a test |
    Then the schema failed to be created with error "HTTP 403 Forbidden"
