Feature: Schema Registry Client

  Background: Schema Registry is running
    Given that Schema Registry is running

  Scenario: Create a schema meta only and then delete it

    When we create a new schema meta "Head" with the following parameters:
      | Name            | Value          |
      | type            | avro           |
      | schemaGroup     | Kafka          |
      | compatibility   | BACKWARD       |
      | validationLevel | ALL            |
      | evolve          | true           |
      | description     | this is a test |
    Then the schema is successfully created

    When we delete the schema "Head"
    And we search for aggregated schemas with the following parameters:
      | Name | Description | OrderBy |
      | Head |             |         |
    Then the resulting list size is 0


  Scenario: Create a schema with versions and then delete it

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

    When we delete the schema "Car"
    And we search for aggregated schemas with the following parameters:
      | Name | Description | OrderBy |
      | Car  |             |         |

    Then the resulting list size is 0


  Scenario: Create a new schema, init a new client and then delete it

    When we create a new schema meta "Car2" with the following parameters:
      | Name            | Value          |
      | type            | avro           |
      | schemaGroup     | Kafka          |
      | compatibility   | BACKWARD       |
      | validationLevel | ALL            |
      | evolve          | true           |
      | description     | this is a test |
    Then the schema is successfully created

    When we create a new version for schema "Car2" with the following schema:
    """
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "Car2",
      "fields": [
                { "name": "model", "type":  "string" },
                { "name": "color", "type":  "string", "default": "blue" },
                { "name": "price", "type":  "string", "default": "0" }
      ]
    }
    """
    Then the version is successfully created

    # We do this in order to clear any cache the previous client may have had
    When we initialize a new schema registry client
    And we delete the schema "Car2"
    And we search for aggregated schemas with the following parameters:
      | Name | Description | OrderBy |
      | Car2 |             |         |

    Then the resulting list size is 0
