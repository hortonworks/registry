Feature: Schema Registry Client

  Background: Schema Registry is running
    Given that Atlas is running
    And Atlas integration is enabled
    And that Schema Registry is running

  Scenario: Create a new schema
    When we create a new schema meta "Car" with the following parameters:
    | Name            | Value          |
    | type            | avro           |
    | schemaGroup     | Kafka          |
    | compatibility   | BACKWARD       |
    | validationLevel | ALL            |
    | evolve          | true           |
    | description     | this is a test |
    Then the schema is successfully created
    And after waiting for no more than 10 seconds, a request is sent to Atlas to create a new schema "Car"

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
    And after waiting for no more than 10 seconds, a request is sent to Atlas to create a new version of schema "Car"
    
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
    And after waiting for no more than 10 seconds, a request is sent to Atlas to create a new version of schema "Car"


    When we create a new version for schema "Car" with the following schema:
    """
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "Car",
      "fields": [ { "name": "model", "type":  "string" } ]
    }
    """
    Then the version is successfully created
    And after waiting for no more than 10 seconds, a request is sent to Atlas to create a new version of schema "Car"


    When we update the schema "Car" with the following parameters:
      | Name            | Value     |
      | Name            | Value     |
      | type            | avro      |
      | schemaGroup     | Kafka     |
      | compatibility   | BACKWARD  |
      | validationLevel | ALL       |
      | evolve          | true      |
      | description     | new value |
    Then the schema is successfully updated
    And after waiting for no more than 10 seconds, a request is sent to Atlas to update schema "Car"

    When we check the compatibility of schema "Car" with the following Avro test:
    """
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "Car",
      "fields": [ { "name": "model", "type":  "string" } ]
    }
    """
    Then the Avro test is compatible
