Feature: Integration with Atlas

  Background: Schema Registry is running
    Given that Atlas is running
    And Atlas integration is enabled
    And the model should be created in Atlas
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
    And after waiting for no more than 10 seconds, a request is sent to Atlas to create a new version of schema "Car v1"
    
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
    And after waiting for no more than 10 seconds, a request is sent to Atlas to create a new version of schema "Car v2"


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
    And after waiting for no more than 10 seconds, a request is sent to Atlas to create a new version of schema "Car v3"


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

  Scenario: Creating a schema with a preexisting topic
    Given there is preexisting "Car2" topic
    When we create a new schema meta "Car2" with the following parameters:
      | Name            | Value          |
      | type            | avro           |
      | schemaGroup     | Kafka          |
      | compatibility   | BACKWARD       |
      | validationLevel | ALL            |
      | evolve          | true           |
      | description     | Atlas test     |
    Then the schema is successfully created
    And after waiting for no more than 10 seconds, a request is sent to Atlas to create a new schema "Car2"
    And a new relationship was made in Atlas between the "Car2" schema and the "Car2" topic
    And no failed Atlas events remain

  Scenario: Creating a schema without an existing topic
    Given no "Car3" topic exists
    When we create a new schema meta "Car3" with the following parameters:
      | Name            | Value          |
      | type            | avro           |
      | schemaGroup     | Kafka          |
      | compatibility   | BACKWARD       |
      | validationLevel | ALL            |
      | evolve          | true           |
      | description     | Atlas test     |
    Then the schema is successfully created
    And after waiting for no more than 10 seconds, a request is sent to Atlas to create a new schema "Car3"
    But no request was made to Atlas to connect the "Car3" schema with any topic
    And no failed Atlas events remain
