Feature: Schema Registry REST API

  Background: Schema Registry is running
    Given that Schema Registry is running

  Scenario: Create a new json schema
    When we send an HTTP POST request to "/api/v1/schemaregistry/schemas" with the following parameters:
      | Name            | Value    |
      | name            | Perfume  |
      | schemaGroup     | Kafka    |
      | type            | json     |
      | description     | desc     |
      | compatibility   | BACKWARD |
      | validationLevel | ALL      |
    Then the response code is 201

    When we send an HTTP GET request to "/api/v1/schemaregistry/schemas" with the following query parameters:
      | Name           | Value       |
      | name           | Perfume     |
      | _orderByFields | timestamp,d |
    Then the response code is 200
    And the response will contain 1 entity

    When we send an HTTP POST request to "/api/v1/schemaregistry/schemas/Perfume/versions?branch=MASTER&disableCanonicalCheck=false" with the following payload:
    """
    {
      "description": "desc",
      "schemaText": "{\"type\": \"object\",\"additionalProperties\": false,\"title\": \"Perfume\",\"$schema\": \"http: //json-schema.org/draft-07/schema#\",\"properties\": {\"name\": {\"oneOf\": [{\"type\": \"null\",\"title\": \"Not included\"},{\"type\": \"string\"}]},\"version\": {\"oneOf\": [{\"type\": \"null\",\"title\": \"Not included\"},{\"type\": \"integer\"}]}}}"
    }
    """
    Then the response code is 201
    
    When we send an HTTP POST request to "/api/v1/schemaregistry/schemas/Perfume/versions?branch=MASTER&disableCanonicalCheck=false" with the following payload:
    """
    {
      "description": "desc",
      "schemaText": "{\"type\": \"object\",\"additionalProperties\": false,\"title\": \"Perfume\",\"$schema\": \"http: //json-schema.org/draft-07/schema#\",\"properties\": {\"name\": {\"oneOf\": [{\"type\": \"null\",\"title\": \"Not included\"},{\"type\": \"integer\"}]},\"version\": {\"oneOf\": [{\"type\": \"null\",\"title\": \"Not included\"},{\"type\": \"integer\"}]}}}"
    }
    """
    Then the response code is 201

    When we send an HTTP GET request to "/api/v1/schemaregistry/schemas" with the following query parameters:
      | Name           | Value       |
      | name           | Perfume     |
      | _orderByFields | timestamp,d |
    Then the response code is 200
    And the response will contain 1 entity

    When we send an HTTP GET request to "/api/v1/schemaregistry/schemas/Perfume/versions?branch=MASTER"
    Then the response code is 200
    And the response will contain 2 entities

    When we send an HTTP POST request to "/api/v1/schemaregistry/schemas/Perfume/versions?branch=MASTER&disableCanonicalCheck=false" with the following payload:
    """
    {
      "description": "json with invalid type",
      "schemaText": "{\"type\": \"object\",\"additionalProperties\": false,\"title\": \"Perfume\",\"properties\": {\"name\": {\"oneOf\": [{\"type\": \"hippopotamus\",\"title\": \"Not included\"},{\"type\": \"string\"}]},\"version\": {\"oneOf\": [{\"type\": \"null\",\"title\": \"Not included\"},{\"type\": \"integer\"}]}}}"
    }
    """
    Then the response code is 500