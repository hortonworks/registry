/*
 * Copyright 2016-2023 Cloudera, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.schemaregistry.client;

import com.hortonworks.registries.auth.client.KerberosAuthenticator;
import com.hortonworks.registries.common.catalog.CatalogResponse;
import com.hortonworks.registries.schemaregistry.CompatibilityResult;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SchemaVersionMergeResult;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaBranchDeletionException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchAlreadyExistsException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.exceptions.RegistryRetryableException;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import com.hortonworks.registries.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import com.hortonworks.registries.shaded.com.fasterxml.jackson.databind.JsonNode;
import com.hortonworks.registries.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.registries.shaded.javax.ws.rs.BadRequestException;
import com.hortonworks.registries.shaded.javax.ws.rs.ClientErrorException;
import com.hortonworks.registries.shaded.javax.ws.rs.ForbiddenException;
import com.hortonworks.registries.shaded.javax.ws.rs.InternalServerErrorException;
import com.hortonworks.registries.shaded.javax.ws.rs.NotAcceptableException;
import com.hortonworks.registries.shaded.javax.ws.rs.NotAllowedException;
import com.hortonworks.registries.shaded.javax.ws.rs.NotAuthorizedException;
import com.hortonworks.registries.shaded.javax.ws.rs.NotFoundException;
import com.hortonworks.registries.shaded.javax.ws.rs.NotSupportedException;
import com.hortonworks.registries.shaded.javax.ws.rs.ServerErrorException;
import com.hortonworks.registries.shaded.javax.ws.rs.ServiceUnavailableException;
import com.hortonworks.registries.shaded.javax.ws.rs.WebApplicationException;
import com.hortonworks.registries.shaded.javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ResponseHandler {

    public ResponseHandler() {

    }

    public void checkSchemaVersionKeyResponse(Response response) throws IncompatibleSchemaException, InvalidSchemaException {
        int status = response.getStatus();
        String msg = response.readEntity(String.class);
        if (status == Response.Status.BAD_REQUEST.getStatusCode() || status == Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
            CatalogResponse catalogResponse;
            catalogResponse = SchemaRegistryClient.readCatalogResponse(msg);
            if (catalogResponse == null) {
                return;
            }
            if (CatalogResponse.ResponseMessage.INCOMPATIBLE_SCHEMA.getCode() == catalogResponse.getResponseCode()) {
                throw new IncompatibleSchemaException(catalogResponse.getResponseMessage());
            } else if (CatalogResponse.ResponseMessage.INVALID_SCHEMA.getCode() == catalogResponse.getResponseCode()) {
                throw new InvalidSchemaException(catalogResponse.getResponseMessage());
            } else {
                throw new RuntimeException(catalogResponse.getResponseMessage());
            }
        }
    }

    public SchemaVersionKey handleSchemaVersionKeyResponse(SchemaMetadataInfo schemaMetadataInfo,
                                                           Response response) {
        throwIfNotStatusCreated(response);
        String msg = response.readEntity(String.class);
        Integer version = readEntity(msg, Integer.class);
        return new SchemaVersionKey(schemaMetadataInfo.getSchemaMetadata().getName(), version);
    }

    public CompatibilityResult handleCompatibilityResultResponse(Response response) {
        throwIfNotStatusOk(response);
        String msg = response.readEntity(String.class);
        return readEntity(msg, CompatibilityResult.class);
    }

    public void checkSchemaVersionMergeResultResponse(Response response, Long schemaVersionId)
        throws IncompatibleSchemaException, SchemaNotFoundException {
        int status = response.getStatus();
        if (status == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new SchemaNotFoundException(response.readEntity(String.class), String.valueOf(schemaVersionId));
        } else if (status == Response.Status.BAD_REQUEST.getStatusCode()) {
            throw new IncompatibleSchemaException(response.readEntity(String.class));
        }
    }

    public SchemaVersionMergeResult handleSchemaVersionMergeResultResponse(Response response) {
        throwIfNotStatusOk(response);
        String msg = response.readEntity(String.class);
        return readEntity(msg, SchemaVersionMergeResult.class);
    }

    public void checkSchemaBranchResponse(Response response)
        throws SchemaNotFoundException, SchemaBranchAlreadyExistsException {
        int status = response.getStatus();
        if (status == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new SchemaNotFoundException(response.readEntity(String.class));
        } else if (status == Response.Status.CONFLICT.getStatusCode()) {
            throw new SchemaBranchAlreadyExistsException(response.readEntity(String.class));
        }
    }

    public SchemaBranch handleSchemaBranchResponse(Response response) {
        throwIfNotStatusOk(response);
        String msg = response.readEntity(String.class);
        return readEntity(msg, SchemaBranch.class);
    }

    public void checkSchemaBranchesResponse(Response response, String schemaName)
        throws SchemaNotFoundException {
        int status = response.getStatus();
        if (status == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new SchemaNotFoundException(response.readEntity(String.class), schemaName);
        }
    }

    public Collection<SchemaBranch> handleSchemaBranchesResponse(Response response) {
        throwIfNotStatusOk(response);
        return parseResponseAsEntities(response.readEntity(String.class), SchemaBranch.class);
    }

    public void checkBranchDeleteResponse(Response response) throws InvalidSchemaBranchDeletionException {
        int status = response.getStatus();
        if (status == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new SchemaBranchNotFoundException(response.readEntity(String.class));
        } else if (status == Response.Status.BAD_REQUEST.getStatusCode()) {
            throw new InvalidSchemaBranchDeletionException(response.readEntity(String.class));
        }
    }

    public void handleBranchDeleteResponse(Response response) {
        throwIfNotStatusOk(response);
    }

    public void checkSchemaDeleteResponse(Response response, String schemaName) throws SchemaNotFoundException {
        int status = response.getStatus();
        if (status == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new SchemaNotFoundException(response.readEntity(String.class), schemaName);
        }
    }

    public void checkSchemaVersionDeleteResponse(Response response, String schemaName)
        throws SchemaNotFoundException, SchemaLifecycleException {
        String msg = "null";
        if (response.hasEntity()) {
            msg = response.readEntity(String.class);
        }
        switch (Response.Status.fromStatusCode(response.getStatus())) {
            case NOT_FOUND:
                throw new SchemaNotFoundException(msg, schemaName);
            case BAD_REQUEST:
                throw new SchemaLifecycleException(msg);
            default:
                break;
        }
    }

    public void checkSchemaLifeCycleResponse(Response response) throws SchemaNotFoundException, SchemaLifecycleException {
        int status = response.getStatus();
        if (status == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new SchemaNotFoundException(response.readEntity(String.class));
        } else if (status == Response.Status.BAD_REQUEST.getStatusCode()) {
            CatalogResponse catalogResponse = SchemaRegistryClient.readCatalogResponse(response.readEntity(String.class));
            if (catalogResponse == null) {
                return;
            }
            if (catalogResponse.getResponseCode() == CatalogResponse.ResponseMessage.INCOMPATIBLE_SCHEMA.getCode()) {
                throw new SchemaLifecycleException(new IncompatibleSchemaException(catalogResponse.getResponseMessage()));
            }
            throw new SchemaLifecycleException(catalogResponse.getResponseMessage());
        }
    }

    public boolean handleSchemaLifeCycleResponse(Response response) {
        throwIfNotStatusOk(response);
        String msg = response.readEntity(String.class);
        return readEntity(msg, Boolean.class);
    }

    public <T> List<T> parseResponseAsEntities(String response, Class<T> clazz) {
        List<T> entities = new ArrayList<>();
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(response);
            Iterator<JsonNode> it = node.get("entities").elements();
            while (it.hasNext()) {
                entities.add(mapper.treeToValue(it.next(), clazz));
            }
        } catch (Exception e) {
            if (e instanceof JsonProcessingException) {
                throw new RegistryRetryableException(e);
            }
            throw new RuntimeException(e);
        }
        return entities;
    }

    public <T> T readEntity(String response, Class<T> clazz) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(response, clazz);
        } catch (Exception e) {
            if (e instanceof JsonProcessingException) {
                throw new RegistryRetryableException(e);
            }
            throw new RuntimeException(e);
        }
    }

    public void throwIfRetryable(Response response,
                                 boolean kerberosAuthEnabled,
                                 boolean basicAuthLoginConfigured) {
        // Some erroneous responses might be received e.g. during rolling restart, these should be retried
        if (isSpnegoResponse(response)) {
            throw new RegistryRetryableException("Received SPNEGO related 401 response, retry");
        }
        if (response.getStatus() == Response.Status.SERVICE_UNAVAILABLE.getStatusCode()) {
            throw new RegistryRetryableException("Received HTTP " + response.getStatus() + " response, retry");
        }
        if (response.getStatus() == Response.Status.FORBIDDEN.getStatusCode() && basicAuthLoginConfigured) {
            if (response.getCookies() != null &&
                response.getCookies().keySet().contains("KNOXSESSIONID")) {
                throw new RegistryRetryableException("Received HTTP " + response.getStatus() +
                    " (Forbidden) response, retry request as its cause is possibly just an intermittent Knox" +
                    " gateway outage");
            }
        }
        if (response.getStatus() == Response.Status.FORBIDDEN.getStatusCode() && kerberosAuthEnabled) {
            boolean authorizationFailure = false;
            try {
                CatalogResponse catalogResponse = response.readEntity(CatalogResponse.class);
                if (catalogResponse != null &&
                    catalogResponse.getResponseCode() == CatalogResponse.ResponseMessage.ACCESS_DENIED.getCode()) {
                    authorizationFailure = true;
                }
            } catch (Exception e) {
                // Assume the response is not authorization failure if the response entity is not a proper catalog response
            }
            if (!authorizationFailure) {
                // Retry if the HTTP 403 response is not due to authorization failure
                throw new RegistryRetryableException("Received HTTP " + response.getStatus() +
                    " (Forbidden) response, retry request as its cause is possibly just a" +
                    " false-positive replay-attack");
            }
        }
        if (response.getStatus() == Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
            throw new RegistryRetryableException("Received HTTP " + response.getStatus() + " response, retry");
        }
    }

    private boolean isSpnegoResponse(Response response) {
        if (response == null) {
            return false;
        }
        if (response.getStatus() != Response.Status.UNAUTHORIZED.getStatusCode()) {
            return false;
        }
        String authHeaderValue = response.getHeaderString(KerberosAuthenticator.WWW_AUTHENTICATE);
        if (StringUtils.isBlank(authHeaderValue)) {
            return false;
        }
        return KerberosAuthenticator.NEGOTIATE.equalsIgnoreCase(authHeaderValue.trim());
    }

    private void throwIfNotStatusOk(Response response) {
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new WebApplicationException("Unexpected HTTP response (status code " + response.getStatus() + ")",
                response);
        }
    }

    private void throwIfNotStatusCreated(Response response) {
        if (response.getStatus() != Response.Status.CREATED.getStatusCode()) {
            throw new WebApplicationException("Unexpected HTTP response (status code " + response.getStatus() + ")",
                response);
        }
    }

    public void throwOnFailedResponse(Response response) {
        Response.Status status = Response.Status.fromStatusCode(response.getStatus());

        if (status == null) {
            throwOnErroneousResponseFamily(response);
        } else {
            switch (status) {
                case BAD_REQUEST:
                    throw new BadRequestException(response);
                case UNAUTHORIZED:
                    throw new NotAuthorizedException(response);
                case FORBIDDEN:
                    throw new ForbiddenException(response);
                case NOT_FOUND:
                    throw new NotFoundException(response);
                case METHOD_NOT_ALLOWED:
                    throw new NotAllowedException(response);
                case NOT_ACCEPTABLE:
                    throw new NotAcceptableException(response);
                case UNSUPPORTED_MEDIA_TYPE:
                    throw new NotSupportedException(response);
                case INTERNAL_SERVER_ERROR:
                    throw new InternalServerErrorException(response);
                case SERVICE_UNAVAILABLE:
                    throw new ServiceUnavailableException(response);
                default:
                    throwOnErroneousResponseFamily(response);
                    break;
            }
        }
    }

    private void throwOnErroneousResponseFamily(Response response) {
        switch (response.getStatusInfo().getFamily()) {
            case CLIENT_ERROR:
                throw new ClientErrorException(response);
            case SERVER_ERROR:
                throw new ServerErrorException(response);
            default:
                break;
        }
    }

}
