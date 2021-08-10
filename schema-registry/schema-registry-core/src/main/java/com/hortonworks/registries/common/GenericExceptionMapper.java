/*
 * Copyright 2016-2019 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.common;

import com.hortonworks.registries.common.catalog.CatalogResponse;
import com.hortonworks.registries.common.exception.service.exception.WebServiceException;
import com.hortonworks.registries.common.exception.service.exception.request.BadRequestException;
import com.hortonworks.registries.common.exception.service.exception.server.UnhandledServerException;
import com.hortonworks.registries.common.util.WSUtils;
import com.hortonworks.registries.schemaregistry.authorizer.exception.AuthorizationException;
import com.hortonworks.registries.schemaregistry.authorizer.exception.RangerException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.webservice.validator.exception.InvalidJarFileException;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaBranchDeletionException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchAlreadyExistsException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.UnsupportedSchemaTypeException;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import com.hortonworks.registries.storage.exception.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Path;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.io.FileNotFoundException;
import java.lang.reflect.UndeclaredThrowableException;

/**
 * GenericExceptionMapper handles exceptions from SchemaRegistryResource and ConfluentSchemaRegistryCompatibleResource 
 * and translates them to to http response code translation.
 */
@Provider
public class GenericExceptionMapper implements ExceptionMapper<Throwable> {
    private static final Logger LOG = LoggerFactory.getLogger(GenericExceptionMapper.class);

    @Override
    public Response toResponse(Throwable ex) {
        if (ex instanceof ProcessingException
                || ex instanceof NullPointerException) {
            return BadRequestException.of().getResponse();
        } else if (ex instanceof InvalidJarFileException) {
            LOG.debug("Invalid JAR file. ", ex);
            return WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.BAD_REQUEST_WITH_MESSAGE, ex.getMessage());
        } else if (ex instanceof InvalidSchemaBranchDeletionException) {
            return WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.BAD_REQUEST_WITH_MESSAGE, ex.getMessage());
        } else if (ex instanceof InvalidSchemaException) {
            LOG.error("Invalid schema. ", ex);
            return WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.INVALID_SCHEMA, ex.getMessage());
        } else if (ex instanceof AuthorizationException) {
            LOG.debug("Access denied. ", ex);
            return WSUtils.respond(Response.Status.FORBIDDEN, CatalogResponse.ResponseMessage.ACCESS_DENIED, ex.getMessage());
        } else if (ex instanceof SchemaBranchAlreadyExistsException) {
            return WSUtils.respond(Response.Status.CONFLICT, CatalogResponse.ResponseMessage.ENTITY_CONFLICT, ex.getMessage(), ((SchemaBranchAlreadyExistsException) ex).getEntity());
        } else if (ex instanceof FileNotFoundException) {
            LOG.error("No file found. ", ex);
            return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, ex.getMessage());
        } else if (ex instanceof IllegalArgumentException) {
            return WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.BAD_REQUEST_PARAM_MISSING, ex.getMessage());
        } else if (ex instanceof IncompatibleSchemaException) {
            LOG.error("Schema version is incompatible. ", ex);
            return WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.INCOMPATIBLE_SCHEMA, ex.getMessage());
        } else if (ex instanceof UnsupportedSchemaTypeException) {
            LOG.error("Encountered error while checking schema type. ", ex);
            return WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.UNSUPPORTED_SCHEMA_TYPE, ex.getMessage());
        } else if (ex instanceof SchemaNotFoundException) {
            String entity = ((SchemaNotFoundException) ex).getEntity();
            LOG.info("No schemas found with entity: [{}]", entity);
            return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, entity);
        } else if (ex instanceof SchemaLifecycleException) {
            LOG.error("Encountered error. ", ex);
            return WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.BAD_REQUEST, ex.getMessage());
        } else if (ex instanceof RangerException) {
            return WSUtils.respond(Response.Status.BAD_GATEWAY, CatalogResponse.ResponseMessage.EXTERNAL_ERROR, ex.getMessage());
        } else if (ex instanceof SchemaBranchNotFoundException) {
            return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, ex.getMessage());
        } else if (ex instanceof StorageException) {
            return WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.ENTITY_CONFLICT, ex.getMessage());
        } else if (ex instanceof WebServiceException) {
            return ((WebServiceException) ex).getResponse();
        }

        if (ex instanceof Exception) {
            if (ex instanceof UndeclaredThrowableException) {
                ex = (Exception) ((UndeclaredThrowableException) ex).getUndeclaredThrowable();
            }
            LOG.error("Encountered error while request: " + ex);
            return WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }
        logUnhandledException(ex);
        return new UnhandledServerException(ex.getMessage()).getResponse();
    }

    private void logUnhandledException(Throwable ex) {
        String errMessage = String.format("Got exception: [%s] / message [%s]",
                                          ex.getClass().getSimpleName(), ex.getMessage());
        StackTraceElement elem = findFirstResourceCallFromCallStack(ex.getStackTrace());
        String resourceClassName = null;
        if (elem != null) {
            errMessage += String.format(" / related resource location: [%s.%s](%s:%d)",
                                        elem.getClassName(), elem.getMethodName(), elem.getFileName(), elem.getLineNumber());
            resourceClassName = elem.getClassName();
        }

        Logger log = getEffectiveLogger(resourceClassName);
        log.error(errMessage, ex);
    }

    private StackTraceElement findFirstResourceCallFromCallStack(StackTraceElement[] stackTrace) {
        for (StackTraceElement stackTraceElement : stackTrace) {
            try {
                Class<?> aClass = Class.forName(stackTraceElement.getClassName());
                Path pathAnnotation = aClass.getAnnotation(Path.class);

                if (pathAnnotation != null) {
                    return stackTraceElement;
                }
            } catch (ClassNotFoundException e) {
                // skip
            }
        }

        return null;
    }

    private Logger getEffectiveLogger(String resourceClassName) {
        Logger log = LOG;
        if (resourceClassName != null) {
            log = LoggerFactory.getLogger(resourceClassName);
        }
        return log;
    }

}

