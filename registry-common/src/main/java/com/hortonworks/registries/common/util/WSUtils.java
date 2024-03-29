/**
 * Copyright 2016-2019 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.common.util;

import com.google.common.io.ByteStreams;
import com.hortonworks.registries.common.CollectionResponse;
import com.hortonworks.registries.common.QueryParam;
import com.hortonworks.registries.common.catalog.CatalogResponse;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Utility methods for the webservice.
 */
public final class WSUtils {
    private WSUtils() {
    }

    public static Response respondEntities(Collection<?> entities, Response.Status status) {
        return Response.status(status)
                .entity(CollectionResponse.newResponse().entities(entities).build())
                .build();
    }

    public static Response respondEntity(Object entity, Response.Status status) {
        return Response.status(status)
                .entity(entity)
                .build();
    }

    public static Response respond(Response.Status status) {
        return Response.status(status).build();
    }

    public static Response respond(Response.Status status, CatalogResponse.ResponseMessage msg, String... formatArgs) {
        return Response.status(status)
                .entity(CatalogResponse.newResponse(msg).format(formatArgs))
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }

    public static Response confluentRespond(Response.Status status, CatalogResponse.ResponseMessage msg, String... formatArgs) {
        return Response.status(status)
                .entity(CatalogResponse.newResponse(msg).format(formatArgs))
                .build();
    }

    public static Response respondString(Response.Status status, CatalogResponse.ResponseMessage msg, String... formatArgs) {
        return Response.status(status)
                .entity(CatalogResponse.newResponse(msg).format(formatArgs).getResponseMessage())
                .build();
    }

    public static StreamingOutput wrapWithStreamingOutput(final InputStream inputStream) {
        return new StreamingOutput() {
            public void write(OutputStream os) throws IOException, WebApplicationException {
                try {
                    OutputStream wrappedOutputStream = os;
                    if (!(os instanceof BufferedOutputStream)) {
                        wrappedOutputStream = new BufferedOutputStream(os);
                    }

                    ByteStreams.copy(inputStream, wrappedOutputStream);

                    wrappedOutputStream.flush();
                } finally {
                    inputStream.close();
                }
            }
        };
    }

    public static List<QueryParam> buildQueryParameters(MultivaluedMap<String, String> params) {
        if (params == null || params.isEmpty()) {
            return Collections.<QueryParam>emptyList();
        }

        List<QueryParam> queryParams = new ArrayList<>();
        for (String param : params.keySet()) {
            queryParams.add(new QueryParam(param, params.getFirst(param)));
        }
        return queryParams;
    }


}
