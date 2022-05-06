/**
 * Copyright 2016-2022 Cloudera, Inc.
 * <p>
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
 **/
package com.hortonworks.registries.common.util;

import com.hortonworks.registries.common.CollectionResponse;
import com.hortonworks.registries.common.catalog.CatalogResponse;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WSUtilsTest {

    @Test
    public void testWrapWithStreamingOutput() throws IOException {
        InputStream inputStream = mock(InputStream.class);

        // we simulate an input stream with some data
        final byte[] smallData = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        final AtomicInteger dataCounter = new AtomicInteger();
        when(inputStream.read(any(byte[].class))).thenAnswer(new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) {
                byte[] buffer = invocation.getArgument(0);
                if (dataCounter.get() >= smallData.length) {
                    return -1;
                } else {
                    dataCounter.set(smallData.length);
                    System.arraycopy(smallData, 0, buffer, 0, smallData.length);
                    return smallData.length;
                }
            }
        });

        StreamingOutput streamingOutput = WSUtils.wrapWithStreamingOutput(inputStream);
        assertNotNull(streamingOutput);
        streamingOutput.write(new ByteArrayOutputStream());

        verify(inputStream, atLeastOnce()).read(any(byte[].class));
        verify(inputStream).close();
    }

    @Test
    public void testRespondEntities() {
        final Response.Status status = Response.Status.SEE_OTHER;
        final Collection<Pojo> entities = new ArrayList<>();
        entities.add(new Pojo("test"));
        entities.add(new Pojo("test2"));

        Response response = WSUtils.respondEntities(entities, status);
        assertNotNull(response);
        assertEquals(status, response.getStatusInfo().toEnum());
        assertTrue(response.getEntity() instanceof CollectionResponse);

        CollectionResponse sameCollection = (CollectionResponse) response.getEntity();
        assertEquals(entities.size(), sameCollection.getEntities().size());
        assertIterableEquals(entities, sameCollection.getEntities());
    }

    @Test
    public void testRespondEntity() {
        final Response.Status status = Response.Status.CREATED;

        Response response = WSUtils.respondEntity(new Pojo("test"), status);
        assertNotNull(response);
        assertEquals(status, response.getStatusInfo().toEnum());
        assertTrue(response.getEntity() instanceof Pojo);
    }

    @Test
    public void testRespond() {
        final Response.Status status = Response.Status.NO_CONTENT;
        Response response = WSUtils.respond(status);
        assertNotNull(response);
        assertEquals(status, response.getStatusInfo().toEnum());
        assertNull(response.getEntity());
    }

    @Test
    public void testCatalogRespond() {
        final Response.Status status = Response.Status.FOUND;
        Response response = WSUtils.respond(status, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, "test");
        assertNotNull(response);
        assertEquals(status, response.getStatusInfo().toEnum());
        assertNotNull(response.getEntity());
        assertTrue(response.getEntity() instanceof CatalogResponse);
    }
}

class Pojo {
    private String value;

    public Pojo() { }

    public Pojo(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}