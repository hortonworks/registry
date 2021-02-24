/**
 * Copyright 2016-2020 Cloudera, Inc.
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
package com.hortonworks.registries.schemaregistry.validator;

import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaProviderInfo;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SchemaMetadataTypeValidatorTest {
    private ISchemaRegistry mockSchemaRegistryServiceMock = mock(ISchemaRegistry.class);


    @Test
    public void canAcceptValidType() {
        //given
        SchemaMetadataTypeValidator underTest = new SchemaMetadataTypeValidator(mockSchemaRegistryServiceMock);
        SchemaProviderInfo providerInfo1 = new SchemaProviderInfo("json", null, null, null, null);
        SchemaProviderInfo providerInfo2 = new SchemaProviderInfo("avro", null, null, null, null);
        when(mockSchemaRegistryServiceMock.getSupportedSchemaProviders()).thenReturn(Arrays.asList(providerInfo1, providerInfo2));

        //when
        boolean actual = underTest.isValid("avro");

        //then
        verify(mockSchemaRegistryServiceMock).getSupportedSchemaProviders();
        assertThat(actual, is(true));
    }

    @Test
    public void canAcceptInvalidType() {
        //given
        SchemaMetadataTypeValidator underTest = new SchemaMetadataTypeValidator(mockSchemaRegistryServiceMock);
        SchemaProviderInfo providerInfo1 = new SchemaProviderInfo("json", null, null, null, null);
        SchemaProviderInfo providerInfo2 = new SchemaProviderInfo("avro", null, null, null, null);
        when(mockSchemaRegistryServiceMock.getSupportedSchemaProviders()).thenReturn(Arrays.asList(providerInfo1, providerInfo2));

        //when
        boolean actual = underTest.isValid("cat");

        //then
        verify(mockSchemaRegistryServiceMock).getSupportedSchemaProviders();
        assertThat(actual, is(false));
    }
}