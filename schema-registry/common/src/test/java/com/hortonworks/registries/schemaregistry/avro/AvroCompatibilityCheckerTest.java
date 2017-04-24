/*
 * Copyright 2016 Hortonworks.
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
 */
package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.schemaregistry.CompatibilityResult;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 *
 */
public class AvroCompatibilityCheckerTest {


    @Test
    public void testNoneCompatibility() throws Exception {
        CompatibilityResult compatibilityResult =
                new AvroSchemaProvider().checkCompatibility(fetchResourceText("/avro/user.avsc"),
                                                            initialVersionSchema(),
                                                            SchemaCompatibility.NONE);

        Assert.assertTrue(compatibilityResult.isCompatible());
    }

    private String fetchResourceText(String resourceName) throws IOException {
        return IOUtils.toString(AvroCompatibilityCheckerTest.class.getResourceAsStream(resourceName), "UTF-8");
    }

    private String initialVersionSchema() throws IOException {
        return fetchResourceText("/avro/book-initial.avsc");
    }

    @Test
    public void testBackwardCompatibility() throws Exception {
        CompatibilityResult compatibilityResult =
                new AvroSchemaProvider().checkCompatibility(fetchResourceText("/avro/book-backward.avsc"),
                                                            initialVersionSchema(),
                                                            SchemaCompatibility.BACKWARD);

        Assert.assertTrue(compatibilityResult.isCompatible());
    }

    @Test
    public void testForwardCompatibility() throws Exception {
        CompatibilityResult compatibilityResult =
                new AvroSchemaProvider().checkCompatibility(fetchResourceText("/avro/book-forward.avsc"),
                                                            initialVersionSchema(),
                                                            SchemaCompatibility.FORWARD);

        Assert.assertTrue(compatibilityResult.isCompatible());
    }

    @Test
    public void testFullCompatibility() throws Exception {
        CompatibilityResult compatibilityResult =
                new AvroSchemaProvider().checkCompatibility(fetchResourceText("/avro/book-both.avsc"),
                                                            initialVersionSchema(),
                                                            SchemaCompatibility.BOTH);

        Assert.assertTrue(compatibilityResult.isCompatible());
    }

    @Test
    public void testInvalidCompatibilities() throws Exception {

        SchemaCompatibility[] compatibilities = {SchemaCompatibility.BACKWARD, SchemaCompatibility.FORWARD, SchemaCompatibility.BOTH};

        for (SchemaCompatibility compatibility : compatibilities) {
            CompatibilityResult compatibilityResult =
                    new AvroSchemaProvider().checkCompatibility(fetchResourceText("/avro/book-invalid-compat.avsc"),
                                                                initialVersionSchema(),
                                                                compatibility);

            Assert.assertFalse(compatibilityResult.isCompatible());
        }
    }

}
