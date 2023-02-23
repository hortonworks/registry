/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package com.cloudera.dim.schemaregistry.testcontainers.tests.tlskerb.postgres;

import com.cloudera.dim.schemaregistry.testcontainers.env.testsetup.TestSetup;
import com.cloudera.dim.schemaregistry.testcontainers.tests.AvroSchemaMetadataTest;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AvroSchemaMetadataTestTlskerbPostgres extends AvroSchemaMetadataTest {

    @Override
    protected TestSetup getTestSetup(String tempFolderPath) {
        return TlsKerbPostgres.getTestSetup(tempFolderPath);
    }
}
