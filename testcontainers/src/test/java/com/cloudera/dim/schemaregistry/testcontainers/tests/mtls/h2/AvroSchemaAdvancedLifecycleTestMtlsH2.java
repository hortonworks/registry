/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package com.cloudera.dim.schemaregistry.testcontainers.tests.mtls.h2;

import com.cloudera.dim.schemaregistry.testcontainers.env.testsetup.TestSetup;
import com.cloudera.dim.schemaregistry.testcontainers.tests.AvroSchemaAdvancedLifecycleTest;
import org.junit.jupiter.api.TestInstance;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AvroSchemaAdvancedLifecycleTestMtlsH2 extends AvroSchemaAdvancedLifecycleTest {

    @Override
    protected TestSetup getTestSetup(String tempFolderPath) {
        return MTlsH2.getTestSetup(tempFolderPath);
    }
}
