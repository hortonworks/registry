/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.compatibility;

import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;

/**
 * Encapsulation to ensure test case name is not duplicated resulting in loss of test cases.
 */
public abstract class CompatibiltyPositiveNegativeTestCases {

    protected final ISchemaRegistryClient schemaRegistryClient;

    public CompatibiltyPositiveNegativeTestCases(ISchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
    }

    /**
     * Method to get all positive/negative test cases.
     *
     * @return all compatibility test cases.
     */
    public CompatibilityTestCases getAll() {
        CompatibilityTestCases testCases = new CompatibilityTestCases();
        testCases.append(getForwardCompatibilityTests());
        testCases.append(getBackwardCompatibilityTests());
        testCases.append(getNoneCompatibilityTests());
        testCases.append(getBothCompatibilityTests());
        testCases.append(getUpdateTestCasesForCompatibility());
        return testCases;
    }

    /**
     * Test cases for forward compatibility.
     *
     * @return test cases for compatibility type forward
     */
    protected abstract CompatibilityTestCases getForwardCompatibilityTests();

    /**
     * Test cases for backward compatibility.
     *
     * @return test cases for compatibility type Backward
     */
    protected abstract CompatibilityTestCases getBackwardCompatibilityTests();

    /**
     * Test cases for None compatibility.
     *
     * @return test cases for compatibility type None
     */
    protected abstract CompatibilityTestCases getNoneCompatibilityTests();

    /**
     * Test cases for Both compatibility.
     *
     * @return test cases for compatibility type None
     */
    protected abstract CompatibilityTestCases getBothCompatibilityTests();

    /**
     * Test cases for updating compatibility.
     *
     * @return test cases for update
     */
    protected abstract CompatibilityTestCases getUpdateTestCasesForCompatibility();

}
