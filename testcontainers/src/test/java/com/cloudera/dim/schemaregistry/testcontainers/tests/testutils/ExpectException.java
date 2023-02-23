/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package com.cloudera.dim.schemaregistry.testcontainers.tests.testutils;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Assert;

/**
 * Interface for expecting exception.
 */
@FunctionalInterface
public interface ExpectException {

    /**
     * Method to assert that an exception is thrown.
     *
     * @param expectedException    Expected exception type
     * @param expectedErrorMessage expected error message
     * @param thrownWhen           Action when exception is expected.
     */
    default void assertExceptionThrown(Class<? extends Exception> expectedException,
                                       String expectedErrorMessage,
                                       String thrownWhen) {
        assertExceptionThrown(expectedException, expectedErrorMessage, thrownWhen, null);
    }

    /**
     * Method to assert that an exception is thrown.
     *
     * @param expectedException    Expected exception type
     * @param expectedErrorMessage expected error message
     * @param thrownWhen           Action when exception is expected.
     * @param jiraBugIDRaised      Bug ID if raised
     */
    default void assertExceptionThrown(Class<? extends Exception> expectedException,
                                       String expectedErrorMessage,
                                       String thrownWhen,
                                       String jiraBugIDRaised) {
        StringBuilder raisedBugInfo = new StringBuilder();
        if (jiraBugIDRaised != null) {
            raisedBugInfo.append("Check bugID ");
            raisedBugInfo.append(jiraBugIDRaised);
            raisedBugInfo.append(" to see if it is fixed.");
        }
        try {
            doWork();
            Assert.fail("Expected exception of type " + expectedException + " when " + thrownWhen + "." + raisedBugInfo);
        } catch (Exception e) {
            Assert.assertEquals("Exception type mismatch." + " when " + thrownWhen +
                            "Got : " + ExceptionUtils.getStackTrace(e) + "\n" + raisedBugInfo,
                    e.getClass(), expectedException);
            Assert.assertTrue("Expected error message to be clearer. " +
                            "Expected to have :\"" +
                            expectedErrorMessage + "\"" +
                            "\nGot :\"" + e.getMessage() + "\"",
                    e.getMessage().contains(expectedErrorMessage));

        }
    }

    /**
     * Method that needs to be implemented.
     *
     * @throws Exception Exception thrown when doing work
     */
    void doWork() throws Exception;
}