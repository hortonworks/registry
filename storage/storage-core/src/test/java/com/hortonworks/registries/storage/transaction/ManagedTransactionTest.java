/*
 * Copyright 2016-2021 Cloudera, Inc.
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
 */
package com.hortonworks.registries.storage.transaction;

import com.hortonworks.registries.storage.TransactionManager;
import com.hortonworks.registries.storage.exception.IgnoreTransactionRollbackException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.print.PrintException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class ManagedTransactionTest {

    private TransactionManager mockedTransactionManager;
    private TransactionIsolation transactionIsolation;
    private ManagedTransaction managedTransaction;

    private final String testParam1 = "testParam1";
    private final String testParam2 = "testParam2";
    private final String testParam3 = "testParam3";
    private final String testParam4 = "testParam4";
    private final String testParam5 = "testParam5";

    private Object[] calledArgs;

    @BeforeEach
    public void setUp() throws Exception {
        this.calledArgs = null;
        mockedTransactionManager = mock(TransactionManager.class);
        transactionIsolation = TransactionIsolation.APPLICATION_DEFAULT;
        managedTransaction = new ManagedTransaction(mockedTransactionManager, transactionIsolation);

    }

    @Test
    public void testExecuteFunctionArg0SuccessCase() throws Exception {
        Integer result = managedTransaction.executeFunction(this::callHelperWithReturn);
        assertEquals(Integer.valueOf(1), result);
        verifyInteractionWithTransactionManagerSuccessCase();
    }

    @Test
    public void testExecuteFunctionArg1SuccessCase() throws Exception {
        Integer result = managedTransaction.executeFunction(this::callHelperWithReturn, testParam1);
        assertEquals(Integer.valueOf(1), result);
        verifyInteractionWithTransactionManagerSuccessCase(testParam1);
    }

    @Test
    public void testExecuteFunctionArg2SuccessCase() throws Exception {
        Integer result = managedTransaction.executeFunction(this::callHelperWithReturn, testParam1, testParam2);
        assertEquals(Integer.valueOf(1), result);
        verifyInteractionWithTransactionManagerSuccessCase(testParam1, testParam2);
    }

    @Test
    public void testExecuteFunctionArg3SuccessCase() throws Exception {
        Integer result = managedTransaction.executeFunction(this::callHelperWithReturn, testParam1, testParam2, testParam3);
        assertEquals(Integer.valueOf(1), result);
        verifyInteractionWithTransactionManagerSuccessCase(testParam1, testParam2, testParam3);
    }

    @Test
    public void testExecuteFunctionArg4SuccessCase() throws Exception {
        Integer result = managedTransaction.executeFunction(this::callHelperWithReturn, testParam1, testParam2, testParam3, testParam4);
        assertEquals(Integer.valueOf(1), result);
        verifyInteractionWithTransactionManagerSuccessCase(testParam1, testParam2, testParam3, testParam4);
    }

    @Test
    public void testExecuteFunctionArg5SuccessCase() throws Exception {
        Integer result = managedTransaction.executeFunction(this::callHelperWithReturn, testParam1, testParam2, testParam3, testParam4, testParam5);
        assertEquals(Integer.valueOf(1), result);
        verifyInteractionWithTransactionManagerSuccessCase(testParam1, testParam2, testParam3, testParam4, testParam5);
    }

    @Test
    public void testExecuteFunctionRollbackCase() {
        try {
            managedTransaction.executeFunction(this::callHelperWithThrowException);
            fail("It should propagate Exception!");
        } catch (Exception e) {
            verifyInteractionWithTransactionManagerWithExceptionCase();
        }
    }

    @Test
    public void testExecuteFunctionIgnoreRollbackCase() {
        try {
            managedTransaction.executeFunction(this::callHelperWithThrowIgnoreRollbackException);
            fail("It should propagate Exception!");
        } catch (Exception e) {
            // it should propagate cause, not exception itself
            assertFalse(e instanceof IgnoreTransactionRollbackException);
            assertTrue(e instanceof PrintException);
            verifyInteractionWithTransactionManagerWithIgnoreRollbackCase();
        }
    }

    @Test
    public void testExecuteConsumerArg0SuccessCase() throws Exception {
        managedTransaction.executeConsumer(this::callHelperWithReturn);
        verifyInteractionWithTransactionManagerSuccessCase();
    }

    @Test
    public void testExecuteConsumerArg1SuccessCase() throws Exception {
        managedTransaction.executeConsumer(this::callHelperWithReturn, testParam1);
        verifyInteractionWithTransactionManagerSuccessCase(testParam1);
    }

    @Test
    public void testExecuteConsumerArg2SuccessCase() throws Exception {
        managedTransaction.executeConsumer(this::callHelperWithReturn, testParam1, testParam2);
        verifyInteractionWithTransactionManagerSuccessCase(testParam1, testParam2);
    }

    @Test
    public void testExecuteConsumerArg3SuccessCase() throws Exception {
        managedTransaction.executeConsumer(this::callHelperWithReturn, testParam1, testParam2, testParam3);
        verifyInteractionWithTransactionManagerSuccessCase(testParam1, testParam2, testParam3);
    }

    @Test
    public void testExecuteConsumerArg4SuccessCase() throws Exception {
        managedTransaction.executeConsumer(this::callHelperWithReturn, testParam1, testParam2, testParam3, testParam4);
        verifyInteractionWithTransactionManagerSuccessCase(testParam1, testParam2, testParam3, testParam4);
    }

    @Test
    public void testExecuteConsumerArg5SuccessCase() throws Exception {
        managedTransaction.executeConsumer(this::callHelperWithReturn, testParam1, testParam2, testParam3, testParam4,
                testParam5);
        verifyInteractionWithTransactionManagerSuccessCase(testParam1, testParam2, testParam3, testParam4, testParam5);
    }

    @Test
    public void testExecuteConsumerRollbackCase() {
        try {
            managedTransaction.executeConsumer(this::callHelperWithThrowException);
            fail("It should propagate Exception");
        } catch (Exception e) {
            assertTrue(e instanceof PrintException);
            verifyInteractionWithTransactionManagerWithExceptionCase();
        }
    }

    @Test
    public void testExecuteConsumerIgnoreRollbackCase() {
        try {
            managedTransaction.executeConsumer(this::callHelperWithThrowIgnoreRollbackException);
            fail("It should propagate Exception");
        } catch (Exception e) {
            // it should propagate cause, not exception itself
            assertFalse(e instanceof IgnoreTransactionRollbackException);
            assertTrue(e instanceof PrintException);
            verifyInteractionWithTransactionManagerWithIgnoreRollbackCase();
        }
    }

    @Test
    public void testCaseCommitTransactionThrowsException() {
        doAnswer(invocation -> {
            throw new PrintException();
        }).when(mockedTransactionManager).commitTransaction();
        assertThrows(Exception.class, () -> managedTransaction.executeConsumer(this::callHelperWithReturn));
        verify(mockedTransactionManager).beginTransaction(transactionIsolation);
        verify(mockedTransactionManager).commitTransaction();
        verify(mockedTransactionManager).rollbackTransaction();
    }

    @Test
    public void testCaseCommitTransactionThrowsExceptionAfterCaseIgnoreRollbackException() {
        doAnswer(invocation -> {
            throw new PrintException();
        }).when(mockedTransactionManager).commitTransaction();
        assertThrows(Exception.class, () -> managedTransaction.executeConsumer(this::callHelperWithThrowIgnoreRollbackException));
        verify(mockedTransactionManager).beginTransaction(transactionIsolation);
        verify(mockedTransactionManager).commitTransaction();
        verify(mockedTransactionManager).rollbackTransaction();
    }

    @Test
    public void testCaseRollbackTransactionThrowsException() {
        assertThrows(Exception.class, () -> managedTransaction.executeConsumer(this::callHelperWithThrowException));
        verify(mockedTransactionManager).beginTransaction(transactionIsolation);
        verify(mockedTransactionManager, never()).commitTransaction();
        verify(mockedTransactionManager).rollbackTransaction();
    }

    private Integer callHelperWithReturn(Object...args) {
        this.calledArgs = args;
        return 1;
    }

    private Integer callHelperWithThrowException(Object...args) throws Exception {
        this.calledArgs = args;
        throw new PrintException();
    }

    private Integer callHelperWithThrowIgnoreRollbackException(Object...args) throws Exception {
        this.calledArgs = args;
        throw new IgnoreTransactionRollbackException(new PrintException());
    }

    private void verifyInteractionWithTransactionManagerSuccessCase(Object...args) {
        assertArrayEquals(args, this.calledArgs);

        verify(mockedTransactionManager).beginTransaction(transactionIsolation);
        verify(mockedTransactionManager).commitTransaction();
        verify(mockedTransactionManager, never()).rollbackTransaction();
    }

    private void verifyInteractionWithTransactionManagerWithExceptionCase(Object...args) {
        assertArrayEquals(args, this.calledArgs);

        verify(mockedTransactionManager).beginTransaction(transactionIsolation);
        verify(mockedTransactionManager, never()).commitTransaction();
        verify(mockedTransactionManager).rollbackTransaction();
    }

    private void verifyInteractionWithTransactionManagerWithIgnoreRollbackCase(Object...args) {
        assertArrayEquals(args, this.calledArgs);

        verify(mockedTransactionManager).beginTransaction(transactionIsolation);
        verify(mockedTransactionManager).commitTransaction();
        verify(mockedTransactionManager, never()).rollbackTransaction();
    }
}