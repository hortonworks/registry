/**
 * Copyright 2017-2019 Cloudera, Inc.
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

package com.hortonworks.registries.storage.transaction;

import com.hortonworks.registries.storage.TransactionManager;
import org.glassfish.jersey.server.model.ResourceMethod;
import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TransactionEventListener implements ApplicationEventListener {

    private final ConcurrentMap<ResourceMethod, Optional<UnitOfWork>> methodMap = new ConcurrentHashMap<>();
    private final TransactionManager transactionManager;
    private final boolean runWithTxnIfNotConfigured;
    private TransactionIsolation defaultTransactionIsolation;

    /**
     * Creates instance by taking the below arguments and webservice methods are not run in transaction unless they use
     * {@link UnitOfWork} on respective webservice resource methods.
     *
     * @param transactionManager transactionManager to be used for txn lifecycle invocations.
     */
    public TransactionEventListener(TransactionManager transactionManager) {
        this(transactionManager, false);
    }

    public TransactionEventListener(TransactionManager transactionManager,
                                    TransactionIsolation defaultTransactionIsolation) {
        this(transactionManager, false);
        this.defaultTransactionIsolation = defaultTransactionIsolation;
    }

    /**
     * Creates instance by taking the below arguments.
     *
     * @param transactionManager        transactionManager to be used for txn lifecycle invocations.
     * @param runWithTxnIfNotConfigured All webservice resource methods are invoked in a transaction even if they are
     *                                  not set with {@link UnitOfWork}.
     */
    public TransactionEventListener(TransactionManager transactionManager,
                                    boolean runWithTxnIfNotConfigured) {
        this.transactionManager = transactionManager;
        this.runWithTxnIfNotConfigured = runWithTxnIfNotConfigured;
    }

    public TransactionEventListener(TransactionManager transactionManager,
                                    boolean runWithTxnIfNotConfigured,
                                    TransactionIsolation defaultTransactionIsolation) {
        this.transactionManager = transactionManager;
        this.runWithTxnIfNotConfigured = runWithTxnIfNotConfigured;
        this.defaultTransactionIsolation = defaultTransactionIsolation;
    }

    private static class UnitOfWorkEventListener implements RequestEventListener {
        private final ConcurrentMap<ResourceMethod, Optional<UnitOfWork>> methodMap;
        private final TransactionManager transactionManager;
        private final boolean runWithTxnIfNotConfigured;
        private final TransactionIsolation defaultTransactionIsolation;
        private boolean useTransactionForUnitOfWork = true;
        private boolean isTransactionActive = false;

        public UnitOfWorkEventListener(ConcurrentMap<ResourceMethod, Optional<UnitOfWork>> methodMap,
                                       TransactionManager transactionManager,
                                       boolean runWithTxnIfNotConfigured,
                                       TransactionIsolation defaultTransactionIsolation) {
            this.methodMap = methodMap;
            this.transactionManager = transactionManager;
            this.runWithTxnIfNotConfigured = runWithTxnIfNotConfigured;
            this.defaultTransactionIsolation = defaultTransactionIsolation;
        }

        @Override
        public void onEvent(RequestEvent event) {
            final RequestEvent.Type eventType = event.getType();

            if (eventType == RequestEvent.Type.RESOURCE_METHOD_START) {

                // Start transaction before invoking the resource method for the request

                Optional<UnitOfWork> unitOfWork = methodMap.computeIfAbsent(event.getUriInfo()
                                                                                 .getMatchedResourceMethod(),
                                                                            UnitOfWorkEventListener::registerUnitOfWorkAnnotations);

                // get property whether to have unitOfWork with DB default transaction by default
                useTransactionForUnitOfWork =
                        unitOfWork.map(UnitOfWork::transactional).orElse(runWithTxnIfNotConfigured);

                TransactionIsolation transactionIsolation =
                        unitOfWork.map(x -> {
                                    TransactionIsolation result = x.transactionIsolation();
                                    if (result == TransactionIsolation.APPLICATION_DEFAULT) {
                                        result = defaultTransactionIsolation == null ? TransactionIsolation.DATABASE_SENSITIVE : defaultTransactionIsolation;
                                    }
                                    return result;
                                }
                        ).orElse(TransactionIsolation.DATABASE_SENSITIVE);

                if (useTransactionForUnitOfWork) {
                    transactionManager.beginTransaction(transactionIsolation);
                    isTransactionActive = true;
                }
            } else if (eventType == RequestEvent.Type.RESP_FILTERS_START) {

                // Once the response from the resource method is available we should either rollback or commit the
                // transaction. In case the resource method throws an error, ON_EXCEPTION is called first which will
                // rollback the transaction and eventually RESP_FILTERS_START is called (which won't do anything)
                // after exception mapper handles the exception. In case the exception is not thrown, RESP_FILTERS_START
                // is called which then commits the transaction.
                //      Its not advisable to handle rollbacks and commits when the request transitions to FINISHED state.
                // For example, we might have a client which add an entity (E1) and lets say entity id is returned as a response.
                // Lets say the client then adds a second entity (E2) which has foreign key reference to the first entity (E1).
                // If we commit the transaction in FINISHED state, then the client received the entity id after adding E1
                // at time say t1 and adding entity E2 at t2, then we might run into a scenario where we might commit the changes
                // to E1 only after t2 as responses to the client are dispatched before FINISHED, then client will receive
                // an error that E1 is not found even thought the operation of adding E1 had succeeded from the client's point of view.

                if (useTransactionForUnitOfWork && isTransactionActive) {
                    if (event.getContainerResponse().getStatus() < 400) {
                        transactionManager.commitTransaction();
                    } else {
                        transactionManager.rollbackTransaction();
                    }

                    isTransactionActive = false;
                }
            } else if (eventType == RequestEvent.Type.ON_EXCEPTION) {

                // Rollback the transaction in case an exception is thrown from the resource method.

                if (useTransactionForUnitOfWork && isTransactionActive) {
                    transactionManager.rollbackTransaction();
                    isTransactionActive = false;
                }
            }
        }

        private static Optional<UnitOfWork> registerUnitOfWorkAnnotations(ResourceMethod method) {
            UnitOfWork annotation = method.getInvocable().getDefinitionMethod().getAnnotation(UnitOfWork.class);
            if (annotation == null) {
                annotation = method.getInvocable().getHandlingMethod().getAnnotation(UnitOfWork.class);
            }
            return Optional.ofNullable(annotation);
        }
    }

    @Override
    public void onEvent(ApplicationEvent applicationEvent) {
    }

    @Override
    public RequestEventListener onRequest(RequestEvent requestEvent) {
        return new UnitOfWorkEventListener(methodMap, transactionManager, runWithTxnIfNotConfigured, defaultTransactionIsolation);
    }
}