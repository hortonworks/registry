/**
 * Copyright 2016-2023 Cloudera, Inc.
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
package com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.compatibility;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.util.function.Function;

/**
 * Class to represent a certain step.
 */
@Getter
@Builder
@ToString
public final class TestStep {
    @NonNull
    private final Function functionToBeExecuted;
    @NonNull
    private final Object testObject;
    @NonNull
    private final CompatibilityTestCase.TestAction testAction;
    @Builder.Default
    private Class<? extends Exception> expectedException = null;
    @Builder.Default
    private String errorMessage = null;
    @Builder.Default
    private String bugID = null;

//    /**
//     * Builder method to add mandatory arguments.
//     * @param functionToBeExecuted Function to be executed for the test step
//     * @param testObject Test object for the test step
//     * @param testAction Test action representing the function being executed
//     * @return Builder object to build test step
//     */
//    static TestStepBuilder builder(Function functionToBeExecuted,
//                                   Object testObject,
//                                   CompatibilityTestCase.TestAction testAction) {
//        return hiddenBuilder()
//                .functionToBeExecuted(functionToBeExecuted)
//                .testObject(testObject)
//                .testAction(testAction);
//    }

//    /**
//     * Clone method to deep copy.
//     * @return A deep copy version of the test step
//     */
//    public TestStep clone() {
//        return TestStep.builder(functionToBeExecuted, testObject, testAction).build();
//    }
}