/**
 * Copyright 2017 Hortonworks.
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

package com.hortonworks.registries.schemaregistry.errors;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class InvalidSchemaBranchDeletionException extends Exception {

    public InvalidSchemaBranchDeletionException(String message) {
        super(message);
    }

    public InvalidSchemaBranchDeletionException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public static InvalidSchemaBranchDeletionException getSchemaVersionTiedToOtherBranchException(Map<Integer, List<String>> versionToBranchMap) {
        StringBuffer message = new StringBuffer();
        message.append("Failed to delete branch");
        versionToBranchMap.entrySet().stream().forEach(versionWithBranch -> {
            message.append(", schema version : '").append(versionWithBranch.getKey()).append("'");
            message.append(" is tied to branch : '").append(Arrays.toString(versionWithBranch.getValue().toArray())).append("'");
        });
        return new InvalidSchemaBranchDeletionException(message.toString());
    }
}
