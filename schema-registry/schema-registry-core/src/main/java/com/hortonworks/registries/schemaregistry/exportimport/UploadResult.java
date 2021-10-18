/**
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
 **/
package com.hortonworks.registries.schemaregistry.exportimport;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * After uploading a file containing schemas exported from another database, we will
 * respond to the user with information about how many schemas were imported successfully
 * and how many failed.
 */
public class UploadResult {

    private int successCount;
    private int failedCount;
    private List<Long> failedIds;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UploadResult that = (UploadResult) o;
        Collections.sort(failedIds);
        Collections.sort(that.failedIds);
        return successCount == that.successCount && failedCount == that.failedCount && Objects.equals(failedIds, that.failedIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(successCount, failedCount, failedIds);
    }

    public UploadResult() { }

    public UploadResult(int successCount, int failedCount, List<Long> failedIds) {
        this.successCount = successCount;
        this.failedCount = failedCount;
        this.failedIds = removeDuplicates(failedIds);
    }

    private static List<Long> removeDuplicates(List<Long> failedIds) {
        if (failedIds == null || failedIds.size() <= 1) {
            return failedIds;
        }
        // remove duplicates and nulls
        return failedIds.stream().filter(Objects::nonNull).distinct().collect(Collectors.toList());
    }

    public int getSuccessCount() {
        return successCount;
    }

    public void setSuccessCount(int successCount) {
        this.successCount = successCount;
    }

    public int getFailedCount() {
        return failedCount;
    }

    public void setFailedCount(int failedCount) {
        this.failedCount = failedCount;
    }

    public List<Long> getFailedIds() {
        return failedIds;
    }

    public void setFailedIds(List<Long> failedIds) {
        this.failedIds = failedIds;
    }

}
