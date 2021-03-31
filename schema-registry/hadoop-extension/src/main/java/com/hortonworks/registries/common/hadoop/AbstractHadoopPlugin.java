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
package com.hortonworks.registries.common.hadoop;

import com.hortonworks.registries.common.FileStorageConfiguration;
import com.hortonworks.registries.common.ServiceAuthenticationConfiguration;
import com.hortonworks.registries.common.util.HadoopPlugin;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

/** Abstract adapter, allowing descending classes to implement only what's needed. */
abstract class AbstractHadoopPlugin implements HadoopPlugin {

    @Override
    public void initialize(FileStorageConfiguration config) throws IOException {
        throw new AbstractMethodError("Not implemented");
    }

    @Override
    public String uploadInternal(InputStream inputStream, String name) throws IOException {
        throw new AbstractMethodError("Not implemented");
    }

    @Override
    public InputStream downloadInternal(String name) throws IOException {
        throw new AbstractMethodError("Not implemented");
    }

    @Override
    public boolean deleteInternal(String name) throws IOException {
        throw new AbstractMethodError("Not implemented");
    }

    @Override
    public boolean existsInternal(String name) throws IOException {
        throw new AbstractMethodError("Not implemented");
    }

    @Override
    public void loadKerberosUser(ServiceAuthenticationConfiguration authConfig) {
        throw new AbstractMethodError("Not implemented");
    }

    @Override
    public Set<String> getGroupsForUser(String kerberosShortName) {
        throw new AbstractMethodError("Not implemented");
    }
}
