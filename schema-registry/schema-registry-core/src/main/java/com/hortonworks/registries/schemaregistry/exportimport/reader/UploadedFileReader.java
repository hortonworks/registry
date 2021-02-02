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
package com.hortonworks.registries.schemaregistry.exportimport.reader;

import com.hortonworks.registries.schemaregistry.exportimport.RawSchema;

import java.io.IOException;

/** Read schemas from an uploaded file. */
public interface UploadedFileReader {

    /**
     * Read the next schema in the input file.
     *
     * @return  null if there are no more shemas remaining
     */
    RawSchema readSchema() throws IOException;

}

