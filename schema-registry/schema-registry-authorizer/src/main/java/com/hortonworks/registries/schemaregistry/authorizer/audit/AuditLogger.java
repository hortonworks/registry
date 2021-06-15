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
package com.hortonworks.registries.schemaregistry.authorizer.audit;

import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;

/** Create an audit log for every invokcation of SchemaRegistry. */
public interface AuditLogger {

    /** Get an instance of ISchemaRegistry which will not perform any action other than
     * logging the invoked method and params. */
    ISchemaRegistry withAuth(Authorizer.UserAndGroups auth);

}
