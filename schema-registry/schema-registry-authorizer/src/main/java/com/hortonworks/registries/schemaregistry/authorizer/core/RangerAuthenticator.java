/**
 * Copyright 2016-2022 Cloudera, Inc.
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
package com.hortonworks.registries.schemaregistry.authorizer.core;

import javax.annotation.Nullable;
import javax.ws.rs.core.SecurityContext;

/** Schema Registry receives its principal through SPNEGO, OAuth2 or other mechanisms.
 * We need to pass the principal to Ranger. */
public interface RangerAuthenticator {

 /** Get the user which will be sent to Ranger for authentication. */
 @Nullable
 Authorizer.UserAndGroups getUserAndGroups(SecurityContext sc);

}
