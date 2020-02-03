/*
 * Copyright 2016-2020 Cloudera, Inc.
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
package com.hortonworks.registries.schemaregistry.authorizer;

import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
import com.hortonworks.registries.schemaregistry.authorizer.ranger.shim.RangerSchemaRegistryAuthorizer;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * This is a factory class that is used to create instance of  {@link Authorizer}.
 * The default authorizer is {@link RangerSchemaRegistryAuthorizer}. User defined authorizers
 * are supported.
 */
public class AuthorizerFactory {
    public static Authorizer getAuthorizer(Map<String, Object> props) {

        Authorizer authorizer;
        // If authorizer is not specified in config then RangerSchemaRegistryAuthorizer
        // is used by default
        if(props == null
                || !props.containsKey("authorizerClassName")
                || props.get("authorizerClassName")
                .equals(RangerSchemaRegistryAuthorizer.class.getCanonicalName())){

            authorizer = new RangerSchemaRegistryAuthorizer();
        } else {
            try {
                Class<Authorizer> cl = (Class<Authorizer>) Class.forName((String) props.get("authorizerClassName"));
                Constructor<Authorizer> constr = cl.getConstructor();
                authorizer = constr.newInstance();
            } catch (ClassNotFoundException | InstantiationException
                    | IllegalAccessException | NoSuchMethodException
                    | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
        authorizer.configure(props);

        return authorizer;
    }
}