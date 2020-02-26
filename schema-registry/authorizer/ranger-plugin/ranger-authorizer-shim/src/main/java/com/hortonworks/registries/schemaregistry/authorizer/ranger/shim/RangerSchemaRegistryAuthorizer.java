/*
 * Copyright 2016-2019 Cloudera, Inc.
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
package com.hortonworks.registries.schemaregistry.authorizer.ranger.shim;

import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
import java.lang.reflect.Constructor;
import java.util.Map;

public class RangerSchemaRegistryAuthorizer implements Authorizer {

    private static final Logger LOG  = LoggerFactory.getLogger(RangerSchemaRegistryAuthorizer.class);

    private static final String   RANGER_PLUGIN_TYPE = "schema-registry";
    private static final String   RANGER_SR_AUTHORIZER_IMPL_CLASSNAME  =
            "com.hortonworks.registries.schemaregistry.authorizer.ranger.RangerSchemaRegistryAuthorizerImpl";

    private Authorizer  rangerSRAuthorizerImpl;
    private static RangerPluginClassLoader rangerPluginClassLoader;

    public void configure(Map<String, Object> props) { }

    public RangerSchemaRegistryAuthorizer() {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerSchemaRegistryAuthorizer.RangerSchemaRegistryAuthorizer()");
        }

        this.init();

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerSchemaRegistryAuthorizer.RangerSchemaRegistryAuthorizer()");
        }
    }

    @Override
    public boolean authorize(Resource resource,
                             AccessType accessType,
                             UserAndGroups userAndGroups) {
        if(LOG.isDebugEnabled()) {
            LOG.debug(String.format(
                    "==> RangerSchemaRegistryAuthorizer.authorize(resource='%s' accessType='%s', uName='%s', uGroup='%s')",
                    resource,
                    accessType,
                    userAndGroups.getUser(),
                    userAndGroups.getGroups()));
        }

        boolean ret;

        try {
            activatePluginClassLoader();
            ret = rangerSRAuthorizerImpl.authorize(resource, accessType, userAndGroups);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerSchemaRegistryAuthorizer.authorize: " + ret);
        }

        return ret;
    }

    private void init(){
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerSchemaRegistryAuthorizer.init()");
        }

        try {

            rangerPluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());

            @SuppressWarnings("unchecked")
            Class<Authorizer> cls = (Class<Authorizer>) Class.forName(RANGER_SR_AUTHORIZER_IMPL_CLASSNAME, true, rangerPluginClassLoader);
            Constructor<Authorizer> constr = cls.getConstructor();

            activatePluginClassLoader();

            rangerSRAuthorizerImpl = constr.newInstance();
        } catch (Exception e) {
            // check what need to be done
            LOG.error("Error Enabling RangerSchemaRegistryAuthorizer", e);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerSchemaRegistryAuthorizer.init()");
        }
    }

    private void activatePluginClassLoader() {
        if(rangerPluginClassLoader != null) {
            rangerPluginClassLoader.activate();
        }
    }

    private void deactivatePluginClassLoader() {
        if(rangerPluginClassLoader != null) {
            rangerPluginClassLoader.deactivate();
        }
    }
}
