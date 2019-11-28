package com.hortonworks.registries.ranger.services.schemaregistry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import com.hortonworks.registries.ranger.services.schemaregistry.client.SchemaRegistryConnectionMgr;
import com.hortonworks.registries.ranger.services.schemaregistry.client.SchemaRegistryResourceMgr;

import java.util.*;

public class RangerServiceSchemaRegistry extends RangerBaseService {

    private static final Log LOG = LogFactory.getLog(RangerServiceSchemaRegistry.class);


    @Override
    public void init(RangerServiceDef serviceDef, RangerService service) {
        super.init(serviceDef, service);
    }

    @Override
    public HashMap<String, Object> validateConfig() throws Exception {
        HashMap<String, Object> ret = new HashMap<String, Object>();

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceSchemaRegistry.validateConfig(" + serviceName + ")");
        }

        if (configs != null) {
            try {
                ret = SchemaRegistryConnectionMgr.connectionTest(serviceName, configs);
            } catch (Exception e) {
                LOG.error("<== RangerServiceSchemaRegistry.validateConfig Error:" + e);
                throw e;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceSchemaRegistry.validateConfig(" + serviceName + "): ret=" + ret);
        }

        return ret;
    }

    @Override
    public List<String> lookupResource(ResourceLookupContext context) throws Exception {
        List<String> ret = new ArrayList<String>();

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceSchemaRegistry.lookupResource(" + serviceName + ")");
        }

        if (configs != null) {
            ret = SchemaRegistryResourceMgr.getSchemaRegistryResources(serviceName, configs, context);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceSchemaRegistry.lookupResource(" + serviceName + "): ret=" + ret);
        }

        return ret;
    }

}
