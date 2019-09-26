package org.apache.ranger.services.schemaregistry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.schemaregistry.client.SchemaRegistryConnectionMgr;
import org.apache.ranger.services.schemaregistry.client.SchemaRegistryResourceMgr;

import java.util.*;

public class RangerServiceSchemaRegistry extends RangerBaseService {

    private static final Log LOG = LogFactory.getLog(RangerServiceSchemaRegistry.class);

    public static final String RESOURCE_FILE = "file";
    public static final String RESOURCE_METADATA = "metadata";
    public static final String RESOURCE_SERDE = "serde";
    public static final String RESOURCE_SCHEMA = "schema";
    public static final String RESOURCE_BRANCH = "branch";
    public static final String RESOURCE_SCHEMA_VERSION = "schema-version";

    public static final String ACCESS_TYPE_CREATE = "create";
    public static final String ACCESS_TYPE_READ = "read";
    public static final String ACCESS_TYPE_UPDATE = "update";
    public static final String ACCESS_TYPE_DELETE    = "delete";
    public static final String WILDCARD_ASTERISK  = "*";

    public static final String METADATA_POLICYNAME = "default database tables columns";

    @Override
    public Map<String, Object> validateConfig() throws Exception {
        Map<String, Object> ret = new HashMap<String, Object>();

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

    @Override
    public List<RangerPolicy> getDefaultRangerPolicies() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceSchemaRegistry.getDefaultRangerPolicies()");
        }

        List<RangerPolicy> ret = super.getDefaultRangerPolicies();

        //Default policy for metadata resource
        RangerPolicy metadataPolicy = createMetadataDefaultPolicy();
        ret.add(metadataPolicy);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceSchemaRegistry.getDefaultRangerPolicies()");
        }

        return ret;
    }

    private RangerPolicy createMetadataDefaultPolicy() {
        RangerPolicy metadataDefaultPolicy = new RangerPolicy();

        metadataDefaultPolicy.setName(METADATA_POLICYNAME);
        metadataDefaultPolicy.setService(serviceName);
        metadataDefaultPolicy.setResources(createDefaultMetadataPolicyResource());
        metadataDefaultPolicy.setPolicyItems(createDefaultMetadataPolicyItem());

        return metadataDefaultPolicy;
    }

    private Map<String, RangerPolicy.RangerPolicyResource> createDefaultMetadataPolicyResource() {
        Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();

        resources.put(RESOURCE_METADATA, new RangerPolicy.RangerPolicyResource(Arrays.asList(RESOURCE_METADATA), false, false));

        return resources;
    }

    private List<RangerPolicy.RangerPolicyItem> createDefaultMetadataPolicyItem() {
        List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<RangerPolicy.RangerPolicyItemAccess>();

        accesses.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_READ));

        RangerPolicy.RangerPolicyItem item = new RangerPolicy.RangerPolicyItem(accesses, null, Arrays.asList(RangerPolicyEngine.GROUP_PUBLIC), null, null, false);

        return Collections.singletonList(item);
    }


}
