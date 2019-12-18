package com.hortonworks.registries.schemaregistry.authorization.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.ranger.admin.client.AbstractRangerAdminClient;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.ServiceTags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.List;

/**
 * A test implementation of the RangerAdminClient interface that just reads policies from a file and returns them
 */
public class TestRangerAdminClientImpl extends AbstractRangerAdminClient {
    private static final Logger LOG = LoggerFactory.getLogger(TestRangerAdminClientImpl.class);
    private final static String cacheFilename = "schema-registry-policies.json";
    private final static String tagFilename = "schema-registry-policies-tag.json";
    private Gson gson;

    public void init(String serviceName, String appId, String configPropertyPrefix) {
        Gson gson = null;
        try {
            gson = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create();
        } catch(Throwable excp) {
            LOG.error("TestRangerAdminClientImpl: failed to create GsonBuilder object", excp);
        }
        this.gson = gson;
    }

    public ServicePolicies getServicePoliciesIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception {

        String basedir = System.getProperty("basedir");
        if (basedir == null) {
            basedir = new File(".").getCanonicalPath();
        }

        java.nio.file.Path cachePath = FileSystems.getDefault().getPath(basedir, "/src/test/resources/" + cacheFilename);
        byte[] cacheBytes = Files.readAllBytes(cachePath);

        return gson.fromJson(new String(cacheBytes), ServicePolicies.class);
    }

    public ServiceTags getServiceTagsIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception {
        String basedir = System.getProperty("basedir");
        if (basedir == null) {
            basedir = new File(".").getCanonicalPath();
        }

        java.nio.file.Path cachePath = FileSystems.getDefault().getPath(basedir, "/src/test/resources/" + tagFilename);
        byte[] cacheBytes = Files.readAllBytes(cachePath);

        return gson.fromJson(new String(cacheBytes), ServiceTags.class);
    }

    public List<String> getTagTypes(String tagTypePattern) throws Exception {
        return null;
    }

    
}