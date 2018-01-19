package com.hortonworks.registries.schemaregistry;


import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.PrimaryKey;
import com.hortonworks.registries.storage.catalog.AbstractStorable;

import java.util.HashMap;
import java.util.Map;

public class HostConfigStorable extends AbstractStorable {

    public static final String NAME_SPACE = "host_config";

    public static final String ID = "id";
    public static final String HOST_URL = "hostUrl";
    public static final String TIMESTAMP = "timestamp";

    private Long id;
    private String hostUrl;
    private Long timestamp;

    @Override
    public String getNameSpace() {
        return NAME_SPACE;
    }

    @Override
    public PrimaryKey getPrimaryKey() {
        Map<Schema.Field, Object> values = new HashMap<>();
        values.put(new Schema.Field(HOST_URL, Schema.Type.STRING), hostUrl);
        return new PrimaryKey(values);
    }

    public HostConfigStorable() {

    }

    public HostConfigStorable(Long id, String hostUrl, Long timestamp) {
        this.id = id;
        this.hostUrl = hostUrl;
        this.timestamp = timestamp;
    }

    public HostConfigStorable(String hostUrl) {
        this.hostUrl = hostUrl;
    }

    public void setId(Long id) {
        this.id =id;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public void setHostUrl(String hostUrl) {
        this.hostUrl = hostUrl;
    }

    public String getHostUrl() {
        return this.hostUrl;
    }

    public Long getId() {
        return this.id;
    }

    public Long getTimestamp() {
        return this.timestamp;
    }

}
