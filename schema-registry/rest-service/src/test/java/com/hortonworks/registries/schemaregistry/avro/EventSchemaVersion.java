package com.hortonworks.registries.schemaregistry.avro;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class EventSchemaVersion {

    private final String name;
    private final String description;
    private final int version;
    private final String group;
    private final String content;
    private final Type type;

    /**
     *
     * @param name - schema name.
     * @param description - brief description of the schema.
     * @param group - group name the schema belongs to.
     * @param content - input stream.
     * @param version - schema version.
     * @param type - schema type.
     * @return - EventSchemaVersion
     * @throws IOException - thrown when IO operations fail.
     */
    public static EventSchemaVersion fromIO(String name, String description, String group, InputStream content, int version, Type type) throws IOException {
        String schema = getSchemaContentFromStream(content);
        return new EventSchemaVersion(name, description, group, schema, version, type);
    }

    /**
     * Constructs new immutable {@link EventSchemaVersion} object.
     *
     * @param name        - schema name.
     * @param description - short description about the schema.
     * @param group       - group the schema belongs to
     * @param content     - schema content.
     * @param version     - schema version.
     * @param type        - schema type. E.g., avro, xsd etc.
     */
    public EventSchemaVersion(String name, String description, String group, String content, int version, Type type) {
        this.name = name;
        this.description = description;
        this.group = group;
        this.content = content;
        this.version = version;
        this.type = type;
    }

    /**
     * Get schema name.
     *
     * @return - schema name.
     */
    public String getName() {
        return name;
    }

    /**
     * Get schema description.
     *
     * @return - schema description.
     */
    public String getDescription() {
        return description;
    }

    /**
     * Get schema group.
     *
     * @return - group the schema belongs to.
     */
    public String getGroup() {
        return group;
    }

    /**
     * Get schema version.
     *
     * @return - schema version.
     */
    public int getVersion() {
        return version;
    }

    /**
     * Get schema content.
     *
     * @return - schema content.
     */
    public String getContent() {
        return content;
    }

    /**
     * Get schema type.
     *
     * @return - schema type.
     */
    public Type getType() {
        return type;
    }

    private static String getSchemaContentFromStream(InputStream schemaInputStream) throws IOException {
        BufferedInputStream bis = new BufferedInputStream(schemaInputStream);
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            int size = bis.available();
            while (size > 0) {
                byte[] b = new byte[size];
                bis.read(b);
                baos.write(b);
                size = bis.available();
            }
            return new String(baos.toByteArray(), Charset.defaultCharset());
        } finally {
            bis.close();
        }
    }
}
