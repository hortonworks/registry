/**
 * Copyright 2016 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.schemaregistry;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.io.Serializable;

/**
 *
 * todo: Better to store serializer and deserializer in pairs instead of individual entities.
 */
public class SerDesInfo implements Serializable {
    protected Long id;
    protected String description;
    protected String name;
    protected String fileId;
    protected String className;

    @JsonProperty
    protected boolean isSerializer;

    protected SerDesInfo() {
    }

    protected SerDesInfo(Long id, String name, String description, String fileId, String className, boolean isSerializer) {
        this.id = id;
        this.description = description;
        this.name = name;
        this.fileId = fileId;
        this.className = className;
        this.isSerializer = isSerializer;
    }

    public Long getId() {
        return id;
    }

    public String getDescription() {
        return description;
    }

    public String getName() {
        return name;
    }

    public String getFileId() {
        return fileId;
    }

    public String getClassName() {
        return className;
    }

    public boolean getIsSerializer() {
        return isSerializer;
    }

    @Override
    public String toString() {
        return "SerDesInfo{" +
                "id=" + id +
                ", description='" + description + '\'' +
                ", name='" + name + '\'' +
                ", fileId='" + fileId + '\'' +
                ", className='" + className + '\'' +
                ", isSerializer=" + isSerializer +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SerDesInfo that = (SerDesInfo) o;

        if (isSerializer != that.isSerializer) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (description != null ? !description.equals(that.description) : that.description != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (fileId != null ? !fileId.equals(that.fileId) : that.fileId != null) return false;
        return className != null ? className.equals(that.className) : that.className == null;

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (fileId != null ? fileId.hashCode() : 0);
        result = 31 * result + (className != null ? className.hashCode() : 0);
        result = 31 * result + (isSerializer ? 1 : 0);
        return result;
    }

    public static class Builder {
        private Long id;
        private String description;
        private String name;
        private String fileId;
        private String className;

        public Builder() {
        }

        public Builder id(Long id) {
            this.id = id;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder fileId(String fileId) {
            this.fileId = fileId;
            return this;
        }

        public Builder className(String className) {
            this.className = className;
            return this;
        }

        public SerDesInfo buildSerializerInfo() {
            validate();

            return new SerDesInfo(id, name, description, fileId, className, true);
        }

        public SerDesInfo buildDeserializerInfo() {
            validate();

            return new SerDesInfo(id, name, description, fileId, className, false);
        }

        private void validate() {
            Preconditions.checkNotNull(name, "name can not be null");
            Preconditions.checkNotNull(fileId, "fileId can not be null");
            Preconditions.checkNotNull(className, "className can not be null");
        }

    }

}
