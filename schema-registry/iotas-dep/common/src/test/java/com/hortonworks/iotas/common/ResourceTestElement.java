/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.iotas.common;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;

/**
 * A Test element holder class
 */
public class ResourceTestElement {
    private final Object resourceToPost; // resource that will be used to test post
    private final Object resourceToPut; // resource that will be used to test put
    private final String id; //Id by which Get(id) and Delete(id) will be tested, should match the actual Id set in post/put request.
    private final String url; //Rest Url to test.
    private boolean multipart;
    private String entityNameHeader;
    private String fileNameHeader;
    private File fileToUpload;
    private List<String> fieldsToIgnore;

    public ResourceTestElement(Object resourceToPost, Object resourceToPut, String id, String url) {
        this.resourceToPost = resourceToPost;
        this.resourceToPut = resourceToPut;
        this.id = id;
        this.url = url;
    }

    public ResourceTestElement withMultiPart() {
        this.multipart = true;
        return this;
    }

    public ResourceTestElement withEntitiyNameHeader(String entitiyNameHeader) {
        this.entityNameHeader = entitiyNameHeader;
        return this;
    }

    public ResourceTestElement withFileNameHeader(String fileNameHeader) {
        this.fileNameHeader = fileNameHeader;
        return this;
    }

    public ResourceTestElement withFileToUpload(String fileName) {
        try {
            this.fileToUpload = Paths.get(this.getClass().getClassLoader().getResource(fileName).toURI()).toFile();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public ResourceTestElement withFieldsToIgnore(List<String> fields) {
        this.fieldsToIgnore = fields;
        return this;
    }

    public String getFileNameHeader() {
        return fileNameHeader;
    }

    public File getFileToUpload() {
        return fileToUpload;
    }

    public String getEntityNameHeader() {
        return entityNameHeader;
    }

    public Object getResourceToPost() {
        return resourceToPost;
    }

    public Object getResourceToPut() {
        return resourceToPut;
    }

    public String getId() {
        return id;
    }

    public String getUrl() {
        return url;
    }

    public boolean isMultipart() {
        return multipart;
    }

    public List<String> getFieldsToIgnore() {
        return fieldsToIgnore;
    }
}
