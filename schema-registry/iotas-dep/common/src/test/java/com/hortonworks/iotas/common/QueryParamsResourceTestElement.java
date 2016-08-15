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

import java.util.List;

/**
 * A Test element holder class for testing resources that support a get
 * with query params
 */
public class QueryParamsResourceTestElement {
    public final List<Object> resourcesToPost; // resources that will be posted to postUrl
    public final String postUrl; // Rest Url to post.
    // get urls with different query parameters. To be called before
    // and after post
    public final List<String> getUrls;
    // expected results for each get url above after the post. should be
    // same length as getUrls
    public final List<List<Object>> getResults;

    public QueryParamsResourceTestElement(List<Object> resourcesToPost, String postUrl, List<String> getUrls, List<List<Object>> getResults) {
        this.resourcesToPost = resourcesToPost;
        this.postUrl = postUrl;
        this.getUrls = getUrls;
        this.getResults = getResults;
    }
}
