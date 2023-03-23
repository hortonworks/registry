/*
 * Copyright 2016-2019 Cloudera, Inc.
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
 */
package com.hortonworks.registries.schemaregistry.client;

import java.util.LinkedList;

/**
 * This class defines strategy to failover to a url when the current chosen url is considered to be failed.
 */
public class FailoverUrlSelector extends AbstractUrlSelector {
    private final LinkedList<String> failedUrls;
    private volatile String current;

    public FailoverUrlSelector(String clusterUrl) {
        super(clusterUrl);
        failedUrls = new LinkedList<>();
        current = urls[0];
    }

    @Override
    public String select() {
        return current;
    }

    @Override
    public void urlWithError(String url, Exception e) {
        if (failedError(e)) {
            synchronized (failedUrls) {
                if (!failedUrls.contains(url)) {
                    failedUrls.add(url);
                }
                if (failedUrls.size() == urls.length) {
                    current = failedUrls.remove();
                } else if (current.equals(url)) {
                    for (String s : urls) {
                        if (!failedUrls.contains(s)) {
                            current = s;
                            break;
                        }
                    }
                }
            }
        }
    }

    /**
     * Returns true if the given Exception indicates the respective URL can be treated as failed.
     *
     * @param ex
     */
    protected boolean failedError(Exception ex) {
        return true;
    }

}
