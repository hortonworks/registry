/**
 * Copyright 2017-2019 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.auth.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.util.Locale;

public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
    /**
     * Create a new thread
     * @param name The name of the thread
     * @param runnable The work for the thread to do
     * @param daemon Should the thread block JVM shutdown?
     * @return The unstarted thread
     */
    public static Thread newThread (String name, Runnable runnable, boolean daemon) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(daemon);
        thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error("Uncaught exception in thread '" + t.getName() + "':", e);
            }
        });
        return thread;
    }

    /**
     * Return non browser user agents as an array from a config string
     * @param nonBrowserUserAgentsConfig config string
     * @return array of strings converted to lowercase where each string is a non browser user agent
     */
    public static String[] getNonBrowserUserAgents (String nonBrowserUserAgentsConfig) {
        String[] result = nonBrowserUserAgentsConfig.split("\\W*,\\W*");
        for (int i = 0; i < result.length; i++) {
            result[i] = result[i].toLowerCase(Locale.ENGLISH);
        }
        return result;
    }

    /**
     * Checks if HttpServletRequest is from a browser by inspecting header named User-Agent
     * @param nonBrowserUserAgents array of strings representing non browser user agents
     * @param request http request
     * @return true if header is not present in array representing non browser agents, false otherwise
     */
    public static boolean isBrowser(String[] nonBrowserUserAgents, HttpServletRequest request) {
        String userAgent = request.getHeader("User-Agent");
        LOG.debug("User agent is " + userAgent);
        if (userAgent == null) {
            return false;
        }
        userAgent = userAgent.toLowerCase(Locale.ENGLISH);
        boolean isBrowser = true;
        for (String nonBrowserUserAgent : nonBrowserUserAgents) {
            if (userAgent.contains(nonBrowserUserAgent)) {
                isBrowser = false;
                break;
            }
        }
        return isBrowser;
    }

    /**
     * Checks if the given CharSequence has any blank space.
     */
    public static boolean isBlank(CharSequence cs) {
        if (cs == null || cs.length() == 0) {
            return true;
        }

        int strLen = cs.length();
        for(int i = 0; i < strLen; ++i) {
            if (!Character.isWhitespace(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
