/*
 * Copyright 2016 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.schemaregistry.webservice.RewriteUriFilter;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 *
 */
public class RewriteUriFilterTest {
    private static final Logger LOG = LoggerFactory.getLogger(RewriteUriFilterTest.class);

    @Test
    public void testRedirectPaths() throws Exception {
        String[] paths = {"/", "/subjects/", "/schemas/ids/", "/subjects", "/schemas/ids", "/subjects/1/", "/schemas/ids/2"};

        doTestForwardRedirectPaths(paths, false);
    }

    @Test
    public void testNonRedirectForwardPaths() throws Exception {
        final String[] paths = {"/ui", "/api/v1/confluent/", "/401.html", "/ui/401/html"};

        doTestForwardRedirectPaths(paths, true);
    }

    private void doTestForwardRedirectPaths(String[] paths,
                                            boolean filterChainShouldBeInvoked) throws ServletException, IOException {
        RewriteUriFilter rewriteUriFilter = new RewriteUriFilter();

        FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
        Mockito.when(filterConfig.getInitParameter("forwardPaths")).thenReturn("/api/v1/confluent,/subjects/*,/schemas/ids/*");
        Mockito.when(filterConfig.getInitParameter("redirectPaths")).thenReturn("/ui/,/");

        rewriteUriFilter.init(filterConfig);


        for (String requestPath : paths) {
            LOG.info("Received path [{}]", requestPath);
            HttpServletRequest servletRequest = Mockito.mock(HttpServletRequest.class);
            Mockito.when(servletRequest.getRequestURI()).thenReturn(requestPath);
            Mockito.when(servletRequest.getRequestDispatcher(Mockito.anyString()))
                   .thenReturn(Mockito.mock(RequestDispatcher.class));

            HttpServletResponse servletResponse = Mockito.mock(HttpServletResponse.class);

            MyFilterChain filterChain = new MyFilterChain();

            rewriteUriFilter.doFilter(servletRequest, servletResponse, filterChain);

            if(filterChainShouldBeInvoked) {
                Assert.assertTrue("Filter chain should have been invoked", filterChain.isFilterChainInvoked());
            } else {
                Assert.assertFalse("Filter chain should not have been invoked", filterChain.isFilterChainInvoked());
            }
        }
    }

    private static class MyFilterChain implements FilterChain {
        private boolean filterChainInvoked;

        @Override
        public void doFilter(ServletRequest request, ServletResponse response) throws IOException, ServletException {
            filterChainInvoked = true;
        }

        public boolean isFilterChainInvoked() {
            return filterChainInvoked;
        }
    }
}
