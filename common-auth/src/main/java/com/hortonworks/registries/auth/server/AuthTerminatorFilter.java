/**
 * Copyright 2016-2022 Cloudera, Inc.
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
 **/
package com.hortonworks.registries.auth.server;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/** When joining multiple auth filters into a composite filter, the last filter
 * needs to be the terminator filter, which terminates the chain. If the preceeding
 * auth filters managed to authenticate the user, the terminator filter will let
 * the request pass. If not, a HTTP Unauthorized will be returned. */
public class AuthTerminatorFilter extends AuthenticationFilter {

    @Override
    public void init(FilterConfig filterConfig) throws ServletException { }

    @Override
    public void destroy() { }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        Object value = httpRequest.getAttribute(AUTH_FAILED);

        if (value == null || "false".equals(value)) {
            chain.doFilter(request, response);
        } else {
            respondWithUnauthorized(request, httpResponse, HttpServletResponse.SC_UNAUTHORIZED, null);
        }
    }


}
