/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.zib.scalaris.examples.wikipedia.tomcat;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Enumeration;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

/**
 * Filter that will reject requests if there was a failure during parameter
 * parsing. This filter can be used to ensure that none parameter values
 * submitted by client are lost.
 *
 * <p>
 * Note that it has side effect that it triggers parameter parsing and thus
 * consumes the body for POST requests. Parameter parsing does check content
 * type of the request, so there should not be problems with addresses that use
 * <code>request.getInputStream()</code> and <code>request.getReader()</code>,
 * if requests parsed by them do not use standard value for content mime-type.
 */
public class FailedRequestFilter implements Filter {
    String PARAMETER_PARSE_FAILED_ATTR = null;
    
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        Enumeration<String> paramNames = filterConfig.getInitParameterNames();
        while (paramNames.hasMoreElements()) {
            String paramName = paramNames.nextElement();
            String msg = "The property " + paramName
                    + " is not defined for filters of type "
                    + this.getClass().getName();
            throw new ServletException(msg);
        }
        // apache tomcat:
        try {
            Class<?> globals = Class.forName("org.apache.catalina.Globals");
            Field f = globals.getDeclaredField("PARAMETER_PARSE_FAILED_ATTR");
            PARAMETER_PARSE_FAILED_ATTR = (String) f.get(null);
        } catch (ClassNotFoundException e) {
        } catch (SecurityException e) {
            throw new ServletException(e);
        } catch (NoSuchFieldException e) {
            throw new ServletException(e);
        } catch (IllegalAccessException e) {
            throw new ServletException(e);
        }
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
            FilterChain chain) throws IOException, ServletException {
        if (!isGoodRequest(request)) {
            ((HttpServletResponse) response)
                    .sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }
        chain.doFilter(request, response);
    }

    private boolean isGoodRequest(ServletRequest request) throws ServletException {
        // Trigger parsing of parameters
        try {
            request.getParameter("none");
            // Detect failure (apache tomcat)
            if (PARAMETER_PARSE_FAILED_ATTR != null
                    && request.getAttribute(PARAMETER_PARSE_FAILED_ATTR) != null) {
                return false;
            }
            return true;
        } catch (IllegalArgumentException e) {
            // Detect failure (jetty)
            return false;
        }
    }

    @Override
    public void destroy() {
        // NOOP
    }

}
