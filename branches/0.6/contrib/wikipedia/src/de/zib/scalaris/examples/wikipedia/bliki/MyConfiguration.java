/**
 *  Copyright 2011-2013 Zuse Institute Berlin
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package de.zib.scalaris.examples.wikipedia.bliki;

import info.bliki.wiki.model.Configuration;
import info.bliki.wiki.template.ITemplateFunction;

import java.net.URL;
import java.util.Map;
import java.util.TreeMap;

/**
 * Wiki configuration with its own interwiki and template maps.
 * 
 * Note: {@link Configuration} uses a static interwiki and template function
 * maps valid for all instances.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class MyConfiguration extends Configuration {
    /**
     * Map from the interwiki shortcut to the real Interwiki-URL
     */
    protected final Map<String, String> INTERWIKI_MAP = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);

    /**
     * Map the template's function name to the TemplateFunction implementation
     */
    protected final Map<String, ITemplateFunction> TEMPLATE_FUNCTION_MAP = new TreeMap<String, ITemplateFunction>(
            String.CASE_INSENSITIVE_ORDER);

    /**
     * Creates a wiki configuration with its own interwiki map.
     */
    public MyConfiguration() {
        INTERWIKI_MAP.putAll(Configuration.INTERWIKI_MAP);
        TEMPLATE_FUNCTION_MAP.putAll(Configuration.TEMPLATE_FUNCTION_MAP);
    }

    /**
     * Creates a wiki configuration with its own interwiki map. Removes the
     * interwiki mapping to the base of this wiki.
     * 
     * @param namespace
     *            the namespace to use
     */
    public MyConfiguration(MyNamespace namespace) {
        INTERWIKI_MAP.putAll(Configuration.INTERWIKI_MAP);
        try {
            /*
             * simple.wikipedia.org
             * simple.wiktionary.org
             */
            final String base = namespace.getSiteinfo().getBase();
            if (!base.isEmpty()) {
                String baseUrl = new URL(base).getHost();
                String domain = baseUrl.split("\\.")[1];
                INTERWIKI_MAP.remove(domain);
            }
        } catch (Exception e) {
            // ignore if the URL is not valid
            e.printStackTrace();
        }
        TEMPLATE_FUNCTION_MAP.putAll(Configuration.TEMPLATE_FUNCTION_MAP);
    }

    /* (non-Javadoc)
     * @see info.bliki.wiki.model.Configuration#getInterwikiMap()
     */
    @Override
    public Map<String, String> getInterwikiMap() {
        return INTERWIKI_MAP;
    }

    /* (non-Javadoc)
     * @see info.bliki.wiki.model.Configuration#addInterwikiLink(java.lang.String, java.lang.String)
     */
    @Override
    public String addInterwikiLink(String key, String value) {
        return INTERWIKI_MAP.put(key, value);
    }

    /* (non-Javadoc)
     * @see info.bliki.wiki.model.Configuration#addTemplateFunction(java.lang.String, info.bliki.wiki.template.ITemplateFunction)
     */
    @Override
    public ITemplateFunction addTemplateFunction(String key,
            ITemplateFunction value) {
        return TEMPLATE_FUNCTION_MAP.put(key, value);
    }

    /* (non-Javadoc)
     * @see info.bliki.wiki.model.Configuration#getTemplateMap()
     */
    @Override
    public Map<String, ITemplateFunction> getTemplateMap() {
        return TEMPLATE_FUNCTION_MAP;
    }

}
