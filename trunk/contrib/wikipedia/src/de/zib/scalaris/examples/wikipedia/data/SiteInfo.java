/**
 *  Copyright 2007-2011 Zuse Institute Berlin
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
package de.zib.scalaris.examples.wikipedia.data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents generic site information.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class SiteInfo implements Serializable {
    /**
     * Version for serialisation.
     */
    private static final long serialVersionUID = 1L;
    
    protected String base;
    protected String sitename;
    protected String generator;
    protected String caseStr;
    /**
     * Maps namespace keys to a map with the following two entries:
     * <ul>
     * <li><tt>{@link #NAMESPACE_PREFIX}</tt>: prefix of the namespace</li>
     * <li><tt>{@link #NAMESPACE_CASE}</tt>: case of the namespace, e.g. "first-letter"</li>
     * </ul>
     */
    protected Map<String, Map<String, String>> namespaces;
    
    /**
     * Key for getting the namespace prefix in the maps contained in
     * {@link #namespaces}.
     * 
     * @see #getNamespaces()
     */
    public final static String NAMESPACE_PREFIX = "prefix";

    /**
     * Key for getting the namespace case in the maps contained in
     * {@link #namespaces}.
     * 
     * @see #getNamespaces()
     */
    public final static String NAMESPACE_CASE = "case";

    /**
     * Creates a site info object with the given data.
     */
    public SiteInfo() {
        this.base = "";
        this.sitename = "";
        this.generator = "";
        this.caseStr = "";
        this.namespaces = new HashMap<String, Map<String, String>>();
    }

    /**
     * Creates a site info object with the given data.
     * 
     * @param base
     *            the url of the main site
     * @param sitename
     *            the name of the site
     * @param generator
     *            the generator of the site (MediaWiki version string)
     * @param caseStr
     *            the case option of the site
     * @param namespaces
     *            the namespaces of the site
     */
    public SiteInfo(String base, String sitename, String generator, String caseStr, Map<String, Map<String, String>> namespaces) {
        this.base = base;
        this.sitename = sitename;
        this.generator = generator;
        this.caseStr = caseStr;
        this.namespaces = namespaces;
    }

    /**
     * Gets the base URL of the site.
     * 
     * @return the base URL
     */
    public String getBase() {
        return base;
    }

    /**
     * Sets the base URL of the site.
     * 
     * @param base the base URL to set
     */
    public void setBase(String base) {
        this.base = base;
    }

    /**
     * Gets the site's name.
     * 
     * @return the sitename
     */
    public String getSitename() {
        return sitename;
    }

    /**
     * Sets the site's name.
     * 
     * @param sitename the sitename to set
     */
    public void setSitename(String sitename) {
        this.sitename = sitename;
    }

    /**
     * Gets the site's generator (MediaWiki version string).
     * 
     * @return the generator
     */
    public String getGenerator() {
        return generator;
    }

    /**
     * Sets the site's generator (MediaWiki version string).
     * 
     * @param generator the generator to set
     */
    public void setGenerator(String generator) {
        this.generator = generator;
    }

    /**
     * Gets the namespace mapping.
     * 
     * Maps namespace keys to a map with the following two entries:
     * <ul>
     * <li><tt>{@link #NAMESPACE_PREFIX}</tt>: prefix of the namespace</li>
     * <li><tt>{@link #NAMESPACE_CASE}</tt>: case of the namespace, e.g. "first-letter"</li>
     * </ul>
     * 
     * @return the namespace
     */
    public Map<String, Map<String, String>> getNamespaces() {
        return namespaces;
    }

    /**
     * Sets the namespace mapping.
     * 
     * @param namespaces the namespace to set
     */
    public void setNamespaces(Map<String, Map<String, String>> namespaces) {
        this.namespaces = namespaces;
    }

    /**
     * Gets the case option of the site.
     * 
     * @return the case
     */
    public String getCase() {
        return caseStr;
    }

    /**
     * Sets the case option of the site.
     * 
     * @param caseStr the case to set
     */
    public void setCase(String caseStr) {
        this.caseStr = caseStr;
    }
}
