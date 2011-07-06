/**
 *  Copyright 2011 Zuse Institute Berlin
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
package de.zib.scalaris.examples.wikipedia;

import de.zib.scalaris.examples.wikipedia.bliki.WikiServlet;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;

/**
 * Interface for classes accessing the {@link WikiServlet} class without the
 * need to include the servlet API.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public interface WikiServletContext {
    /**
     * Gets the namespace of the wiki.
     * 
     * @return the namespace
     */
    public abstract NamespaceUtils getNamespace();

    /**
     * Gets the siteinfo of the wiki.
     * 
     * @return the siteinfo
     */
    public abstract SiteInfo getSiteinfo();

    /**
     * Gets the version of the wiki servlet.
     * 
     * @return the version
     */
    public abstract String getVersion();

    /**
     * Gets the base URL for the wiki servlet relative to the servlet's context
     * path.
     * 
     * @return the wikibaseurl
     */
    public abstract String getWikibaseurl();

    /**
     * Gets the base URL for links to articles relative to the servlet's context
     * path.
     * 
     * @return the linkbaseurl
     */
    public abstract String getLinkbaseurl();

    /**
     * Gets the base URL for links to images relative to the servlet's context
     * path.
     * 
     * @return the imagebaseurl
     */
    public abstract String getImagebaseurl();
}