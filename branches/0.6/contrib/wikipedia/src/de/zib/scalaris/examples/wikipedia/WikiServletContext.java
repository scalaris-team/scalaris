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
package de.zib.scalaris.examples.wikipedia;

import de.zib.scalaris.examples.wikipedia.bliki.WikiPageBeanBase;
import de.zib.scalaris.examples.wikipedia.bliki.WikiServlet;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;
import de.zib.scalaris.examples.wikipedia.plugin.WikiEventHandler;

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
     * Gets the version of the DB used by the wiki servlet.
     * 
     * @return the version
     */
    public abstract String getDbVersion();

    /**
     * Gets the version of the Server running the wiki servlet.
     * 
     * @return the version
     */
    public abstract String getServerVersion();

    /**
     * Gets the version of the bliki rendering library used by the wiki servlet.
     * 
     * @return the version
     */
    public abstract String getBlikiVersion();

    /**
     * Gets the base URL for links to articles relative to the servlet's context
     * path.
     * 
     * @param page
     *            the page to get the URL for
     * 
     * @return the linkbaseurl
     */
    public abstract String getLinkbaseurl(WikiPageBeanBase page);

    /**
     * Gets the base URL for links to images relative to the servlet's context
     * path.
     * 
     * @param page
     *            the page to get the URL for
     * 
     * @return the imagebaseurl
     */
    public abstract String getImagebaseurl(WikiPageBeanBase page);

    /**
     * Adds the given event handler to the list of event handlers.
     * 
     * @param handler
     *            the event handler to add
     */
    public abstract void registerEventHandler(WikiEventHandler handler);
    
    /**
     * Called at the end of each <tt>jsp</tt> for storing user requests.
     * 
     * Adds a user request to the user request log if enabled by setting
     * {@link Options#LOG_USER_REQS} to a value larger than <tt>0</tt>. Also
     * calls {@link WikiEventHandler#onPageView(WikiPageBeanBase, Object)} for
     * each registered event handler no matter what
     * {@link Options#LOG_USER_REQS} is set.
     * 
     * @param page
     *            some info on the shown page
     * @param servertime
     *            time spend in the web server
     */
    public abstract void storeUserReq(WikiPageBeanBase page, long servertime);
}