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
package de.zib.scalaris.examples.wikipedia.plugin;

import javax.servlet.http.HttpServletRequest;

import de.zib.scalaris.examples.wikipedia.SavePageResult;
import de.zib.scalaris.examples.wikipedia.ValueResult;
import de.zib.scalaris.examples.wikipedia.bliki.NormalisedTitle;
import de.zib.scalaris.examples.wikipedia.bliki.WikiPageBean;
import de.zib.scalaris.examples.wikipedia.bliki.WikiPageBeanBase;
import de.zib.scalaris.examples.wikipedia.bliki.WikiPageEditBean;
import de.zib.scalaris.examples.wikipedia.bliki.WikiServlet;

/**
 * Simple handler of events in the
 * {@link de.zib.scalaris.examples.wikipedia.bliki.WikiServlet} class.
 * 
 * Note: this API is not stable and will probably change in future.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public interface WikiEventHandler {
    /**
     * Will be called after the user submitted a new revision to be stored in
     * the wiki.
     * 
     * Note that if the operation was not successful,
     * {@link #onPageView(WikiPageBeanBase, Object)} will also be called!
     * 
     * @param page
     *            some info on the edited page
     * @param result
     *            the result of the operation (may not be successful)
     * @param connection
     *            connection to the data store
     */
    public <Connection> void onPageSaved(WikiPageEditBean page, SavePageResult result,
            Connection connection);

    /**
     * Will be called for a page view. This also includes empty pages,
     * NoArticleText, BadTitle, unsuccessful data connections, page edit/preview
     * pages, special pages, page history pages etc.
     * 
     * @param page
     *            some info on the shown page
     * @param connection
     *            connection to the data store (may be <tt>null</tt>!)
     */
    public <Connection> void onPageView(WikiPageBeanBase page, Connection connection);

    /**
     * Will be called during a request for an image.
     * 
     * @param image
     *            the plain image name from the rendering
     * @param realImageUrl
     *            the translated URL pointing to the wikipedia
     */
    public void onImageRedirect(String image, String realImageUrl);

    /**
     * Will be called when a random page should be shown.
     * 
     * @param page
     *            some info about the call
     * @param result
     *            DB query result
     * @param connection
     *            connection to the data store
     */
    public <Connection> void onViewRandomPage(WikiPageBean page,
            ValueResult<NormalisedTitle> result, Connection connection);

    /**
     * Checks whether the given user is allowed to access the wiki.
     * 
     * @param <Connection>
     * 
     * @param serviceUser
     *            the provided username
     * @param request
     *            the request object (allows reading parameters or writing
     *            status messages, e.g. errors using
     *            {@link WikiServlet#setParam_error(HttpServletRequest, String)}
     * @param connection
     *            connection to the data store
     * 
     * @return <tt>true</tt> if access is allowed, <tt>false</tt> otherwise
     */
    public <Connection> boolean checkAccess(String serviceUser, HttpServletRequest request,
            Connection connection);

    /**
     * Gets a descriptive name of this event handler.
     * 
     * @return a (HTML) string
     */
    public String getName();

    /**
     * Gets a URL where to get the plugin from or get more information about it.
     * 
     * @return a URL
     */
    public String getURL();

    /**
     * Gets a descriptive version number of this event handler.
     * 
     * @return a (HTML) string
     */
    public String getVersion();

    /**
     * Gets a brief description of what this event handler is doing.
     * 
     * @return a (HTML) string
     */
    public String getDescription();

    /**
     * Gets the name of the author(s) of this event handler.
     * 
     * @return a (HTML) string
     */
    public String getAuthor();
}
