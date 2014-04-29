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

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Random;

import de.zib.scalaris.examples.wikipedia.PageHistoryResult;
import de.zib.scalaris.examples.wikipedia.RevisionResult;
import de.zib.scalaris.examples.wikipedia.SavePageResult;
import de.zib.scalaris.examples.wikipedia.ValueResult;
import de.zib.scalaris.examples.wikipedia.data.Contribution;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;

/**
 * Interface for data-retrieving methods used by {@link WikiServlet}.
 * 
 * @param <Connection> connection to a DB
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public interface WikiServletDataHandler<Connection> {
    /**
     * Gets the key to store {@link SiteInfo} objects at.
     * 
     * @return DB key
     */
    public String getSiteInfoKey();
    
    /**
     * Gets the key to store the list of pages in the given namespace at.
     * 
     * @param namespace  the namespace ID
     * 
     * @return DB key
     */
    public String getPageListKey(int namespace);
    
    /**
     * Gets the key to store the number of pages at.
     * 
     * @param namespace  the namespace ID
     * 
     * @return DB key
     */
    public String getPageCountKey(int namespace);
    
    /**
     * Gets the key to store the number of articles, i.e. pages in the main
     * namespace, at.
     * 
     * @return DB key
     */
    public String getArticleCountKey();
    
    /**
     * Gets the key to store {@link Revision} objects at.
     * 
     * @param title
     *            the title of the page
     * @param id
     *            the id of the revision
     * @param nsObject
     *            the namespace for page title normalisation
     * 
     * @return DB key
     */
    public String getRevKey(String title, int id, final MyNamespace nsObject);
    
    /**
     * Gets the key to store {@link Page} objects at.
     * 
     * @param title
     *            the title of the page
     * @param nsObject
     *            the namespace for page title normalisation
     * 
     * @return DB key
     */
    public String getPageKey(String title, final MyNamespace nsObject);
    
    /**
     * Gets the key to store the list of revisions of a page at.
     * 
     * @param title
     *            the title of the page
     * @param nsObject
     *            the namespace for page title normalisation
     * 
     * @return DB key
     */
    public String getRevListKey(String title, final MyNamespace nsObject);
    
    /**
     * Gets the key to store the list of pages belonging to a category at.
     * 
     * @param title
     *            the category title (including <tt>Category:</tt>)
     * @param nsObject
     *            the namespace for page title normalisation
     * 
     * @return DB key
     */
    public String getCatPageListKey(String title, final MyNamespace nsObject);
    
    /**
     * Gets the key to store the number of pages belonging to a category at.
     * 
     * @param title
     *            the category title (including <tt>Category:</tt>)
     * @param nsObject
     *            the namespace for page title normalisation
     * 
     * @return DB key
     */
    public String getCatPageCountKey(String title, final MyNamespace nsObject);
    
    /**
     * Gets the key to store the list of pages using a template at.
     * 
     * @param title
     *            the template title (including <tt>Template:</tt>)
     * @param nsObject
     *            the namespace for page title normalisation
     * 
     * @return DB key
     */
    public String getTplPageListKey(String title, final MyNamespace nsObject);
    
    /**
     * Gets the key to store the list of pages linking to the given title.
     * 
     * @param title
     *            the page's title
     * @param nsObject
     *            the namespace for page title normalisation
     * 
     * @return DB key
     */
    public String getBackLinksPageListKey(String title, final MyNamespace nsObject);
    
    /**
     * Gets the key to store the number of page edits.
     * 
     * @return DB key
     */
    public String getStatsPageEditsKey();
    
    /**
     * Gets the key to store the list of contributions of a user.
     * 
     * @param contributor  the user name or IP address of the user who created
     *                     the revision
     * 
     * @return DB key
     */
    public String getContributionListKey(String contributor);

    
    /**
     * Retrieves the back-ends version string.
     * 
     * @param connection
     *            the connection to the DB
     * 
     * @return a result object with the version string on success
     */
    public ValueResult<String> getDbVersion(Connection connection);

    /**
     * Retrieves a page's history from the DB.
     * 
     * @param connection
     *            the connection to the DB
     * @param title
     *            the title of the page
     * @param nsObject
     *            the namespace for page title normalisation
     * 
     * @return a result object with the page history on success
     */
    public PageHistoryResult getPageHistory(Connection connection,
            String title, final MyNamespace nsObject);

    /**
     * Retrieves the current, i.e. most up-to-date, version of a page from
     * the DB.
     * 
     * @param connection
     *            the connection to the DB
     * @param title
     *            the title of the page
     * @param nsObject
     *            the namespace for page title normalisation
     * 
     * @return a result object with the page and revision on success
     */
    public RevisionResult getRevision(Connection connection, String title,
            final MyNamespace nsObject);
    
    /**
     * Retrieves the given version of a page from the DB.
     * 
     * @param connection
     *            the connection to the DB
     * @param title
     *            the title of the page
     * @param id
     *            the id of the version
     * @param nsObject
     *            the namespace for page title normalisation
     * 
     * @return a result object with the page and revision on success
     */
    public RevisionResult getRevision(Connection connection, String title,
            int id, final MyNamespace nsObject);
    
    /**
     * Retrieves a list of all available pages from the DB.
     * 
     * @param connection
     *            the connection to the DB
     * 
     * @return a result object with the page list on success
     */
    public ValueResult<List<NormalisedTitle>> getPageList(Connection connection);
    
    /**
     * Retrieves a list of available pages in the given namespace from the DB.
     * 
     * @param namespace
     *            the namespace ID
     * @param connection
     *            the connection to the DB
     * 
     * @return a result object with the page list on success
     */
    public ValueResult<List<NormalisedTitle>> getPageList(int namespace, Connection connection);
    
    /**
     * Retrieves a list of pages in the given category from the DB.
     * 
     * @param connection
     *            the connection to the DB
     * @param title
     *            the title of the category
     * 
     * @return a result object with the page list on success
     */
    public ValueResult<List<NormalisedTitle>> getPagesInCategory(Connection connection,
            NormalisedTitle title);
    
    /**
     * Retrieves a list of pages using the given template from the DB.
     * 
     * @param connection
     *            the connection to the DB
     * @param title
     *            the title of the template
     * 
     * @return a result object with the page list on success
     */
    public ValueResult<List<NormalisedTitle>> getPagesInTemplate(Connection connection,
            NormalisedTitle title);

    /**
     * Retrieves a list of pages using the given templates from the DB.
     * 
     * @param connection
     *            the connection to the DB
     * @param titles
     *            the titles of the templates
     * @param pageTitle
     *            the title of the page to retrieve the list for (will be
     *            included in the statname)
     * 
     * @return a result object with the page list on success
     */
    public ValueResult<List<NormalisedTitle>> getPagesInTemplates(Connection connection,
            List<NormalisedTitle> titles, String pageTitle);
    
    /**
     * Retrieves a list of pages linking to the given page from the DB.
     * 
     * @param connection
     *            the connection to the DB
     * @param title
     *            the title of the page
     * @param nsObject
     *            the namespace for page title normalisation
     * 
     * @return a result object with the page list on success
     */
    public ValueResult<List<NormalisedTitle>> getPagesLinkingTo(Connection connection,
            String title, final MyNamespace nsObject);

    /**
     * Retrieves a list of pages linking to the given page from the DB.
     * 
     * @param connection
     *            the connection to the DB
     * @param contributor
     *            the user name or IP address of the user who created the
     *            revision
     * 
     * @return a result object with the page list on success
     */
    public ValueResult<List<Contribution>> getContributions(
            Connection connection, String contributor);
    
    /**
     * Retrieves the number of all available pages from the DB.
     * 
     * @param connection
     *            the connection to the DB
     * 
     * @return a result object with the number of pages on success
     */
    public ValueResult<BigInteger> getPageCount(Connection connection);
    
    /**
     * Retrieves the number of available pages in the given namespace from the
     * DB.
     * 
     * @param namespace
     *            the namespace ID
     * @param connection
     *            the connection to the DB
     * 
     * @return a result object with the number of pages on success
     */
    public ValueResult<BigInteger> getPageCount(int namespace, Connection connection);
    
    /**
     * Retrieves the number of available articles, i.e. pages in the main
     * namespace, from the DB.
     * 
     * @param connection
     *            the connection to the DB
     * 
     * @return a result object with the number of articles on success
     */
    public ValueResult<BigInteger> getArticleCount(Connection connection);

    /**
     * Retrieves the number of pages in the given category from the DB.
     * 
     * @param connection
     *            the connection to the DB
     * @param title
     *            the title of the category
     * @param nsObject
     *            the namespace for page title normalisation
     * 
     * @return a result object with the number of pages on success
     */
    public ValueResult<BigInteger> getPagesInCategoryCount(Connection connection,
            String title, final MyNamespace nsObject);
    
    /**
     * Retrieves the number of available articles, i.e. pages in the main
     * namespace, from the DB.
     * 
     * @param connection
     *            the connection to the DB
     * 
     * @return a result object with the number of articles on success
     */
    public ValueResult<BigInteger> getStatsPageEdits(Connection connection);
    
    /**
     * Retrieves a random page title from the DB.
     * 
     * @param connection
     *            the connection to the DB
     * @param random
     *            the random number generator to use
     * 
     * @return a result object with the page list on success
     */
    public ValueResult<NormalisedTitle> getRandomArticle(Connection connection, Random random);
    
    /**
     * Saves or edits a page with the given parameters
     * 
     * @param connection
     *            the connection to use
     * @param title
     *            the title of the page
     * @param newRev
     *            the new revision to add
     * @param prevRevId
     *            the version of the previously existing revision or <tt>-1</tt>
     *            if there was no previous revision or <tt>-2</tt> if the
     *            previous revision is unknown and should be determined during
     *            the save
     * @param restrictions
     *            new restrictions of the page or <tt>null</tt> if they should
     *            not be changed
     * @param siteinfo
     *            information about the wikipedia (used for parsing categories
     *            and templates)
     * @param username
     *            name of the user editing the page (for enforcing restrictions)
     * @param nsObject
     *            the namespace for page title normalisation
     * 
     * @return success status
     */
    public SavePageResult savePage(Connection connection, String title,
            Revision newRev, int prevRevId, Map<String, String> restrictions,
            SiteInfo siteinfo, String username, final MyNamespace nsObject);
}
