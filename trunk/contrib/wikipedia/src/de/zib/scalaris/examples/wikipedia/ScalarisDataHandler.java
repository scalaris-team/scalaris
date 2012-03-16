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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import de.zib.scalaris.AbortException;
import de.zib.scalaris.Connection;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.ErlangValue;
import de.zib.scalaris.NotANumberException;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.ScalarisVM;
import de.zib.scalaris.Transaction;
import de.zib.scalaris.Transaction.ResultList;
import de.zib.scalaris.TransactionSingleOp;
import de.zib.scalaris.UnknownException;
import de.zib.scalaris.examples.wikipedia.bliki.MyNamespace;
import de.zib.scalaris.examples.wikipedia.bliki.MyWikiModel;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.ShortRevision;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;

/**
 * Retrieves and writes values from/to Scalaris.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class ScalarisDataHandler {
    /**
     * Gets the key to store {@link SiteInfo} objects at.
     * 
     * @return Scalaris key
     */
    public final static String getSiteInfoKey() {
        return "siteinfo";
    }
    
    /**
     * Gets the key to store the (complete) list of pages at.
     * 
     * @return Scalaris key
     */
    public final static String getPageListKey() {
        return "pages";
    }
    
    /**
     * Gets the key to store the number of pages at.
     * 
     * @return Scalaris key
     */
    public final static String getPageCountKey() {
        return "pages:count";
    }
    
    /**
     * Gets the key to store the (complete) list of articles, i.e. pages in
     * the main namespace) at.
     * 
     * @return Scalaris key
     */
    public final static String getArticleListKey() {
        return "articles";
    }
    
    /**
     * Gets the key to store the number of articles, i.e. pages in the main
     * namespace, at.
     * 
     * @return Scalaris key
     */
    public final static String getArticleCountKey() {
        return "articles:count";
    }
    
    /**
     * Gets the key to store {@link Revision} objects at.
     * 
     * @param title     the title of the page
     * @param id        the id of the revision
     * @param nsObject  the namespace for page title normalisation
     * 
     * @return Scalaris key
     */
    public final static String getRevKey(String title, int id, final MyNamespace nsObject) {
        return MyWikiModel.normalisePageTitle(title, nsObject) + ":rev:" + id;
    }
    
    /**
     * Gets the key to store {@link Page} objects at.
     * 
     * @param title     the title of the page
     * @param nsObject  the namespace for page title normalisation
     * 
     * @return Scalaris key
     */
    public final static String getPageKey(String title, final MyNamespace nsObject) {
        return MyWikiModel.normalisePageTitle(title, nsObject) + ":page";
    }
    
    /**
     * Gets the key to store the list of revisions of a page at.
     * 
     * @param title     the title of the page
     * @param nsObject  the namespace for page title normalisation
     * 
     * @return Scalaris key
     */
    public final static String getRevListKey(String title, final MyNamespace nsObject) {
        return MyWikiModel.normalisePageTitle(title, nsObject) + ":revs";
    }
    
    /**
     * Gets the key to store the list of pages belonging to a category at.
     * 
     * @param title     the category title (including <tt>Category:</tt>)
     * @param nsObject  the namespace for page title normalisation
     * 
     * @return Scalaris key
     */
    public final static String getCatPageListKey(String title, final MyNamespace nsObject) {
        return MyWikiModel.normalisePageTitle(title, nsObject) + ":cpages";
    }
    
    /**
     * Gets the key to store the number of pages belonging to a category at.
     * 
     * @param title     the category title (including <tt>Category:</tt>)
     * @param nsObject  the namespace for page title normalisation
     * 
     * @return Scalaris key
     */
    public final static String getCatPageCountKey(String title, final MyNamespace nsObject) {
        return MyWikiModel.normalisePageTitle(title, nsObject) + ":cpages:count";
    }
    
    /**
     * Gets the key to store the list of pages using a template at.
     * 
     * @param title     the template title (including <tt>Template:</tt>)
     * @param nsObject  the namespace for page title normalisation
     * 
     * @return Scalaris key
     */
    public final static String getTplPageListKey(String title, final MyNamespace nsObject) {
        return MyWikiModel.normalisePageTitle(title, nsObject) + ":tpages";
    }
    
    /**
     * Gets the key to store the list of pages linking to the given title.
     * 
     * @param title     the page's title
     * @param nsObject  the namespace for page title normalisation
     * 
     * @return Scalaris key
     */
    public final static String getBackLinksPageListKey(String title, final MyNamespace nsObject) {
        return MyWikiModel.normalisePageTitle(title, nsObject) + ":blpages";
    }
    
    /**
     * Gets the key to store the number of page edits.
     * 
     * @return Scalaris key
     */
    public final static String getStatsPageEditsKey() {
        return "stats:pageedits";
    }

    
    /**
     * Retrieves the Scalaris version string.
     * 
     * @param connection
     *            the connection to the DB
     * 
     * @return a result object with the version string on success
     */
    public static ValueResult<String> getDbVersion(Connection connection) {
        final long timeAtStart = System.currentTimeMillis();
        final String statName = "Scalaris version";
        List<String> involvedKeys = new ArrayList<String>();
        if (connection == null) {
            return new ValueResult<String>(false, involvedKeys,
                    "no connection to Scalaris", true, statName,
                    System.currentTimeMillis() - timeAtStart);
        }

        String node = connection.getRemote().toString();
        try {
            ScalarisVM scalarisVm = new ScalarisVM(node);
            String version = scalarisVm.getVersion();
            return new ValueResult<String>(involvedKeys, version, statName,
                    System.currentTimeMillis() - timeAtStart);
        } catch (ConnectionException e) {
            return new ValueResult<String>(false, involvedKeys,
                    "no connection to Scalaris node \"" + node + "\"", true,
                    statName, System.currentTimeMillis() - timeAtStart);
        } catch (UnknownException e) {
            return new ValueResult<String>(false, involvedKeys,
                    "unknown exception reading Scalaris version from node \""
                            + node + "\"", true, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
    }
    
    /**
     * Retrieves a page's history from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param title
     *            the title of the page
     * @param nsObject
     *            the namespace for page title normalisation
     * 
     * @return a result object with the page history on success
     */
    public static PageHistoryResult getPageHistory(Connection connection,
            String title, final MyNamespace nsObject) {
        final long timeAtStart = System.currentTimeMillis();
        final String statName = "history of " + title;
        List<String> involvedKeys = new ArrayList<String>();
        if (connection == null) {
            return new PageHistoryResult(false, involvedKeys, "no connection to Scalaris",
                    true, statName, System.currentTimeMillis() - timeAtStart);
        }
        
        TransactionSingleOp scalaris_single = new TransactionSingleOp(connection);
        TransactionSingleOp.RequestList requests = new TransactionSingleOp.RequestList();
        requests.addRead(getPageKey(title, nsObject)).addRead(getRevListKey(title, nsObject));
        
        TransactionSingleOp.ResultList results;
        try {
            involvedKeys.addAll(requests.keyList());
            results = scalaris_single.req_list(requests);
        } catch (Exception e) {
            return new PageHistoryResult(false, involvedKeys,
                    "unknown exception reading \""
                            + getPageKey(title, nsObject) + "\" or \""
                            + getRevListKey(title, nsObject)
                            + "\" from Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        
        Page page;
        try {
            page = results.processReadAt(0).jsonValue(Page.class);
        } catch (NotFoundException e) {
            PageHistoryResult result = new PageHistoryResult(
                    false, involvedKeys,
                    "page not found at \"" + getPageKey(title, nsObject) + "\"",
                    false, statName, System.currentTimeMillis() - timeAtStart);
            result.not_existing = true;
            return result;
        } catch (Exception e) {
            return new PageHistoryResult(false, involvedKeys,
                    "unknown exception reading \""
                            + getPageKey(title, nsObject)
                            + "\" from Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, statName,
                    System.currentTimeMillis() - timeAtStart);
        }

        List<ShortRevision> revisions;
        try {
            revisions = results.processReadAt(1).jsonListValue(ShortRevision.class);
        } catch (NotFoundException e) {
            PageHistoryResult result = new PageHistoryResult(false,
                    involvedKeys, "revision list \""
                            + getRevListKey(title, nsObject)
                            + "\" does not exist", false, statName,
                    System.currentTimeMillis() - timeAtStart);
            result.not_existing = true;
            return result;
        } catch (Exception e) {
            return new PageHistoryResult(false, involvedKeys,
                    "unknown exception reading \""
                            + getRevListKey(title, nsObject)
                            + "\" from Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        return new PageHistoryResult(involvedKeys, page, revisions, statName,
                System.currentTimeMillis() - timeAtStart);
    }

    /**
     * Retrieves the current, i.e. most up-to-date, version of a page from
     * Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param title
     *            the title of the page
     * @param nsObject
     *            the namespace for page title normalisation
     * 
     * @return a result object with the page and revision on success
     */
    public static RevisionResult getRevision(Connection connection,
            String title, final MyNamespace nsObject) {
        return getRevision(connection, title, -1, nsObject);
    }

    /**
     * Retrieves the given version of a page from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param title
     *            the title of the page
     * @param id
     *            the id of the version
     * @param nsObject
     *            the namespace for page title normalisation
     * 
     * @return a result object with the page and revision on success
     */
    public static RevisionResult getRevision(Connection connection,
            String title, int id, final MyNamespace nsObject) {
        final long timeAtStart = System.currentTimeMillis();
        Page page = null;
        Revision revision = null;
        List<String> involvedKeys = new ArrayList<String>();
        if (connection == null) {
            return new RevisionResult(false, involvedKeys,
                    "no connection to Scalaris", true, page, revision, false,
                    false, title, System.currentTimeMillis() - timeAtStart);
        }
        
        TransactionSingleOp scalaris_single;
        String scalaris_key;
        
        scalaris_single = new TransactionSingleOp(connection);

        scalaris_key = getPageKey(title, nsObject);
        try {
            involvedKeys.add(scalaris_key);
            page = scalaris_single.read(scalaris_key).jsonValue(Page.class);
        } catch (NotFoundException e) {
            return new RevisionResult(false, involvedKeys,
                    "page not found at \"" + scalaris_key + "\"", false, page,
                    revision, true, false, title,
                    System.currentTimeMillis() - timeAtStart);
        } catch (Exception e) {
            return new RevisionResult(false, involvedKeys,
                    "unknown exception reading \"" + scalaris_key
                            + "\" from Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, page, revision, false,
                    false, title, System.currentTimeMillis() - timeAtStart);
        }

        // load requested version if it is not the current one cached in the Page object
        if (id != page.getCurRev().getId() && id >= 0) {
            scalaris_key = getRevKey(title, id, nsObject);
            try {
                involvedKeys.add(scalaris_key);
                revision = scalaris_single.read(scalaris_key).jsonValue(Revision.class);
            } catch (NotFoundException e) {
                return new RevisionResult(false, involvedKeys,
                        "revision not found at \"" + scalaris_key + "\"",
                        false, page, revision, false, true, title,
                        System.currentTimeMillis() - timeAtStart);
            } catch (Exception e) {
                return new RevisionResult(false, involvedKeys,
                        "unknown exception reading \"" + scalaris_key
                                + "\" from Scalaris: " + e.getMessage(),
                        e instanceof ConnectionException, page, revision,
                        false, false, title,
                        System.currentTimeMillis() - timeAtStart);
            }
        } else {
            revision = page.getCurRev();
        }

        return new RevisionResult(involvedKeys, page, revision, title,
                System.currentTimeMillis() - timeAtStart);
    }

    /**
     * Retrieves a list of available pages from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * 
     * @return a result object with the page list on success
     */
    public static ValueResult<List<String>> getPageList(Connection connection) {
        return getPageList2(connection, getPageListKey(), false, "page list");
    }

    /**
     * Retrieves a list of available articles, i.e. pages in the main
     * namespace, from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * 
     * @return a result object with the page list on success
     */
    public static ValueResult<List<String>> getArticleList(Connection connection) {
        return getPageList2(connection, getArticleListKey(), false, "article list");
    }

    /**
     * Retrieves a list of pages in the given category from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param title
     *            the title of the category
     * @param nsObject
     *            the namespace for page title normalisation
     * 
     * @return a result object with the page list on success
     */
    public static ValueResult<List<String>> getPagesInCategory(Connection connection,
            String title, final MyNamespace nsObject) {
        return getPageList2(connection, getCatPageListKey(title, nsObject),
                false, "pages in " + title);
    }

    /**
     * Retrieves a list of pages using the given template from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param title
     *            the title of the template
     * @param nsObject
     *            the namespace for page title normalisation
     * 
     * @return a result object with the page list on success
     */
    public static ValueResult<List<String>> getPagesInTemplate(Connection connection,
            String title, final MyNamespace nsObject) {
        return getPageList2(connection, getTplPageListKey(title, nsObject),
                true, "pages in " + title);
    }

    /**
     * Retrieves a list of pages linking to the given page from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param title
     *            the title of the page
     * @param nsObject
     *            the namespace for page title normalisation
     * 
     * @return a result object with the page list on success
     */
    public static ValueResult<List<String>> getPagesLinkingTo(Connection connection,
            String title, final MyNamespace nsObject) {
        final String statName = "links to " + title;
        if (Options.WIKI_USE_BACKLINKS) {
            return getPageList2(connection,
                    getBackLinksPageListKey(title, nsObject), false, statName);
        } else {
            return new ValueResult<List<String>>(new ArrayList<String>(0),
                    new ArrayList<String>(0));
        }
    }

    /**
     * Retrieves a list of pages from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param scalaris_key
     *            the key under which the page list is stored in Scalaris
     * @param failNotFound
     *            whether the operation should fail if the key is not found or
     *            not
     * @param statName
     *            name for the time measurement statistics
     * 
     * @return a result object with the page list on success
     */
    private static ValueResult<List<String>> getPageList2(Connection connection,
            String scalaris_key, boolean failNotFound, String statName) {
        final long timeAtStart = System.currentTimeMillis();
        List<String> involvedKeys = new ArrayList<String>();
        if (connection == null) {
            return new ValueResult<List<String>>(false, involvedKeys,
                    "no connection to Scalaris", true, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        
        TransactionSingleOp scalaris_single = new TransactionSingleOp(connection);
        try {
            involvedKeys.add(scalaris_key);
            List<String> pages = scalaris_single.read(scalaris_key).stringListValue();
            return new ValueResult<List<String>>(involvedKeys, pages, statName,
                    System.currentTimeMillis() - timeAtStart);
        } catch (NotFoundException e) {
            if (failNotFound) {
                return new ValueResult<List<String>>(false, involvedKeys,
                        "unknown exception reading page list at \""
                                + scalaris_key + "\" from Scalaris: "
                                + e.getMessage(), false, statName,
                        System.currentTimeMillis() - timeAtStart);
            } else {
                return new ValueResult<List<String>>(involvedKeys,
                        new ArrayList<String>(0), statName,
                        System.currentTimeMillis() - timeAtStart);
            }
        } catch (Exception e) {
            return new ValueResult<List<String>>(false, involvedKeys,
                    "unknown exception reading page list at \"" + scalaris_key
                            + "\" from Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
    }

    /**
     * Retrieves the number of available pages from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * 
     * @return a result object with the number of pages on success
     */
    public static ValueResult<BigInteger> getPageCount(Connection connection) {
        return getInteger2(connection, getPageCountKey(), false, "page count");
    }

    /**
     * Retrieves the number of available articles, i.e. pages in the main
     * namespace, from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * 
     * @return a result object with the number of articles on success
     */
    public static ValueResult<BigInteger> getArticleCount(Connection connection) {
        return getInteger2(connection, getArticleCountKey(), false, "article count");
    }

    /**
     * Retrieves the number of pages in the given category from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param title
     *            the title of the category
     * @param nsObject
     *            the namespace for page title normalisation
     * 
     * @return a result object with the number of pages on success
     */
    public static ValueResult<BigInteger> getPagesInCategoryCount(
            Connection connection, String title, final MyNamespace nsObject) {
        return getInteger2(connection, getCatPageCountKey(title, nsObject),
                false, "page count in " + title);
    }

    /**
     * Retrieves the number of available articles, i.e. pages in the main
     * namespace, from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * 
     * @return a result object with the number of articles on success
     */
    public static ValueResult<BigInteger> getStatsPageEdits(Connection connection) {
        return getInteger2(connection, getStatsPageEditsKey(), false, "page edits");
    }

    /**
     * Retrieves an integral number from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param scalaris_key
     *            the key under which the number is stored in Scalaris
     * @param failNotFound
     *            whether the operation should fail if the key is not found or
     *            not
     * @param statName
     *            name for the time measurement statistics
     * 
     * @return a result object with the number on success
     */
    private static ValueResult<BigInteger> getInteger2(Connection connection,
            String scalaris_key, boolean failNotFound, String statName) {
        final long timeAtStart = System.currentTimeMillis();
        List<String> involvedKeys = new ArrayList<String>();
        if (connection == null) {
            return new ValueResult<BigInteger>(false, involvedKeys,
                    "no connection to Scalaris", true, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        
        TransactionSingleOp scalaris_single = new TransactionSingleOp(connection);
        try {
            involvedKeys.add(scalaris_key);
            BigInteger number = scalaris_single.read(scalaris_key).bigIntValue();
            return new ValueResult<BigInteger>(involvedKeys, number, statName,
                    System.currentTimeMillis() - timeAtStart);
        } catch (NotFoundException e) {
            if (failNotFound) {
                return new ValueResult<BigInteger>(false, involvedKeys,
                        "unknown exception reading (integral) number at \""
                                + scalaris_key + "\" from Scalaris: "
                                + e.getMessage(), false, statName,
                        System.currentTimeMillis() - timeAtStart);
            } else {
                return new ValueResult<BigInteger>(involvedKeys,
                        BigInteger.valueOf(0), statName,
                        System.currentTimeMillis() - timeAtStart);
            }
        } catch (Exception e) {
            return new ValueResult<BigInteger>(false, involvedKeys,
                    "unknown exception reading (integral) number at \""
                            + scalaris_key + "\" from Scalaris: "
                            + e.getMessage(), e instanceof ConnectionException,
                    statName, System.currentTimeMillis() - timeAtStart);
        }
    }

    /**
     * Retrieves a random page title from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param random
     *            the random number generator to use
     * 
     * @return a result object with the page list on success
     */
    public static ValueResult<String> getRandomArticle(Connection connection, Random random) {
        final long timeAtStart = System.currentTimeMillis();
        final String statName = "random article";
        List<String> involvedKeys = new ArrayList<String>();
        if (connection == null) {
            return new ValueResult<String>(false, involvedKeys,
                    "no connection to Scalaris", true, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        
        TransactionSingleOp scalaris_single = new TransactionSingleOp(connection);
        try {
            final String scalaris_key = getArticleListKey();
            involvedKeys.add(scalaris_key);
            List<ErlangValue> pages = scalaris_single.read(scalaris_key).listValue();
            String randomTitle = pages.get(random.nextInt(pages.size())).stringValue();
            return new ValueResult<String>(involvedKeys, randomTitle, statName,
                    System.currentTimeMillis() - timeAtStart);
        } catch (Exception e) {
            return new ValueResult<String>(false, involvedKeys,
                    "unknown exception reading page list at \"pages\" from Scalaris: "
                            + e.getMessage(), e instanceof ConnectionException,
                    statName, System.currentTimeMillis() - timeAtStart);
        }
    }

    /**
     * Saves or edits a page with the given parameters
     * 
     * @param connection
     *            the connection to use
     * @param title0
     *            the (unnormalised) title of the page
     * @param newRev
     *            the new revision to add
     * @param prevRevId
     *            the version of the previously existing revision or <tt>-1</tt>
     *            if there was no previous revision
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
    public static SavePageResult savePage(final Connection connection, final String title0,
            final Revision newRev, final int prevRevId, final Map<String, String> restrictions,
            final SiteInfo siteinfo, final String username, final MyNamespace nsObject) {
        long timeAtStart = System.currentTimeMillis();
        final String statName = "saving " + title0;
        Page oldPage = null;
        Page newPage = null;
        List<ShortRevision> newShortRevs = null;
        BigInteger pageEdits = null;
        List<String> involvedKeys = new ArrayList<String>();
        if (connection == null) {
            return new SavePageResult(false, involvedKeys,
                    "no connection to Scalaris", true, oldPage, newPage,
                    newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        
        String title = MyWikiModel.normalisePageTitle(title0, nsObject);
        Transaction scalaris_tx = new Transaction(connection);

        // check that the current version is still up-to-date:
        // read old version first, then write
        int oldRevId = -1;
        String pageInfoKey = getPageKey(title0, nsObject);
        
        Transaction.RequestList requests = new Transaction.RequestList();
        requests.addRead(pageInfoKey);
        
        Transaction.ResultList results;
        try {
            involvedKeys.addAll(requests.keyList());
            results = scalaris_tx.req_list(requests);
        } catch (Exception e) {
            return new SavePageResult(false, involvedKeys,
                    "unknown exception getting page info (" + pageInfoKey
                            + ") from Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, oldPage, newPage,
                    newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        
        try {
            oldPage = results.processReadAt(0).jsonValue(Page.class);
            newPage = new Page(oldPage.getTitle(),
                    oldPage.getId(), oldPage.isRedirect(),
                    new LinkedHashMap<String, String>(
                            oldPage.getRestrictions()), newRev);
            oldRevId = oldPage.getCurRev().getId();
        } catch (NotFoundException e) {
            // this is ok and means that the page did not exist yet
            newPage = new Page();
            newPage.setTitle(title0);
            newPage.setCurRev(newRev);
        } catch (Exception e) {
            return new SavePageResult(false, involvedKeys,
                    "unknown exception reading \"" + pageInfoKey
                            + "\" from Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, oldPage, newPage,
                    newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        
        if (!newPage.checkEditAllowed(username)) {
            return new SavePageResult(false, involvedKeys,
                    "operation not allowed: edit is restricted", false,
                    oldPage, newPage, newShortRevs, pageEdits,
                    statName, System.currentTimeMillis() - timeAtStart);
        }
        
        if (prevRevId != oldRevId) {
            return new SavePageResult(false, involvedKeys, "curRev(" + oldRevId
                    + ") != oldRev(" + prevRevId + ")", false, oldPage,
                    newPage, newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }

        // write:
        // get previous categories, templates and backlinks:
        final MyWikiModel wikiModel = new MyWikiModel("", "", new MyNamespace(siteinfo));
        wikiModel.setPageName(title0);
        Set<String> oldCats;
        Set<String> oldTpls;
        Set<String> oldLnks;
        if (oldRevId != -1 && oldPage != null && oldPage.getCurRev() != null) {
            // get a list of previous categories and templates:
            wikiModel.setUp();
            final long timeAtRenderStart = System.currentTimeMillis();
            wikiModel.render(null, oldPage.getCurRev().unpackedText());
            timeAtStart -= (System.currentTimeMillis() - timeAtRenderStart);
            // note: no need to normalise the pages, we will do so during the write/read key generation
            oldCats = wikiModel.getCategories().keySet();
            oldTpls = wikiModel.getTemplates();
            if (Options.WIKI_USE_BACKLINKS) {
                oldLnks = wikiModel.getLinks();
            } else {
                // use empty link lists to turn back-links off
                oldLnks = new HashSet<String>();
            }
            wikiModel.tearDown();
        } else {
            oldCats = new HashSet<String>();
            oldTpls = new HashSet<String>();
            oldLnks = new HashSet<String>();
        }
        // get new categories and templates
        wikiModel.setUp();
        do {
            final long timeAtRenderStart = System.currentTimeMillis();
            wikiModel.render(null, newRev.unpackedText());
            timeAtStart -= (System.currentTimeMillis() - timeAtRenderStart);
        } while (false);
        if (wikiModel.getRedirectLink() != null) {
            newPage.setRedirect(true);
        }
        if (restrictions != null) {
            newPage.setRestrictions(restrictions);
        }
        
        // note: do not tear down the wiki model - the following statements
        // still need it and it will be removed at the end of the method anyway
        // note: no need to normalise the pages, we will do so during the write/read key generation
        final Set<String> newCats = wikiModel.getCategories().keySet();
        Difference catDiff = new Difference(oldCats, newCats,
                new Difference.GetPageListAndCountKey() {
                    @Override
                    public String getPageListKey(String name) {
                        return getCatPageListKey(
                                wikiModel.getCategoryNamespace() + ":" + name,
                                nsObject);
                    }

                    @Override
                    public String getPageCountKey(String name) {
                        return getCatPageCountKey(
                                wikiModel.getCategoryNamespace() + ":" + name,
                                nsObject);
                    }
                });
        final Set<String> newTpls = wikiModel.getTemplates();
        Difference tplDiff = new Difference(oldTpls, newTpls,
                new Difference.GetPageListKey() {
                    @Override
                    public String getPageListKey(String name) {
                        return getTplPageListKey(
                                wikiModel.getTemplateNamespace() + ":" + name,
                                nsObject);
                    }
                });
        // use empty link lists to turn back-links off
        final Set<String> newLnks = Options.WIKI_USE_BACKLINKS ? wikiModel.getLinks() : new HashSet<String>();
        Difference lnkDiff = new Difference(oldLnks, newLnks,
                new Difference.GetPageListKey() {
                    @Override
                    public String getPageListKey(String name) {
                        return getBackLinksPageListKey(name, nsObject);
                    }
                });

        // write differences (categories, templates, backlinks)
        // new page? -> add to page/article lists
        SavePageResult result;
        if (Options.WIKI_USE_NEW_SCALARIS_OPS) {
            result = savePage2NewOps(title0, newRev, nsObject, timeAtStart,
                    oldPage, newPage, newShortRevs, pageEdits, involvedKeys, title,
                    scalaris_tx, oldRevId, wikiModel, catDiff, tplDiff,
                    lnkDiff, statName);
        } else {
            result = savePage2(title0, newRev, nsObject, timeAtStart, oldPage,
                    newPage, newShortRevs, pageEdits, involvedKeys, title, scalaris_tx,
                    oldRevId, wikiModel, catDiff, tplDiff, lnkDiff, statName);
        }
        if (result != null) {
            return result;
        }

        if (Options.WIKI_USE_NEW_SCALARIS_OPS) {
            pageEdits = increasePageEditStat2NewOps(scalaris_tx, involvedKeys);
        } else {
            pageEdits = increasePageEditStat2(scalaris_tx, involvedKeys);
        }
        
        return new SavePageResult(involvedKeys, oldPage, newPage, newShortRevs,
                pageEdits, statName, System.currentTimeMillis() - timeAtStart);
    }

    /**
     * Applies the final steps for a save-page operation using ordinary
     * read/write operations. This includes updating the various page lists.
     * 
     * @param title0
     * @param newRev
     * @param prevRevId
     * @param nsObject
     * @param timeAtStart
     * @param oldPage
     * @param newPage
     * @param newShortRevs
     * @param pageEdits
     * @param involvedKeys
     *            all keys that have been read or written during the operation
     * @param title
     * @param scalaris_tx
     * @param oldRevId
     * @param wikiModel
     * @param catDiff
     * @param tplDiff
     * @param lnkDiff
     * @param statName
     *            name for the time measurement statistics
     */
    private static SavePageResult savePage2(final String title0,
            final Revision newRev, final MyNamespace nsObject,
            long timeAtStart, Page oldPage, Page newPage,
            List<ShortRevision> newShortRevs, BigInteger pageEdits,
            List<String> involvedKeys, String title, Transaction scalaris_tx,
            int oldRevId, final MyWikiModel wikiModel, Difference catDiff,
            Difference tplDiff, Difference lnkDiff, final String statName) {
        Transaction.RequestList requests;
        Transaction.ResultList results;
        final List<Integer> pageListWrites = new ArrayList<Integer>(2);
        final List<String> pageListKeys = new LinkedList<String>();
        if (oldRevId == -1) {
            pageListKeys.add(getPageListKey());
            pageListKeys.add(getPageCountKey());
            if (wikiModel.getNamespace(title0).isEmpty()) {
                pageListKeys.add(getArticleListKey());
                pageListKeys.add(getArticleCountKey());
            }
        }
        //  PAGE LISTS UPDATE, step 1: read old lists
        requests = new Transaction.RequestList();
        String revListKey = getRevListKey(title0, nsObject);
        int curOp;
        requests.addRead(revListKey);
        final int catPageReads = catDiff.updatePageLists_prepare_read(requests, title);
        final int tplPageReads = tplDiff.updatePageLists_prepare_read(requests, title);
        final int lnkPageReads = lnkDiff.updatePageLists_prepare_read(requests, title);
        for (Iterator<String> it = pageListKeys.iterator(); it.hasNext();) {
            String pageList_key = (String) it.next();
            // note: do not need to retrieve page list count (we can calculate it on our own)
            @SuppressWarnings("unused")
            String pageCount_key = (String) it.next();
            updatePageList_prepare_read(requests, pageList_key);
        }
        results = null;
        try {
            involvedKeys.addAll(requests.keyList());
            results = scalaris_tx.req_list(requests);
            curOp = 0;
        } catch (Exception e) {
            return new SavePageResult(false, involvedKeys,
                    "unknown exception reading page lists for page \"" + title0
                            + "\" from Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, oldPage, newPage,
                    newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        //  PAGE LISTS UPDATE, step 2: prepare writes of new lists
        requests = new Transaction.RequestList();
        try {
            newShortRevs = results.processReadAt(curOp++).jsonListValue(ShortRevision.class);
        } catch (NotFoundException e) {
            if (oldRevId == -1) { // new page?
                newShortRevs = new LinkedList<ShortRevision>();
            } else {
//              e.printStackTrace();
                // corrupt DB - don't save page
                return new SavePageResult(false, involvedKeys,
                        "revision list for page \"" + title0
                                + "\" not found at \"" + revListKey + "\"",
                        false, oldPage, newPage, newShortRevs, pageEdits,
                        statName, System.currentTimeMillis() - timeAtStart);
            }
        } catch (Exception e) {
            return new SavePageResult(false, involvedKeys,
                    "unknown exception reading \"" + revListKey
                            + "\" from Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, oldPage, newPage,
                    newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        newShortRevs.add(0, new ShortRevision(newRev));
        requests.addWrite(revListKey, newShortRevs);
        ValueResult<Integer> pageListResult;
        pageListResult = catDiff.updatePageLists_prepare_write(results,
                requests, title, curOp, statName, involvedKeys);
        curOp += catPageReads;
        if (!pageListResult.success) {
            return new SavePageResult(false, involvedKeys,
                    pageListResult.message, pageListResult.connect_failed,
                    oldPage, newPage, newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        final int catPageWrites = pageListResult.value;
        pageListResult = tplDiff.updatePageLists_prepare_write(results,
                requests, title, curOp, statName, involvedKeys);
        curOp += tplPageReads;
        if (!pageListResult.success) {
            return new SavePageResult(false, involvedKeys,
                    pageListResult.message, pageListResult.connect_failed,
                    oldPage, newPage, newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        final int tplPageWrites = pageListResult.value;
        pageListResult = lnkDiff.updatePageLists_prepare_write(results,
                requests, title, curOp, statName, involvedKeys);
        curOp += lnkPageReads;
        if (!pageListResult.success) {
            return new SavePageResult(false, involvedKeys,
                    pageListResult.message, pageListResult.connect_failed,
                    oldPage, newPage, newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        final int lnkPageWrites = pageListResult.value;
        final List<String> newPages = Arrays.asList(title);
        for (Iterator<String> it = pageListKeys.iterator(); it.hasNext();) {
            String pageList_key = (String) it.next();
            String pageCount_key = (String) it.next();
            
            pageListResult = updatePageList_prepare_write(results, requests,
                    pageList_key, pageCount_key, new LinkedList<String>(),
                    newPages, curOp, statName, involvedKeys);
            if (!pageListResult.success) {
                return new SavePageResult(false, involvedKeys,
                        pageListResult.message, pageListResult.connect_failed,
                        oldPage, newPage, newShortRevs, pageEdits, statName,
                        System.currentTimeMillis() - timeAtStart);
            }
            ++curOp; // processed one read op during each call of updatePageList_prepare_write
            pageListWrites.add(pageListResult.value);
        }
        // smuggle in the final write requests to save a round-trip to Scalaris:
        requests.addWrite(getPageKey(title0, nsObject), newPage)
                .addWrite(getRevKey(title0, newRev.getId(), nsObject), newRev)
                .addCommit();
        //  PAGE LISTS UPDATE, step 3: execute and evaluate writes of new lists
        results = null;
        try {
            involvedKeys.addAll(requests.keyList());
            results = scalaris_tx.req_list(requests);
            curOp = 0;
        } catch (Exception e) {
            final SavePageResult result = new SavePageResult(false, involvedKeys,
                    "unknown exception writing page or page lists for page \""
                            + title0 + "\" to Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, oldPage, newPage,
                    newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
            if (e instanceof AbortException) {
                AbortException e1 = (AbortException) e;
                result.failedKeys.addAll(e1.getFailedKeys());
            }
            return result;
        }

        try {
            results.processWriteAt(curOp++);
        } catch (Exception e) {
            return new SavePageResult(false, involvedKeys,
                    "unknown exception writing page \"" + title0
                    + "\" to Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, oldPage, newPage,
                    newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        pageListResult = catDiff.updatePageLists_check_writes(results, title, curOp, statName, involvedKeys);
        curOp += catPageWrites;
        if (!pageListResult.success) {
            return new SavePageResult(false, involvedKeys,
                    pageListResult.message, pageListResult.connect_failed,
                    oldPage, newPage, newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        pageListResult = tplDiff.updatePageLists_check_writes(results, title, curOp, statName, involvedKeys);
        curOp += tplPageWrites;
        if (!pageListResult.success) {
            return new SavePageResult(false, involvedKeys,
                    pageListResult.message, pageListResult.connect_failed,
                    oldPage, newPage, newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        pageListResult = lnkDiff.updatePageLists_check_writes(results, title, curOp, statName, involvedKeys);
        curOp += lnkPageWrites;
        if (!pageListResult.success) {
            return new SavePageResult(false, involvedKeys,
                    pageListResult.message, pageListResult.connect_failed,
                    oldPage, newPage, newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        int pageList = 0;
        for (Iterator<String> it = pageListKeys.iterator(); it.hasNext();) {
            String pageList_key = (String) it.next();
            String pageCount_key = (String) it.next();

            final Integer writeOps = pageListWrites.get(pageList);
            pageListResult = updatePageList_check_writes(results, pageList_key,
                    pageCount_key, curOp, writeOps, statName, involvedKeys);
            if (!pageListResult.success) {
                return new SavePageResult(false, involvedKeys,
                        pageListResult.message, pageListResult.connect_failed,
                        oldPage, newPage, newShortRevs, pageEdits, statName,
                        System.currentTimeMillis() - timeAtStart);
            }
            curOp += writeOps;
            ++pageList;
        }

        try {
            results.processWriteAt(curOp++);
            results.processWriteAt(curOp++);
        } catch (Exception e) {
            return new SavePageResult(false, involvedKeys,
                    "unknown exception writing page \"" + title0
                    + "\" to Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, oldPage, newPage,
                    newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        return null;
    }

    /**
     * Applies the final steps for a save-page operation using the new
     * {@link Transaction#addOnNr(String, Object)} and
     * {@link Transaction#addDelOnList(String, List, List)} operations. This
     * includes updating the various page lists. .
     * 
     * @param title0
     * @param newRev
     * @param prevRevId
     * @param nsObject
     * @param timeAtStart
     * @param oldPage
     * @param newPage
     * @param newShortRevs
     * @param pageEdits
     * @param involvedKeys
     *            all keys that have been read or written during the operation
     * @param title
     * @param scalaris_tx
     * @param oldRevId
     * @param wikiModel
     * @param catDiff
     * @param tplDiff
     * @param lnkDiff
     * @param statName
     *            name for the time measurement statistics
     */
    private static SavePageResult savePage2NewOps(final String title0,
            final Revision newRev, final MyNamespace nsObject,
            long timeAtStart, Page oldPage, Page newPage,
            List<ShortRevision> newShortRevs, BigInteger pageEdits,
            List<String> involvedKeys, String title, Transaction scalaris_tx,
            int oldRevId, final MyWikiModel wikiModel, Difference catDiff,
            Difference tplDiff, Difference lnkDiff, final String statName) {
        Transaction.RequestList requests;
        Transaction.ResultList results;
        final List<Integer> pageListWrites = new ArrayList<Integer>(2);
        final List<String> pageListKeys = new LinkedList<String>();
        if (oldRevId == -1) {
            pageListKeys.add(getPageListKey());
            pageListKeys.add(getPageCountKey());
            if (wikiModel.getNamespace(title0).isEmpty()) {
                pageListKeys.add(getArticleListKey());
                pageListKeys.add(getArticleCountKey());
            }
        }
        //  PAGE LISTS UPDATE, step 1: append to / remove from old lists
        requests = new Transaction.RequestList();
        String revListKey = getRevListKey(title0, nsObject);
        requests.addAddDelOnList(revListKey, Arrays.asList(new ShortRevision(newRev)), null);
        ValueResult<Integer> pageListResult;
        pageListResult = catDiff.updatePageLists_prepare_appends(requests, title, statName, involvedKeys);
        if (!pageListResult.success) {
            return new SavePageResult(false, involvedKeys,
                    pageListResult.message, pageListResult.connect_failed,
                    oldPage, newPage, newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        final int catPageWrites = pageListResult.value;
        pageListResult = tplDiff.updatePageLists_prepare_appends(requests, title, statName, involvedKeys);
        if (!pageListResult.success) {
            return new SavePageResult(false, involvedKeys,
                    pageListResult.message, pageListResult.connect_failed,
                    oldPage, newPage, newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        final int tplPageWrites = pageListResult.value;
        pageListResult = lnkDiff.updatePageLists_prepare_appends(requests, title, statName, involvedKeys);
        if (!pageListResult.success) {
            return new SavePageResult(false, involvedKeys,
                    pageListResult.message, pageListResult.connect_failed,
                    oldPage, newPage, newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        final int lnkPageWrites = pageListResult.value;
        final List<String> newPages = Arrays.asList(title);
        for (Iterator<String> it = pageListKeys.iterator(); it.hasNext();) {
            String pageList_key = (String) it.next();
            String pageCount_key = (String) it.next();
            
            pageListResult = updatePageList_prepare_appends(requests,
                    pageList_key, pageCount_key, new LinkedList<String>(),
                    newPages, statName, involvedKeys);
            if (!pageListResult.success) {
                return new SavePageResult(false, involvedKeys,
                        pageListResult.message, pageListResult.connect_failed,
                        oldPage, newPage, newShortRevs, pageEdits, statName,
                        System.currentTimeMillis() - timeAtStart);
            }
            pageListWrites.add(pageListResult.value);
        }
        // smuggle in the final write requests to save a round-trip to Scalaris:
        requests.addWrite(getPageKey(title0, nsObject), newPage)
                .addWrite(getRevKey(title0, newRev.getId(), nsObject), newRev)
                .addCommit();
        //  PAGE LISTS UPDATE, step 2: execute and evaluate operations
        int curOp;
        results = null;
        try {
            involvedKeys.addAll(requests.keyList());
            results = scalaris_tx.req_list(requests);
            curOp = 0;
        } catch (Exception e) {
            final SavePageResult result = new SavePageResult(false, involvedKeys,
                    "unknown exception writing page or page lists for page \""
                            + title0 + "\" to Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, oldPage, newPage,
                    newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
            if (e instanceof AbortException) {
                AbortException e1 = (AbortException) e;
                result.failedKeys.addAll(e1.getFailedKeys());
            }
            return result;
        }

        try {
            results.processAddDelOnListAt(curOp++);
        } catch (Exception e) {
            return new SavePageResult(false, involvedKeys,
                    "unknown exception writing page \"" + title0
                    + "\" to Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, oldPage, newPage,
                    newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        pageListResult = catDiff.updatePageLists_check_appends(results, title, curOp, statName, involvedKeys);
        curOp += catPageWrites;
        if (!pageListResult.success) {
            return new SavePageResult(false, involvedKeys,
                    pageListResult.message, pageListResult.connect_failed,
                    oldPage, newPage, newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        pageListResult = tplDiff.updatePageLists_check_appends(results, title, curOp, statName, involvedKeys);
        curOp += tplPageWrites;
        if (!pageListResult.success) {
            return new SavePageResult(false, involvedKeys,
                    pageListResult.message, pageListResult.connect_failed,
                    oldPage, newPage, newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        pageListResult = lnkDiff.updatePageLists_check_appends(results, title, curOp, statName, involvedKeys);
        curOp += lnkPageWrites;
        if (!pageListResult.success) {
            return new SavePageResult(false, involvedKeys,
                    pageListResult.message, pageListResult.connect_failed,
                    oldPage, newPage, newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        int pageList = 0;
        for (Iterator<String> it = pageListKeys.iterator(); it.hasNext();) {
            String pageList_key = (String) it.next();
            String pageCount_key = (String) it.next();

            final Integer writeOps = pageListWrites.get(pageList);
            pageListResult = updatePageList_check_appends(results, pageList_key,
                    pageCount_key, curOp, writeOps, statName, involvedKeys);
            if (!pageListResult.success) {
                return new SavePageResult(false, involvedKeys,
                        pageListResult.message, pageListResult.connect_failed,
                        oldPage, newPage, newShortRevs, pageEdits, statName,
                        System.currentTimeMillis() - timeAtStart);
            }
            curOp += writeOps;
            ++pageList;
        }

        try {
            results.processWriteAt(curOp++);
            results.processWriteAt(curOp++);
        } catch (Exception e) {
            return new SavePageResult(false, involvedKeys,
                    "unknown exception writing page \"" + title0
                    + "\" to Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, oldPage, newPage,
                    newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        return null;
    }

    /**
     * Increases the number of overall page edits statistic using ordinary
     * read/write operations.
     * 
     * @param scalaris_tx
     *            the transaction object to use
     * @param involvedKeys
     *            all keys that have been read or written during the operation
     * 
     * @return the new number of page edits
     */
    private static BigInteger increasePageEditStat2(Transaction scalaris_tx,
            List<String> involvedKeys) {
        BigInteger pageEdits = null;
        // increase number of page edits (for statistics)
        // as this is not that important, use a separate transaction and do not
        // fail if updating the value fails
        try {
            String scalaris_key = getStatsPageEditsKey();
            try {
                involvedKeys.add(scalaris_key);
                pageEdits = scalaris_tx.read(scalaris_key).bigIntValue();
            } catch (NotFoundException e) {
                pageEdits = BigInteger.valueOf(0);
            }
            pageEdits = pageEdits.add(BigInteger.valueOf(1));
            Transaction.RequestList requests = new Transaction.RequestList();
            requests.addWrite(scalaris_key, pageEdits).addCommit();
            involvedKeys.addAll(requests.keyList());
            scalaris_tx.req_list(requests);
        } catch (Exception e) {
        }
        return pageEdits;
    }

    /**
     * Increases the number of overall page edits statistic using the new
     * {@link Transaction#addOnNr(String, Object)} operation.
     * 
     * @param scalaris_tx
     *            the transaction object to use
     * @param involvedKeys
     *            all keys that have been read or written during the operation
     * 
     * @return <tt>null</tt> (we do not retrieve the actual number of edits)
     */
    private static BigInteger increasePageEditStat2NewOps(
            Transaction scalaris_tx, List<String> involvedKeys) {
        // increase number of page edits (for statistics)
        // as this is not that important, use a separate transaction and do not
        // fail if updating the value fails
        try {
            String scalaris_key = getStatsPageEditsKey();
            try {
                Transaction.RequestList requests = new Transaction.RequestList();
                requests.addAddOnNr(scalaris_key, 1).addCommit();
                involvedKeys.addAll(requests.keyList());
                scalaris_tx.req_list(requests).processAddOnNrAt(0);
            } catch (NotANumberException e) {
                // internal error that should not occur!
                e.printStackTrace();
            }
        } catch (Exception e) {
        }
        return null;
    }
    
    /**
     * Handles differences of sets.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    private static class Difference {
        public Set<String> onlyOld;
        public Set<String> onlyNew;
        @SuppressWarnings("unchecked")
        private Set<String>[] changes = new Set[2];
        private GetPageListKey keyGen;
        
        /**
         * Creates a new object calculating differences of two sets.
         * 
         * @param oldSet
         *            the old set
         * @param newSet
         *            the new set
         * @param keyGen
         *            object creating the Scalaris key for the page lists (based
         *            on a set entry)
         */
        public Difference(Set<String> oldSet, Set<String> newSet, GetPageListKey keyGen) {
            this.onlyOld = new HashSet<String>(oldSet);
            this.onlyNew = new HashSet<String>(newSet);
            this.onlyOld.removeAll(newSet);
            this.onlyNew.removeAll(oldSet);
            this.changes[0] = this.onlyOld;
            this.changes[1] = this.onlyNew;
            this.keyGen = keyGen;
        }
        
        static public interface GetPageListKey {
            /**
             * Gets the Scalaris key for a page list for the given article's
             * name.
             * 
             * @param name the name of an article
             * @return the key for Scalaris
             */
            public abstract String getPageListKey(String name);
        }
        
        static public interface GetPageListAndCountKey extends GetPageListKey {
            /**
             * Gets the Scalaris key for a page list counter for the given
             * article's name.
             * 
             * @param name the name of an article
             * @return the key for Scalaris
             */
            public abstract String getPageCountKey(String name);
        }
        
        /**
         * Adds read operations to the given request list as required by
         * {@link #updatePageLists_prepare_write(ResultList, de.zib.scalaris.Transaction.RequestList, String, int, String, List)}.
         * 
         * @param readRequests
         *            list of requests, i.e. operations for Scalaris
         * @param title
         *            (normalised) page name to update
         * 
         * @return the number of added requests, i.e. operations
         */
        public int updatePageLists_prepare_read(
                Transaction.RequestList readRequests, String title) {
            int ops = 0;
            // read old and new page lists
            for (Set<String> curList : changes) {
                for (String name: curList) {
                    readRequests.addRead(keyGen.getPageListKey(name));
                    ++ops;
                }
            }
            return ops;
        }
        
        /**
         * Removes <tt>title</tt> from the list of pages in the old set and adds
         * it to the new ones. Evaluates reads from
         * {@link #updatePageLists_prepare_read(de.zib.scalaris.Transaction.RequestList, String)}
         * and adds writes to the given request list.
         * 
         * @param readResults
         *            results of previous read operations by
         *            {@link #updatePageLists_prepare_read(de.zib.scalaris.Transaction.RequestList, String)}
         * @param writeRequests
         *            list of requests, i.e. operations for Scalaris
         * @param title
         *            (normalised) page name to update
         * @param firstOp
         *            position of the first operation in the result list
         * @param statName
         *            name for the time measurement statistics
         * @param involvedKeys
         *            all keys that have been read or written during the
         *            operation
         * 
         * @return the result of the operation
         */
        public ValueResult<Integer> updatePageLists_prepare_write(
                Transaction.ResultList readResults,
                Transaction.RequestList writeRequests, String title,
                int firstOp, final String statName, List<String> involvedKeys) {
            final long timeAtStart = System.currentTimeMillis();
            int writeOps = 0;
            String scalaris_key;
            // beware: keep order of operations in sync with readPageLists_prepare!
            // remove from old page list
            for (String name: onlyOld) {
                scalaris_key = keyGen.getPageListKey(name);
//                System.out.println(scalaris_key + " -= " + title);
                List<String> pageList;
                try {
                    pageList = readResults.processReadAt(firstOp++).stringListValue();
                    pageList.remove(title);
                    writeRequests.addWrite(scalaris_key, pageList);
                    ++writeOps;
//                } catch (NotFoundException e) {
//                    // this is NOT ok
                } catch (Exception e) {
                    return new ValueResult<Integer>(false, involvedKeys,
                            "unknown exception removing \"" + title
                                    + "\" from \"" + scalaris_key
                                    + "\" in Scalaris: " + e.getMessage(),
                            e instanceof ConnectionException, statName,
                            System.currentTimeMillis() - timeAtStart);
                }
                if (keyGen instanceof GetPageListAndCountKey) {
                    GetPageListAndCountKey keyCountGen = (GetPageListAndCountKey) keyGen;
                    writeRequests.addWrite(keyCountGen.getPageCountKey(name), pageList.size());
                    ++writeOps;
                }
            }
            // add to new page list
            for (String name: onlyNew) {
                scalaris_key = keyGen.getPageListKey(name);
//              System.out.println(scalaris_key + " += " + title);
                List<String> pageList;
                try {
                    pageList = readResults.processReadAt(firstOp++).stringListValue();
                } catch (NotFoundException e) {
                    // this is ok
                    pageList = new LinkedList<String>();
                } catch (Exception e) {
                    return new ValueResult<Integer>(false, involvedKeys,
                            "unknown exception reading \"" + scalaris_key
                                    + "\" in Scalaris: " + e.getMessage(),
                            e instanceof ConnectionException, statName,
                            System.currentTimeMillis() - timeAtStart);
                }
                pageList.add(title);
                writeRequests.addWrite(scalaris_key, pageList);
                ++writeOps;
                if (keyGen instanceof GetPageListAndCountKey) {
                    GetPageListAndCountKey keyCountGen = (GetPageListAndCountKey) keyGen;
                    writeRequests.addWrite(keyCountGen.getPageCountKey(name), pageList.size());
                    ++writeOps;
                }
            }
            return new ValueResult<Integer>(involvedKeys, writeOps, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        
        /**
         * Checks whether all writes from
         * {@link #updatePageLists_prepare_write(ResultList, de.zib.scalaris.Transaction.RequestList, String, int, String, List)}
         * were successful.
         * 
         * @param writeResults
         *            results of previous write operations
         * @param title
         *            (normalised) page name to update
         * @param firstOp
         *            position of the first operation in the result list
         * @param statName
         *            name for the time measurement statistics
         * @param involvedKeys
         *            all keys that have been read or written during the
         *            operation
         * 
         * @return the result of the operation
         */
        public ValueResult<Integer> updatePageLists_check_writes(
                Transaction.ResultList writeResults, String title, int firstOp,
                final String statName, List<String> involvedKeys) {
            final long timeAtStart = System.currentTimeMillis();
            // beware: keep order of operations in sync with readPageLists_prepare!
            // evaluate results changing the old and new page lists
            for (Set<String> curList : changes) {
                for (String name : curList) {
                    try {
                        writeResults.processWriteAt(firstOp++);
                    } catch (Exception e) {
                        return new ValueResult<Integer>(false, involvedKeys,
                                "unknown exception validating the addition/removal of \""
                                        + title + "\" to/from \""
                                        + keyGen.getPageListKey(name)
                                        + "\" in Scalaris: " + e.getMessage(),
                                e instanceof ConnectionException, statName,
                                System.currentTimeMillis() - timeAtStart);
                    }
                }
            }
            return new ValueResult<Integer>(involvedKeys, null, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        
        /**
         * Removes <tt>title</tt> from the list of pages in the old set and adds
         * it to the new ones using
         * {@link Transaction#addDelOnList(String, List, List)}.
         * 
         * @param writeRequests
         *            list of requests, i.e. operations for Scalaris
         * @param title
         *            (normalised) page name to update
         * @param statName
         *            name for the time measurement statistics
         * @param involvedKeys
         *            all keys that have been read or written during the
         *            operation
         * 
         * @return the result of the operation
         */
        public ValueResult<Integer> updatePageLists_prepare_appends(
                Transaction.RequestList writeRequests, String title,
                final String statName, List<String> involvedKeys) {
            final long timeAtStart = System.currentTimeMillis();
            int writeOps = 0;
            String scalaris_key;
            // beware: keep order of operations in sync with readPageLists_prepare!
            // remove from old page list
            for (String name: onlyOld) {
                scalaris_key = keyGen.getPageListKey(name);
//                System.out.println(scalaris_key + " -= " + title);
                writeRequests.addAddDelOnList(scalaris_key, null, Arrays.asList(title));
                ++writeOps;
                if (keyGen instanceof GetPageListAndCountKey) {
                    GetPageListAndCountKey keyCountGen = (GetPageListAndCountKey) keyGen;
                    writeRequests.addAddOnNr(keyCountGen.getPageCountKey(name), -1);
                    ++writeOps;
                }
            }
            // add to new page list
            for (String name: onlyNew) {
                scalaris_key = keyGen.getPageListKey(name);
//              System.out.println(scalaris_key + " += " + title);
                writeRequests.addAddDelOnList(scalaris_key, Arrays.asList(title), null);
                ++writeOps;
                if (keyGen instanceof GetPageListAndCountKey) {
                    GetPageListAndCountKey keyCountGen = (GetPageListAndCountKey) keyGen;
                    writeRequests.addAddOnNr(keyCountGen.getPageCountKey(name), 1);
                    ++writeOps;
                }
            }
            return new ValueResult<Integer>(involvedKeys, writeOps, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        
        /**
         * Checks whether all appends from
         * {@link #updatePageLists_prepare_appends(de.zib.scalaris.Transaction.RequestList, String, String, List)}
         * were successful.
         * 
         * @param appendResults
         *            results of previous append operations
         * @param title
         *            (normalised) page name to update
         * @param firstOp
         *            position of the first operation in the result list
         * @param statName
         *            name for the time measurement statistics
         * @param involvedKeys
         *            all keys that have been read or written during the
         *            operation
         * 
         * @return the result of the operation
         */
        public ValueResult<Integer> updatePageLists_check_appends(
                Transaction.ResultList appendResults, String title,
                int firstOp, final String statName, List<String> involvedKeys) {
            final long timeAtStart = System.currentTimeMillis();
            // beware: keep order of operations in sync with readPageLists_prepare!
            // evaluate results changing the old and new page lists
            for (Set<String> curList : changes) {
                for (String name : curList) {
                    try {
                        appendResults.processAddDelOnListAt(firstOp++);
                    } catch (Exception e) {
                        return new ValueResult<Integer>(false, involvedKeys,
                                "unknown exception validating the addition/removal of \""
                                        + title + "\" to/from \""
                                        + keyGen.getPageListKey(name)
                                        + "\" in Scalaris: " + e.getMessage(),
                                e instanceof ConnectionException, statName,
                                System.currentTimeMillis() - timeAtStart);
                    }
                }
            }
            return new ValueResult<Integer>(involvedKeys, null, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
    }
    
    /**
     * Updates a list of pages by removing and/or adding new page titles.
     * 
     * @param requests
     *            list of requests, i.e. operations for Scalaris
     * @param pageList_key
     *            Scalaris key for the page list
     * 
     * @return the number of added requests, i.e. operations
     */
    protected static int updatePageList_prepare_read(
            Transaction.RequestList requests, String pageList_key) {
        requests.addRead(pageList_key);
        return 1;
    }
    
    /**
     * Updates a list of pages by removing and/or adding new page titles.
     * 
     * @param readResults
     *            results of previous read operations by
     *            {@link #updatePageList_prepare_read(RequestList, String)}
     * @param writeRequests
     *            list of requests, i.e. operations for Scalaris
     * @param pageList_key
     *            Scalaris key for the page list
     * @param pageCount_key
     *            Scalaris key for the number of pages in the list (may be null
     *            if not used)
     * @param entriesToRemove
     *            (normalised) page names to remove
     * @param entriesToAdd
     *            (normalised) page names to add
     * @param firstOp
     *            number of the first operation in the result set
     * @param statName
     *            name for the time measurement statistics
     * @param involvedKeys
     *            all keys that have been read or written during the operation
     * 
     * @return the result of the operation
     */
    protected static ValueResult<Integer> updatePageList_prepare_write(
            Transaction.ResultList readResults,
            Transaction.RequestList writeRequests, String pageList_key,
            String pageCount_key, Collection<String> entriesToRemove,
            Collection<String> entriesToAdd, int firstOp,
            final String statName, List<String> involvedKeys) {
        final long timeAtStart = System.currentTimeMillis();
        List<String> pages;
        try {
            pages = readResults.processReadAt(firstOp++).stringListValue();
        } catch (NotFoundException e) {
            pages = new LinkedList<String>();
        } catch (Exception e) {
            return new ValueResult<Integer>(false, involvedKeys,
                    "unknown exception updating \"" + pageList_key
                            + "\" and \"" + pageCount_key + "\" in Scalaris: "
                            + e.getMessage(), e instanceof ConnectionException,
                    statName, System.currentTimeMillis() - timeAtStart);
        }
        // note: no need to read the old count - we already have a list of all pages and can calculate the count on our own
        int oldCount = pages.size();
        
        int count = oldCount;
        boolean listChanged = false;
        
        pages.removeAll(entriesToRemove);
        if (pages.size() != count) {
            listChanged = true;
        }
        count = pages.size();
        
        pages.addAll(entriesToAdd);
        if (pages.size() != count) {
            listChanged = true;
        }
        count = pages.size();

        int writeOps = 0;
        if (listChanged) {
            writeRequests.addWrite(pageList_key, pages);
            ++writeOps;
            if (pageCount_key != null && !pageCount_key.isEmpty() && count != oldCount) {
                writeRequests.addWrite(pageCount_key, count);
                ++writeOps;
            }
        }
        return new ValueResult<Integer>(involvedKeys, writeOps, statName,
                System.currentTimeMillis() - timeAtStart);
    }
    
    /**
     * Checks whether all writes from
     * {@link #updatePageList_prepare_write(ResultList, de.zib.scalaris.Transaction.RequestList, String, String, Collection, Collection, int, String)}
     * were successful.
     * 
     * @param writeResults
     *            results of previous write operations
     * @param pageList_key
     *            Scalaris key for the page list
     * @param pageCount_key
     *            Scalaris key for the number of pages in the list (may be null
     *            if not used)
     * @param firstOp
     *            position of the first operation in the result list
     * @param writeOps
     *            number of supposed write operations
     * @param statName
     *            name for the time measurement statistics
     * @param involvedKeys
     *            all keys that have been read or written during the operation
     * 
     * @return the result of the operation
     */
    protected static ValueResult<Integer> updatePageList_check_writes(
            Transaction.ResultList writeResults, String pageList_key,
            String pageCount_key, int firstOp, int writeOps,
            final String statName, List<String> involvedKeys) {
        final long timeAtStart = System.currentTimeMillis();
        try {
            for (int i = firstOp; i < firstOp + writeOps; ++i) {
                writeResults.processWriteAt(i);
            }
        } catch (Exception e) {
            return new ValueResult<Integer>(false, involvedKeys,
                    "unknown exception updating \"" + pageList_key
                            + "\" and \"" + pageCount_key + "\" in Scalaris: "
                            + e.getMessage(), e instanceof ConnectionException,
                    statName, System.currentTimeMillis() - timeAtStart);
        }
        return new ValueResult<Integer>(involvedKeys, null, statName,
                System.currentTimeMillis() - timeAtStart);
    }
    
    /**
     * Updates a list of pages by removing and/or adding new page titles using
     * {@link Transaction#addDelOnList(String, List, List)}.
     * 
     * Assumes that objects in <tt>entriesToRemove</tt> exist in the given list
     * and that objects in <tt>entriesToAdd</tt> do not exist (otherwise the
     * page counter at <tt>pageCount_key</tt> will be wrong).
     * 
     * @param writeRequests
     *            list of requests, i.e. operations for Scalaris
     * @param pageList_key
     *            Scalaris key for the page list
     * @param pageCount_key
     *            Scalaris key for the number of pages in the list (may be null
     *            if not used)
     * @param entriesToRemove
     *            (normalised) page names to remove
     * @param entriesToAdd
     *            (normalised) page names to add
     * @param statName
     *            name for the time measurement statistics
     * @param involvedKeys
     *            all keys that have been read or written during the operation
     * 
     * @return the result of the operation
     */
    protected static ValueResult<Integer> updatePageList_prepare_appends(
            Transaction.RequestList writeRequests, String pageList_key,
            String pageCount_key, List<String> entriesToRemove,
            List<String> entriesToAdd, final String statName, List<String> involvedKeys) {
        final long timeAtStart = System.currentTimeMillis();
        int writeOps = 0;
        writeRequests.addAddDelOnList(pageList_key, entriesToAdd, entriesToRemove);
        ++writeOps;
        if (pageCount_key != null && !pageCount_key.isEmpty()) {
            writeRequests.addWrite(pageCount_key, entriesToAdd.size() - entriesToRemove.size());
            ++writeOps;
        }
        return new ValueResult<Integer>(involvedKeys, writeOps, statName,
                System.currentTimeMillis() - timeAtStart);
    }
    
    /**
     * Checks whether all writes from
     * {@link #updatePageList_prepare_appends(de.zib.scalaris.Transaction.RequestList, String, String, List, List, String)}
     * were successful.
     * 
     * @param writeResults
     *            results of previous write operations
     * @param pageList_key
     *            Scalaris key for the page list
     * @param pageCount_key
     *            Scalaris key for the number of pages in the list (may be null
     *            if not used)
     * @param firstOp
     *            position of the first operation in the result list
     * @param writeOps
     *            number of supposed write operations
     * @param statName
     *            name for the time measurement statistics
     * @param involvedKeys
     *            all keys that have been read or written during the operation
     * 
     * @return the result of the operation
     */
    protected static ValueResult<Integer> updatePageList_check_appends(
            Transaction.ResultList writeResults, String pageList_key,
            String pageCount_key, int firstOp, int writeOps,
            final String statName, List<String> involvedKeys) {
        final long timeAtStart = System.currentTimeMillis();
        try {
            for (int i = firstOp; i < firstOp + writeOps; ++i) {
                writeResults.processAddDelOnListAt(i);
            }
        } catch (Exception e) {
            return new ValueResult<Integer>(false, involvedKeys, "unknown exception updating \""
                    + pageList_key + "\" and \"" + pageCount_key
                    + "\" in Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        return new ValueResult<Integer>(involvedKeys, null, statName,
                System.currentTimeMillis() - timeAtStart);
    }
    
    /**
     * Updates a list of pages by removing and/or adding new page titles.
     * 
     * @param scalaris_tx
     *            connection to Scalaris
     * @param pageList_key
     *            Scalaris key for the page list
     * @param pageCount_key
     *            Scalaris key for the number of pages in the list (may be null
     *            if not used)
     * @param entriesToRemove
     *            list of (normalised) page names to remove from the list
     * @param entriesToAdd
     *            list of (normalised) page names to add to the list
     * @param statName
     *            name for the time measurement statistics
     * 
     * @return the result of the operation
     */
    public static ValueResult<Integer> updatePageList(Transaction scalaris_tx,
            String pageList_key, String pageCount_key,
            List<String> entriesToRemove, List<String> entriesToAdd,
            final String statName) {
        final long timeAtStart = System.currentTimeMillis();
        List<String> involvedKeys = new ArrayList<String>();
        Transaction.RequestList requests;
        Transaction.ResultList results;

        try {
            requests = new Transaction.RequestList();
            ValueResult<Integer> result;
            if (Options.WIKI_USE_NEW_SCALARIS_OPS) {
                result = updatePageList_prepare_appends(requests,
                        pageList_key, pageCount_key, entriesToRemove,
                        entriesToAdd, statName, involvedKeys);
                if (!result.success) {
                    return result;
                }
                requests.addCommit();
                involvedKeys.addAll(requests.keyList());
                results = scalaris_tx.req_list(requests);
                result = updatePageList_check_appends(results, pageList_key,
                        pageCount_key, 0, result.value, statName, involvedKeys);
            } else {
                updatePageList_prepare_read(requests, pageList_key);
                involvedKeys.addAll(requests.keyList());
                results = scalaris_tx.req_list(requests);

                requests = new Transaction.RequestList();
                result = updatePageList_prepare_write(results, requests,
                        pageList_key, pageCount_key, entriesToRemove, entriesToAdd,
                        0, statName, involvedKeys);
                if (!result.success) {
                    return result;
                }
                requests.addCommit();
                involvedKeys.addAll(requests.keyList());
                results = scalaris_tx.req_list(requests);

                result = updatePageList_check_writes(results, pageList_key,
                        pageCount_key, 0, result.value, statName, involvedKeys);
            }
            if (!result.success) {
                return result;
            }
        } catch (Exception e) {
            return new ValueResult<Integer>(false, involvedKeys,
                    "unknown exception updating \"" + pageList_key
                            + "\" and \"" + pageCount_key + "\" in Scalaris: "
                            + e.getMessage(), e instanceof ConnectionException,
                    statName, System.currentTimeMillis() - timeAtStart);
        }
        return new ValueResult<Integer>(involvedKeys, null, statName,
                System.currentTimeMillis() - timeAtStart);
    }
}
