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

import de.zib.scalaris.Connection;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.ErlangValue;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.Transaction;
import de.zib.scalaris.TransactionSingleOp;
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
        if (connection == null) {
            return new PageHistoryResult(false, "no connection to Scalaris",
                    true, System.currentTimeMillis() - timeAtStart);
        }
        
        TransactionSingleOp scalaris_single;

        scalaris_single = new TransactionSingleOp(connection);
        TransactionSingleOp.RequestList requests = new TransactionSingleOp.RequestList();
        requests.addRead(getPageKey(title, nsObject)).addRead(getRevListKey(title, nsObject));
        
        TransactionSingleOp.ResultList results;
        try {
            results = scalaris_single.req_list(requests);
        } catch (Exception e) {
            return new PageHistoryResult(false, "unknown exception reading \""
                    + getPageKey(title, nsObject) + "\" or \""
                    + getRevListKey(title, nsObject) + "\" from Scalaris: "
                    + e.getMessage(), e instanceof ConnectionException,
                    System.currentTimeMillis() - timeAtStart);
        }
        
        Page page;
        try {
            page = results.processReadAt(0).jsonValue(Page.class);
        } catch (NotFoundException e) {
            PageHistoryResult result = new PageHistoryResult(
                    false,
                    "page not found at \"" + getPageKey(title, nsObject) + "\"",
                    false, System.currentTimeMillis() - timeAtStart);
            result.not_existing = true;
            return result;
        } catch (Exception e) {
            return new PageHistoryResult(false, "unknown exception reading \""
                    + getPageKey(title, nsObject) + "\" from Scalaris: "
                    + e.getMessage(), e instanceof ConnectionException,
                    System.currentTimeMillis() - timeAtStart);
        }

        List<ShortRevision> revisions;
        try {
            revisions = results.processReadAt(1).jsonListValue(ShortRevision.class);
        } catch (NotFoundException e) {
            PageHistoryResult result = new PageHistoryResult(false,
                    "revision list \"" + getRevListKey(title, nsObject)
                            + "\" does not exist", false,
                    System.currentTimeMillis() - timeAtStart);
            result.not_existing = true;
            return result;
        } catch (Exception e) {
            return new PageHistoryResult(false, "unknown exception reading \""
                    + getRevListKey(title, nsObject) + "\" from Scalaris: "
                    + e.getMessage(), e instanceof ConnectionException,
                    System.currentTimeMillis() - timeAtStart);
        }
        return new PageHistoryResult(page, revisions, System.currentTimeMillis() - timeAtStart);
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
        if (connection == null) {
            return new RevisionResult(false, "no connection to Scalaris", true,
                    page, revision, false, false, System.currentTimeMillis()
                            - timeAtStart);
        }
        
        TransactionSingleOp scalaris_single;
        String scalaris_key;
        
        scalaris_single = new TransactionSingleOp(connection);

        scalaris_key = getPageKey(title, nsObject);
        try {
            page = scalaris_single.read(scalaris_key).jsonValue(Page.class);
        } catch (NotFoundException e) {
            return new RevisionResult(false, "page not found at \""
                    + scalaris_key + "\"", false, page, revision, true, false,
                    System.currentTimeMillis() - timeAtStart);
        } catch (Exception e) {
            return new RevisionResult(false, "unknown exception reading \""
                    + scalaris_key + "\" from Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, page, revision, false,
                    false, System.currentTimeMillis() - timeAtStart);
        }

        // load requested version if it is not the current one cached in the Page object
        if (id != page.getCurRev().getId() && id >= 0) {
            scalaris_key = getRevKey(title, id, nsObject);
            try {
                revision = scalaris_single.read(scalaris_key).jsonValue(Revision.class);
            } catch (NotFoundException e) {
                return new RevisionResult(false, "revision not found at \""
                        + scalaris_key + "\"", false, page, revision, false,
                        true, System.currentTimeMillis() - timeAtStart);
            } catch (Exception e) {
                return new RevisionResult(false, "unknown exception reading \""
                        + scalaris_key + "\" from Scalaris: " + e.getMessage(),
                        e instanceof ConnectionException, page, revision,
                        false, false, System.currentTimeMillis() - timeAtStart);
            }
        } else {
            revision = page.getCurRev();
        }

        return new RevisionResult(page, revision, System.currentTimeMillis() - timeAtStart);
    }

    /**
     * Retrieves a list of available pages from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * 
     * @return a result object with the page list on success
     */
    public static PageListResult getPageList(Connection connection) {
        return getPageList2(connection, getPageListKey(), false);
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
    public static PageListResult getArticleList(Connection connection) {
        return getPageList2(connection, getArticleListKey(), false);
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
    public static PageListResult getPagesInCategory(Connection connection,
            String title, final MyNamespace nsObject) {
        return getPageList2(connection, getCatPageListKey(title, nsObject),
                false);
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
    public static PageListResult getPagesInTemplate(Connection connection,
            String title, final MyNamespace nsObject) {
        return getPageList2(connection, getTplPageListKey(title, nsObject),
                true);
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
    public static PageListResult getPagesLinkingTo(Connection connection,
            String title, final MyNamespace nsObject) {
        return getPageList2(connection,
                getBackLinksPageListKey(title, nsObject), false);
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
     * 
     * @return a result object with the page list on success
     */
    private static PageListResult getPageList2(Connection connection,
            String scalaris_key, boolean failNotFound) {
        final long timeAtStart = System.currentTimeMillis();
        if (connection == null) {
            return new PageListResult(false, "no connection to Scalaris", true,
                    System.currentTimeMillis() - timeAtStart);
        }
        
        TransactionSingleOp scalaris_single = new TransactionSingleOp(connection);
        try {
            List<String> pages = scalaris_single.read(scalaris_key).stringListValue();
            return new PageListResult(pages, System.currentTimeMillis() - timeAtStart);
        } catch (NotFoundException e) {
            if (failNotFound) {
                return new PageListResult(false,
                        "unknown exception reading page list at \""
                                + scalaris_key + "\" from Scalaris: "
                                + e.getMessage(), false,
                        System.currentTimeMillis() - timeAtStart);
            } else {
                return new PageListResult(new LinkedList<String>(),
                        System.currentTimeMillis() - timeAtStart);
            }
        } catch (Exception e) {
            return new PageListResult(false,
                    "unknown exception reading page list at \"" + scalaris_key
                            + "\" from Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException,
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
    public static BigIntegerResult getPageCount(Connection connection) {
        return getInteger2(connection, getPageCountKey(), false);
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
    public static BigIntegerResult getArticleCount(Connection connection) {
        return getInteger2(connection, getArticleCountKey(), false);
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
    public static BigIntegerResult getPagesInCategoryCount(
            Connection connection, String title, final MyNamespace nsObject) {
        return getInteger2(connection, getCatPageCountKey(title, nsObject), false);
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
    public static BigIntegerResult getStatsPageEdits(Connection connection) {
        return getInteger2(connection, getStatsPageEditsKey(), false);
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
     * 
     * @return a result object with the number on success
     */
    private static BigIntegerResult getInteger2(Connection connection,
            String scalaris_key, boolean failNotFound) {
        final long timeAtStart = System.currentTimeMillis();
        if (connection == null) {
            return new BigIntegerResult(false, "no connection to Scalaris",
                    true, System.currentTimeMillis() - timeAtStart);
        }
        
        TransactionSingleOp scalaris_single = new TransactionSingleOp(connection);
        try {
            BigInteger number = scalaris_single.read(scalaris_key).bigIntValue();
            return new BigIntegerResult(number, System.currentTimeMillis() - timeAtStart);
        } catch (NotFoundException e) {
            if (failNotFound) {
                return new BigIntegerResult(false,
                        "unknown exception reading (integral) number at \""
                                + scalaris_key + "\" from Scalaris: "
                                + e.getMessage(), false,
                        System.currentTimeMillis() - timeAtStart);
            } else {
                return new BigIntegerResult(BigInteger.valueOf(0),
                        System.currentTimeMillis() - timeAtStart);
            }
        } catch (Exception e) {
            return new BigIntegerResult(false,
                    "unknown exception reading (integral) number at \""
                            + scalaris_key + "\" from Scalaris: "
                            + e.getMessage(), e instanceof ConnectionException,
                    System.currentTimeMillis() - timeAtStart);
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
    public static RandomTitleResult getRandomArticle(Connection connection, Random random) {
        final long timeAtStart = System.currentTimeMillis();
        if (connection == null) {
            return new RandomTitleResult(false, "no connection to Scalaris",
                    true, System.currentTimeMillis() - timeAtStart);
        }
        
        TransactionSingleOp scalaris_single = new TransactionSingleOp(connection);
        try {
            List<ErlangValue> pages = scalaris_single.read(getArticleListKey()).listValue();
            String randomTitle = pages.get(random.nextInt(pages.size())).stringValue();
            return new RandomTitleResult(randomTitle, System.currentTimeMillis() - timeAtStart);
        } catch (Exception e) {
            return new RandomTitleResult(false,
                    "unknown exception reading page list at \"pages\" from Scalaris: "
                            + e.getMessage(), e instanceof ConnectionException,
                    System.currentTimeMillis() - timeAtStart);
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
        final long timeAtStart = System.currentTimeMillis();
        Page oldPage = null;
        Page newPage = null;
        List<ShortRevision> newShortRevs = null;
        BigInteger pageEdits = null;
        if (connection == null) {
            return new SavePageResult(false, "no connection to Scalaris", true,
                    oldPage, newPage, newShortRevs, pageEdits,
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
            results = scalaris_tx.req_list(requests);
        } catch (Exception e) {
            return new SavePageResult(false,
                    "unknown exception getting page info (" + pageInfoKey
                            + ") from Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, oldPage, newPage,
                    newShortRevs, pageEdits, System.currentTimeMillis()
                            - timeAtStart);
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
            return new SavePageResult(false, "unknown exception reading \""
                    + pageInfoKey + "\" from Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, oldPage, newPage,
                    newShortRevs, pageEdits, System.currentTimeMillis()
                            - timeAtStart);
        }
        
        if (!newPage.checkEditAllowed(username)) {
            return new SavePageResult(false, "operation not allowed: edit is restricted", false,
                    oldPage, newPage, newShortRevs, pageEdits,
                    System.currentTimeMillis() - timeAtStart);
        }
        
        if (prevRevId != oldRevId) {
            return new SavePageResult(false, "curRev(" + oldRevId
                    + ") != oldRev(" + prevRevId + ")", false, oldPage,
                    newPage, newShortRevs, pageEdits,
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
            wikiModel.render(null, oldPage.getCurRev().unpackedText());
            // note: no need to normalise the pages, we will do so during the write/read key generation
            oldCats = wikiModel.getCategories().keySet();
            oldTpls = wikiModel.getTemplates();
            oldLnks = wikiModel.getLinks();
            wikiModel.tearDown();
        } else {
            oldCats = new HashSet<String>();
            oldTpls = new HashSet<String>();
            oldLnks = new HashSet<String>();
        }
        // get new categories and templates
        wikiModel.setUp();
        wikiModel.render(null, newRev.unpackedText());
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
        final Set<String> newTpls = wikiModel.getTemplates();
        final Set<String> newLnks = wikiModel.getLinks();
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
        Difference tplDiff = new Difference(oldTpls, newTpls,
                new Difference.GetPageListKey() {
                    @Override
                    public String getPageListKey(String name) {
                        return getTplPageListKey(
                                wikiModel.getTemplateNamespace() + ":" + name,
                                nsObject);
                    }
                });
        Difference lnkDiff = new Difference(oldLnks, newLnks,
                new Difference.GetPageListKey() {
                    @Override
                    public String getPageListKey(String name) {
                        return getBackLinksPageListKey(name, nsObject);
                    }
                });

        // write differences (categories, templates, backlinks)
        // new page? -> add to page/article lists
        List<Integer> pageListWrites = new ArrayList<Integer>(2);
        List<String> pageListKeys = new LinkedList<String>();
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
        int catPageReads = catDiff.updatePageLists_prepare_read(requests, title);
        int tplPageReads = tplDiff.updatePageLists_prepare_read(requests, title);
        int lnkPageReads = lnkDiff.updatePageLists_prepare_read(requests, title);
        for (Iterator<String> it = pageListKeys.iterator(); it.hasNext();) {
            String pageList_key = (String) it.next();
            // note: do not need to retrieve page list count (we can calculate it on our own)
            @SuppressWarnings("unused")
            String pageCount_key = (String) it.next();
            updatePageList_prepare_read(requests, pageList_key);
        }
        results = null;
        try {
            results = scalaris_tx.req_list(requests);
            curOp = 0;
        } catch (Exception e) {
            return new SavePageResult(false,
                    "unknown exception reading page lists for page \"" + title0
                            + "\" from Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, oldPage, newPage,
                    newShortRevs, pageEdits, System.currentTimeMillis()
                            - timeAtStart);
        }
        //  PAGE LISTS UPDATE, step 2: prepare writes of new lists
        requests = new Transaction.RequestList();
        try {
            newShortRevs = results.processReadAt(curOp++).jsonListValue(ShortRevision.class);
        } catch (NotFoundException e) {
            if (prevRevId == -1) { // new page?
                newShortRevs = new LinkedList<ShortRevision>();
            } else {
//              e.printStackTrace();
                // corrupt DB - don't save page
                return new SavePageResult(false, "revision list for page \""
                        + title0 + "\" not found at \"" + revListKey + "\"",
                        false, oldPage, newPage, newShortRevs, pageEdits,
                        System.currentTimeMillis() - timeAtStart);
            }
        } catch (Exception e) {
            return new SavePageResult(false, "unknown exception reading \""
                    + revListKey + "\" from Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, oldPage, newPage,
                    newShortRevs, pageEdits, System.currentTimeMillis()
                            - timeAtStart);
        }
        newShortRevs.add(0, new ShortRevision(newRev));
        requests.addWrite(revListKey, newShortRevs);
        SaveResult pageListResult;
        pageListResult = catDiff.updatePageLists_prepare_write(results,
                requests, title, curOp);
        curOp += catPageReads;
        if (!pageListResult.success) {
            return new SavePageResult(false, pageListResult.message,
                    pageListResult.connect_failed, oldPage, newPage,
                    newShortRevs, pageEdits, System.currentTimeMillis()
                            - timeAtStart);
        }
        pageListResult = tplDiff.updatePageLists_prepare_write(results,
                requests, title, curOp);
        curOp += tplPageReads;
        if (!pageListResult.success) {
            return new SavePageResult(false, pageListResult.message,
                    pageListResult.connect_failed, oldPage, newPage,
                    newShortRevs, pageEdits, System.currentTimeMillis()
                            - timeAtStart);
        }
        pageListResult = lnkDiff.updatePageLists_prepare_write(results,
                requests, title, curOp);
        curOp += lnkPageReads;
        if (!pageListResult.success) {
            return new SavePageResult(false, pageListResult.message,
                    pageListResult.connect_failed, oldPage, newPage,
                    newShortRevs, pageEdits, System.currentTimeMillis()
                            - timeAtStart);
        }
        final List<String> newPages = Arrays.asList(title);
        for (Iterator<String> it = pageListKeys.iterator(); it.hasNext();) {
            String pageList_key = (String) it.next();
            String pageCount_key = (String) it.next();
            
            pageListResult = updatePageList_prepare_write(results, requests,
                    pageList_key, pageCount_key, new LinkedList<String>(),
                    newPages, curOp);
            if (!pageListResult.success) {
                return new SavePageResult(false, pageListResult.message,
                        pageListResult.connect_failed, oldPage, newPage,
                        newShortRevs, pageEdits, System.currentTimeMillis()
                                - timeAtStart);
            }
            ++curOp; // processed one read op during each call of updatePageList_prepare_write
            pageListWrites.add((Integer) pageListResult.info);
        }
        // smuggle in the final write requests to save a round-trip to Scalaris:
        requests.addWrite(getPageKey(title0, nsObject), newPage)
                .addWrite(getRevKey(title0, newRev.getId(), nsObject), newRev)
                .addCommit();
        //  PAGE LISTS UPDATE, step 3: execute and evaluate writes of new lists
        results = null;
        try {
            results = scalaris_tx.req_list(requests);
            curOp = 0;
        } catch (Exception e) {
            return new SavePageResult(false,
                    "unknown exception writing page or page lists for page \""
                            + title0 + "\" to Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, oldPage, newPage,
                    newShortRevs, pageEdits, System.currentTimeMillis()
                            - timeAtStart);
        }

        try {
            results.processWriteAt(curOp++);
        } catch (Exception e) {
            return new SavePageResult(false,
                    "unknown exception writing page \"" + title0
                    + "\" to Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, oldPage, newPage,
                    newShortRevs, pageEdits, System.currentTimeMillis()
                    - timeAtStart);
        }
        pageListResult = catDiff.updatePageLists_check_writes(results, title, curOp);
        curOp += catPageReads;
        if (!pageListResult.success) {
            return new SavePageResult(false, pageListResult.message,
                    pageListResult.connect_failed, oldPage, newPage,
                    newShortRevs, pageEdits, System.currentTimeMillis()
                            - timeAtStart);
        }
        pageListResult = tplDiff.updatePageLists_check_writes(results, title, curOp);
        curOp += tplPageReads;
        if (!pageListResult.success) {
            return new SavePageResult(false, pageListResult.message,
                    pageListResult.connect_failed, oldPage, newPage,
                    newShortRevs, pageEdits, System.currentTimeMillis()
                            - timeAtStart);
        }
        pageListResult = lnkDiff.updatePageLists_check_writes(results, title, curOp);
        curOp += lnkPageReads;
        if (!pageListResult.success) {
            return new SavePageResult(false, pageListResult.message,
                    pageListResult.connect_failed, oldPage, newPage,
                    newShortRevs, pageEdits, System.currentTimeMillis()
                            - timeAtStart);
        }
        int pageList = 0;
        for (Iterator<String> it = pageListKeys.iterator(); it.hasNext();) {
            String pageList_key = (String) it.next();
            String pageCount_key = (String) it.next();

            final Integer writeOps = pageListWrites.get(pageList);
            pageListResult = updatePageList_check_writes(results, pageList_key,
                    pageCount_key, curOp, writeOps);
            if (!pageListResult.success) {
                return new SavePageResult(false, pageListResult.message,
                        pageListResult.connect_failed, oldPage, newPage,
                        newShortRevs, pageEdits, System.currentTimeMillis()
                        - timeAtStart);
            }
            curOp += writeOps;
            ++pageList;
        }

        try {
            results.processWriteAt(curOp++);
            results.processWriteAt(curOp++);
        } catch (Exception e) {
            return new SavePageResult(false,
                    "unknown exception writing page \"" + title0
                    + "\" to Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, oldPage, newPage,
                    newShortRevs, pageEdits, System.currentTimeMillis()
                    - timeAtStart);
        }
        
        // increase number of page edits (for statistics)
        // as this is not that important, use a seperate transaction and do not fail if updating the value fails
        try {
            String scalaris_key = getStatsPageEditsKey();
            try {
                pageEdits = scalaris_tx.read(scalaris_key).bigIntValue();
            } catch (NotFoundException e) {
                pageEdits = BigInteger.valueOf(0);
            }
            pageEdits = pageEdits.add(BigInteger.valueOf(1));
            requests = new Transaction.RequestList();
            requests.addWrite(scalaris_key, pageEdits).addCommit();
            scalaris_tx.req_list(requests);
        } catch (Exception e) {
        }
        
        return new SavePageResult(oldPage, newPage, newShortRevs, pageEdits,
                System.currentTimeMillis() - timeAtStart);
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
         * {@link #updatePageLists_prepare_write(de.zib.scalaris.Transaction.ResultList, de.zib.scalaris.Transaction.RequestList, String, int)}.
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
                    if (keyGen instanceof GetPageListAndCountKey) {
                        GetPageListAndCountKey keyCountGen = (GetPageListAndCountKey) keyGen;
                        readRequests.addRead(keyCountGen.getPageCountKey(name));
                        ++ops;
                    }
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
         * 
         * @return the result of the operation
         */
        public SaveResult updatePageLists_prepare_write(
                Transaction.ResultList readResults,
                Transaction.RequestList writeRequests, String title, int firstOp) {
            final long timeAtStart = System.currentTimeMillis();
            String scalaris_key;
            // beware: keep order of operations in sync with readPageLists_prepare!
            // remove from old page list
            for (String name: onlyOld) {
                scalaris_key = keyGen.getPageListKey(name);
//                System.out.println(scalaris_key + " -= " + title);
                List<String> pageList;
                try {
                    pageList = readResults.processReadAt(firstOp).stringListValue();
                    ++firstOp;
                    pageList.remove(title);
                    writeRequests.addWrite(scalaris_key, pageList);
//                } catch (NotFoundException e) {
//                    // this is NOT ok
                } catch (Exception e) {
                    return new SaveResult(false,
                            "unknown exception removing \"" + title
                                    + "\" from \"" + scalaris_key
                                    + "\" in Scalaris: " + e.getMessage(),
                            e instanceof ConnectionException,
                            System.currentTimeMillis() - timeAtStart);
                }
                if (keyGen instanceof GetPageListAndCountKey) {
                    ++firstOp;
                    GetPageListAndCountKey keyCountGen = (GetPageListAndCountKey) keyGen;
                    writeRequests.addWrite(keyCountGen.getPageCountKey(name), pageList.size());
                }
            }
            // add to new page list
            for (String name: onlyNew) {
                scalaris_key = keyGen.getPageListKey(name);
//              System.out.println(scalaris_key + " += " + title);
                List<String> pageList;
                try {
                    pageList = readResults.processReadAt(firstOp).stringListValue();
                    ++firstOp;
                } catch (NotFoundException e) {
                    // this is ok
                    pageList = new LinkedList<String>();
                } catch (Exception e) {
                    return new SaveResult(false, "unknown exception reading \""
                            + scalaris_key + "\" in Scalaris: "
                            + e.getMessage(), e instanceof ConnectionException,
                            System.currentTimeMillis() - timeAtStart);
                }
                pageList.add(title);
                try {
                    writeRequests.addWrite(scalaris_key, pageList);
                } catch (Exception e) {
                    return new SaveResult(false, "unknown exception adding \""
                            + title + "\" to \"" + scalaris_key
                            + "\" in Scalaris: " + e.getMessage(),
                            e instanceof ConnectionException,
                            System.currentTimeMillis() - timeAtStart);
                }
                if (keyGen instanceof GetPageListAndCountKey) {
                    ++firstOp;
                    GetPageListAndCountKey keyCountGen = (GetPageListAndCountKey) keyGen;
                    writeRequests.addWrite(keyCountGen.getPageCountKey(name), pageList.size());
                }
            }
            return new SaveResult(System.currentTimeMillis() - timeAtStart);
        }
        
        /**
         * Checks whether all writes from
         * {@link #updatePageLists_prepare_write(de.zib.scalaris.Transaction.ResultList, de.zib.scalaris.Transaction.RequestList, String, int)}
         * were successful.
         * 
         * @param writeResults
         *            results of previous write operations by
         *            {@link #updatePageLists_prepare_write(de.zib.scalaris.Transaction.ResultList, de.zib.scalaris.Transaction.RequestList, String, int)}
         * @param title
         *            (normalised) page name to update
         * @param firstOp
         *            position of the first operation in the result list
         * 
         * @return the result of the operation
         */
        public SaveResult updatePageLists_check_writes(
                Transaction.ResultList writeResults, String title, int firstOp) {
            final long timeAtStart = System.currentTimeMillis();
            // beware: keep order of operations in sync with readPageLists_prepare!
            // evaluate results changing the old and new page lists
            for (Set<String> curList : changes) {
                for (String name: curList) {
                    try {
                        writeResults.processWriteAt(firstOp++);
                    } catch (Exception e) {
                        return new SaveResult(false,
                                "unknown exception validating the addition of \""
                                        + title + "\" to \""
                                        + keyGen.getPageListKey(name)
                                        + "\" in Scalaris: " + e.getMessage(),
                                e instanceof ConnectionException,
                                System.currentTimeMillis() - timeAtStart);
                    }
                }
            }
            return new SaveResult(System.currentTimeMillis() - timeAtStart);
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
     * 
     * @return the result of the operation
     */
    protected static SaveResult updatePageList_prepare_write(
            Transaction.ResultList readResults,
            Transaction.RequestList writeRequests, String pageList_key,
            String pageCount_key, Collection<String> entriesToRemove,
            Collection<String> entriesToAdd, int firstOp) {
        final long timeAtStart = System.currentTimeMillis();
        List<String> pages;
        try {
            pages = readResults.processReadAt(firstOp++).stringListValue();
        } catch (NotFoundException e) {
            pages = new LinkedList<String>();
        } catch (Exception e) {
            return new SaveResult(false, "unknown exception updating \""
                    + pageList_key + "\" and \"" + pageCount_key
                    + "\" in Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException,
                    System.currentTimeMillis() - timeAtStart);
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
        SaveResult res = new SaveResult(System.currentTimeMillis() - timeAtStart);
        res.info = writeOps;
        return res;
    }
    
    /**
     * Updates a list of pages by removing and/or adding new page titles.
     * 
     * @param writeResults
     *            results of previous write operations by
     *            {@link #updatePageList_prepare_write(ResultList, RequestList, String, String, Collection, Collection, int)}
     * @param pageList_key
     *            Scalaris key for the page list
     * @param pageCount_key
     *            Scalaris key for the number of pages in the list (may be null
     *            if not used)
     * @param firstOp
     *            position of the first operation in the result list
     * @param writeOps
     *            number of supposed write operations
     * 
     * @return the result of the operation
     */
    protected static SaveResult updatePageList_check_writes(
            Transaction.ResultList writeResults, String pageList_key,
            String pageCount_key, int firstOp, int writeOps) {
        final long timeAtStart = System.currentTimeMillis();
        try {
            for (int i = 0; i < writeOps; ++i) {
                writeResults.processWriteAt(firstOp + i);
            }
        } catch (Exception e) {
            return new SaveResult(false, "unknown exception updating \""
                    + pageList_key + "\" and \"" + pageCount_key
                    + "\" in Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException,
                    System.currentTimeMillis() - timeAtStart);
        }
        return new SaveResult(System.currentTimeMillis() - timeAtStart);
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
     * 
     * @return the result of the operation
     */
    public static SaveResult updatePageList(Transaction scalaris_tx,
            String pageList_key, String pageCount_key,
            Collection<String> entriesToRemove, Collection<String> entriesToAdd) {
        final long timeAtStart = System.currentTimeMillis();
        Transaction.RequestList requests;
        Transaction.ResultList results;

        try {
            requests = new Transaction.RequestList();
            updatePageList_prepare_read(requests, pageList_key);
            results = scalaris_tx.req_list(requests);

            requests = new Transaction.RequestList();
            SaveResult result = updatePageList_prepare_write(results, requests,
                    pageList_key, pageCount_key, entriesToRemove, entriesToAdd,
                    0);
            if (!result.success) {
                return result;
            }
            requests.addCommit();
            results = scalaris_tx.req_list(requests);

            result = updatePageList_check_writes(results, pageList_key,
                    pageCount_key, 0, (Integer) result.info);
            if (!result.success) {
                return result;
            }
        } catch (Exception e) {
            return new SaveResult(false, "unknown exception updating \""
                    + pageList_key + "\" and \"" + pageCount_key
                    + "\" in Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException,
                    System.currentTimeMillis() - timeAtStart);
        }
        return new SaveResult(System.currentTimeMillis() - timeAtStart);
    }
}
