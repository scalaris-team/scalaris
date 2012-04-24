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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.ericsson.otp.erlang.OtpErlangString;

import de.zib.scalaris.Connection;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.ErlangValue;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.ScalarisVM;
import de.zib.scalaris.Transaction;
import de.zib.scalaris.TransactionSingleOp;
import de.zib.scalaris.UnknownException;
import de.zib.scalaris.examples.wikipedia.InvolvedKey.OP;
import de.zib.scalaris.examples.wikipedia.Options.STORE_CONTRIB_TYPE;
import de.zib.scalaris.examples.wikipedia.bliki.MyNamespace;
import de.zib.scalaris.examples.wikipedia.bliki.MyWikiModel;
import de.zib.scalaris.examples.wikipedia.data.Contribution;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.ShortRevision;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;
import de.zib.scalaris.operations.Operation;
import de.zib.scalaris.operations.ReadOp;

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
     * Gets the key to store the list of pages in the given namespace at.
     * 
     * @param namespace  the namespace ID
     * 
     * @return Scalaris key
     */
    public final static String getPageListKey(int namespace) {
        return "pages:" + namespace;
    }
    
    /**
     * Gets the key to store the number of pages at.
     * 
     * @param namespace  the namespace ID
     * 
     * @return Scalaris key
     */
    public final static String getPageCountKey(int namespace) {
        return getPageListKey(namespace) + ":count";
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
     * Gets the key to store the list of contributions of a user.
     * 
     * @param contributor  the user name or IP address of the user who created
     *                     the revision
     * 
     * @return Scalaris key
     */
    public final static String getContributionListKey(String contributor) {
        return contributor + ":user:contrib";
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
        List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
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
        List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
        if (connection == null) {
            return new PageHistoryResult(false, involvedKeys, "no connection to Scalaris",
                    true, statName, System.currentTimeMillis() - timeAtStart);
        }
        
        TransactionSingleOp scalaris_single = new TransactionSingleOp(connection);
        TransactionSingleOp.RequestList requests = new TransactionSingleOp.RequestList();
        requests.addOp(new ReadOp(getPageKey(title, nsObject)));
        requests.addOp(new ReadOp(getRevListKey(title, nsObject)));
        
        TransactionSingleOp.ResultList results;
        try {
            ScalarisDataHandler.addInvolvedKeys(involvedKeys, requests.getRequests());
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
        List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
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
            involvedKeys.add(new InvolvedKey(OP.READ, scalaris_key));
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
                involvedKeys.add(new InvolvedKey(OP.READ, scalaris_key));
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
     * Retrieves a list of all available pages from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * 
     * @return a result object with the page list on success
     */
    public static ValueResult<List<String>> getPageList(Connection connection) {
        ArrayList<String> scalaris_keys = new ArrayList<String>(
                MyNamespace.MAX_NAMESPACE_ID - MyNamespace.MIN_NAMESPACE_ID + 1);
        for (int i = MyNamespace.MIN_NAMESPACE_ID; i < MyNamespace.MAX_NAMESPACE_ID; ++i) {
            scalaris_keys.add(getPageListKey(i));
        }
        return getPageList2(connection, ScalarisOpType.PAGE_LIST,
                scalaris_keys, false, "page list");
    }

    /**
     * Retrieves a list of available pages in the given namespace from Scalaris.
     * 
     * @param namespace
     *            the namespace ID
     * @param connection
     *            the connection to Scalaris
     * 
     * @return a result object with the page list on success
     */
    public static ValueResult<List<String>> getPageList(int namespace, Connection connection) {
        return getPageList2(connection, ScalarisOpType.PAGE_LIST,
                Arrays.asList(getPageListKey(namespace)), false, "page list:" + namespace);
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
        return getPageList2(connection, ScalarisOpType.CATEGORY_PAGE_LIST,
                Arrays.asList(getCatPageListKey(title, nsObject)), false,
                "pages in " + title);
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
        return getPageList2(connection, ScalarisOpType.TEMPLATE_PAGE_LIST,
                Arrays.asList(getTplPageListKey(title, nsObject)), true,
                "pages in " + title);
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
        if (Options.getInstance().WIKI_USE_BACKLINKS) {
            return getPageList2(connection, ScalarisOpType.BACKLINK_PAGE_LIST,
                    Arrays.asList(getBackLinksPageListKey(title, nsObject)),
                    false, statName);
        } else {
            return new ValueResult<List<String>>(new ArrayList<InvolvedKey>(0),
                    new ArrayList<String>(0));
        }
    }

    /**
     * Retrieves a list of pages linking to the given page from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param contributor
     *            the user name or IP address of the user who created the
     *            revision
     * 
     * @return a result object with the page list on success
     */
    public static ValueResult<List<Contribution>> getContributions(
            Connection connection, String contributor) {
        final String statName = "contributions of " + contributor;
        if (Options.getInstance().WIKI_STORE_CONTRIBUTIONS != STORE_CONTRIB_TYPE.NONE) {
            ValueResult<List<Contribution>> result = getPageList3(connection,
                    ScalarisOpType.CONTRIBUTION,
                    Arrays.asList(getContributionListKey(contributor)), false,
                    statName, new ErlangConverter<List<Contribution>>() {
                        @Override
                        public List<Contribution> convert(ErlangValue v)
                                throws ClassCastException {
                            return v.jsonListValue(Contribution.class);
                        }
                    });
            if (result.success && result.value == null) {
                result.value = new ArrayList<Contribution>(0);
            }
            return result;
        } else {
            return new ValueResult<List<Contribution>>(
                    new ArrayList<InvolvedKey>(0), new ArrayList<Contribution>(0));
        }
    }

    /**
     * Retrieves a list of pages from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param opType
     *            operation type indicating what is being read
     * @param scalaris_keys
     *            the keys under which the page list is stored in Scalaris
     * @param failNotFound
     *            whether the operation should fail if the key is not found or
     *            not
     * @param statName
     *            name for the time measurement statistics
     * 
     * @return a result object with the page list on success
     */
    private static ValueResult<List<String>> getPageList2(
            Connection connection, ScalarisOpType opType,
            Collection<String> scalaris_keys, boolean failNotFound,
            String statName) {
        ValueResult<List<String>> result = getPageList3(connection, opType,
                scalaris_keys, failNotFound, statName,
                new ErlangConverter<List<String>>() {
                    @Override
                    public List<String> convert(ErlangValue v)
                            throws ClassCastException {
                        return v.stringListValue();
                    }
                });
        if (result.success && result.value == null) {
            result.value = new ArrayList<String>(0);
        }
        return result;
    }

    /**
     * Retrieves a list of pages from Scalaris.
     * @param <T>
     * 
     * @param connection
     *            the connection to Scalaris
     * @param opType
     *            operation type indicating what is being read
     * @param scalaris_keys
     *            the keys under which the page list is stored in Scalaris
     * @param failNotFound
     *            whether the operation should fail if the key is not found or
     *            not (the value contains null if not failed!)
     * @param statName
     *            name for the time measurement statistics
     * 
     * @return a result object with the page list on success
     */
    private static <T> ValueResult<List<T>> getPageList3(Connection connection,
            ScalarisOpType opType, Collection<String> scalaris_keys,
            boolean failNotFound, String statName, ErlangConverter<List<T>> conv) {
        final long timeAtStart = System.currentTimeMillis();
        List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
        
        if (connection == null) {
            return new ValueResult<List<T>>(false, involvedKeys,
                    "no connection to Scalaris", true, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        
        final MyScalarisSingleOpExecutor executor = new MyScalarisSingleOpExecutor(
                new TransactionSingleOp(connection), involvedKeys);

        final ScalarisReadListOp1<T> readOp = new ScalarisReadListOp1<T>(scalaris_keys,
                Options.getInstance().OPTIMISATIONS.get(opType), conv, failNotFound);
        executor.addOp(readOp);
        try {
            executor.run();
        } catch (Exception e) {
            return new ValueResult<List<T>>(false, involvedKeys,
                    "unknown exception reading page list at \""
                            + involvedKeys.toString() + "\" from Scalaris: "
                            + e.getMessage(), e instanceof ConnectionException,
                    statName, System.currentTimeMillis() - timeAtStart);
        }
        
        return new ValueResult<List<T>>(involvedKeys, readOp.getValue(), statName,
                System.currentTimeMillis() - timeAtStart);
    }

    /**
     * Retrieves the number of all available pages from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * 
     * @return a result object with the number of pages on success
     */
    public static ValueResult<BigInteger> getPageCount(Connection connection) {
        ArrayList<String> scalaris_keys = new ArrayList<String>(
                MyNamespace.MAX_NAMESPACE_ID - MyNamespace.MIN_NAMESPACE_ID + 1);
        for (int i = MyNamespace.MIN_NAMESPACE_ID; i < MyNamespace.MAX_NAMESPACE_ID; ++i) {
            scalaris_keys.add(getPageCountKey(i));
        }
        return getInteger2(connection, scalaris_keys, false, "page count");
    }

    /**
     * Retrieves the number of available pages in the given namespace from
     * Scalaris.
     * 
     * @param namespace
     *            the namespace ID
     * @param connection
     *            the connection to Scalaris
     * 
     * @return a result object with the number of pages on success
     */
    public static ValueResult<BigInteger> getPageCount(int namespace, Connection connection) {
        return getInteger2(connection, getPageCountKey(namespace), false, "page count:" + namespace);
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
        return getInteger2(connection, Arrays.asList(scalaris_key), failNotFound, statName);
    }

    /**
     * Retrieves an integral number from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param scalaris_keys
     *            the keys under which the number is stored in Scalaris
     * @param failNotFound
     *            whether the operation should fail if the key is not found or
     *            not
     * @param statName
     *            name for the time measurement statistics
     * 
     * @return a result object with the number on success
     */
    private static ValueResult<BigInteger> getInteger2(Connection connection,
            Collection<String> scalaris_keys, boolean failNotFound, String statName) {
        final long timeAtStart = System.currentTimeMillis();
        List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
        if (connection == null) {
            return new ValueResult<BigInteger>(false, involvedKeys,
                    "no connection to Scalaris", true, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        
        TransactionSingleOp scalaris_single = new TransactionSingleOp(connection);
        TransactionSingleOp.ResultList results;
        try {
            TransactionSingleOp.RequestList requests = new TransactionSingleOp.RequestList();
            for (String scalaris_key : scalaris_keys) {
                involvedKeys.add(new InvolvedKey(OP.READ, scalaris_key));
                requests.addOp(new ReadOp(scalaris_key));
            }
            results = scalaris_single.req_list(requests);
        } catch (Exception e) {
            return new ValueResult<BigInteger>(false, involvedKeys,
                    "unknown exception reading (integral) number(s) at \""
                            + scalaris_keys.toString() + "\" from Scalaris: "
                            + e.getMessage(), e instanceof ConnectionException,
                    statName, System.currentTimeMillis() - timeAtStart);
        }
        BigInteger number = BigInteger.ZERO;
        int curOp = 0;
        for (String scalaris_key : scalaris_keys) {
            try {
                number = number.add(results.processReadAt(curOp++).bigIntValue());
            } catch (NotFoundException e) {
                if (failNotFound) {
                    return new ValueResult<BigInteger>(false, involvedKeys,
                            "unknown exception reading (integral) number at \""
                                    + scalaris_key + "\" from Scalaris: "
                                    + e.getMessage(), false, statName,
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
        return new ValueResult<BigInteger>(involvedKeys, number, statName,
                System.currentTimeMillis() - timeAtStart);
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
        ValueResult<List<ErlangValue>> result = getPageList3(connection,
                ScalarisOpType.PAGE_LIST, Arrays.asList(getPageListKey(0)),
                true, "random article",
                new ErlangConverter<List<ErlangValue>>() {
                    @Override
                    public List<ErlangValue> convert(ErlangValue v)
                            throws ClassCastException {
                        return v.listValue();
                    }
                });
        if (result.success) {
            String randomTitle = result.value.get(
                    random.nextInt(result.value.size())).stringValue();
            return new ValueResult<String>(result.involvedKeys, randomTitle);
        } else {
            return new ValueResult<String>(false, result.involvedKeys,
                    result.message, result.connect_failed);
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
        List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
        if (connection == null) {
            return new SavePageResult(false, involvedKeys,
                    "no connection to Scalaris", true, oldPage, newPage,
                    newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        
        String title;
        Integer namespace;
        do {
            String[] parts = MyWikiModel.splitNsTitle(title0, nsObject);
            namespace = nsObject.getNumberByName(parts[0]);
            title = MyWikiModel.createFullPageName(namespace.toString(),
                    MyWikiModel.normaliseName(parts[1]));
        } while (false);
        Transaction scalaris_tx = new Transaction(connection);

        // check that the current version is still up-to-date:
        // read old version first, then write
        String pageInfoKey = getPageKey(title0, nsObject);
        
        Transaction.RequestList requests = new Transaction.RequestList();
        requests.addOp(new ReadOp(pageInfoKey));
        
        Transaction.ResultList results;
        try {
            ScalarisDataHandler.addInvolvedKeys(involvedKeys, requests.getRequests());
            results = scalaris_tx.req_list(requests);
        } catch (Exception e) {
            return new SavePageResult(false, involvedKeys,
                    "unknown exception getting page info (" + pageInfoKey
                            + ") from Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, oldPage, newPage,
                    newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }

        int oldRevId;
        try {
            oldPage = results.processReadAt(0).jsonValue(Page.class);
            newPage = new Page(oldPage.getTitle(), oldPage.getId(),
                    oldPage.isRedirect(), new LinkedHashMap<String, String>(
                            oldPage.getRestrictions()), newRev);
            oldRevId = oldPage.getCurRev().getId();
        } catch (NotFoundException e) {
            // this is ok and means that the page did not exist yet
            newPage = new Page(title0, 1, false,
                    new LinkedHashMap<String, String>(), newRev);
            oldRevId = 0;
        } catch (Exception e) {
            return new SavePageResult(false, involvedKeys,
                    "unknown exception reading \"" + pageInfoKey
                            + "\" from Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, oldPage, newPage,
                    newShortRevs, pageEdits, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        newRev.setId(oldRevId + 1);
        
        if (!newPage.checkEditAllowed(username)) {
            return new SavePageResult(false, involvedKeys,
                    "operation not allowed: edit is restricted", false,
                    oldPage, newPage, newShortRevs, pageEdits,
                    statName, System.currentTimeMillis() - timeAtStart);
        }
        
        /*
         * if prevRevId is greater than 0, it must match the old revision,
         * if it is -1, then there should not be an old page
         */
        if ((prevRevId > 0 && prevRevId != oldRevId) || (prevRevId == -1 && oldPage != null)) {
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
        if (oldPage != null && oldPage.getCurRev() != null) {
            // get a list of previous categories and templates:
            wikiModel.setUp();
            final long timeAtRenderStart = System.currentTimeMillis();
            wikiModel.render(null, oldPage.getCurRev().unpackedText());
            timeAtStart -= (System.currentTimeMillis() - timeAtRenderStart);
            // note: no need to normalise the pages, we will do so during the write/read key generation
            oldCats = wikiModel.getCategories().keySet();
            oldTpls = wikiModel.getTemplates();
            if (Options.getInstance().WIKI_USE_BACKLINKS) {
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
                }, ScalarisOpType.CATEGORY_PAGE_LIST);
        final Set<String> newTpls = wikiModel.getTemplates();
        Difference tplDiff = new Difference(oldTpls, newTpls,
                new Difference.GetPageListKey() {
                    @Override
                    public String getPageListKey(String name) {
                        return getTplPageListKey(
                                wikiModel.getTemplateNamespace() + ":" + name,
                                nsObject);
                    }
                }, ScalarisOpType.TEMPLATE_PAGE_LIST);
        // use empty link lists to turn back-links off
        final Set<String> newLnks = Options.getInstance().WIKI_USE_BACKLINKS ? wikiModel.getLinks() : new HashSet<String>();
        Difference lnkDiff = new Difference(oldLnks, newLnks,
                new Difference.GetPageListKey() {
                    @Override
                    public String getPageListKey(String name) {
                        return getBackLinksPageListKey(name, nsObject);
                    }
                }, ScalarisOpType.BACKLINK_PAGE_LIST);
        

        // now save the changes:
        do {
            final MyScalarisTxOpExecutor executor0 = new MyScalarisTxOpExecutor(
                    scalaris_tx, involvedKeys);
            executor0.setCommitLast(true);
            MyScalarisOpExecWrapper executor = new MyScalarisOpExecWrapper(
                    executor0);

            int articleCountChange = 0;
            final boolean wasArticle = (oldPage != null)
                    && MyWikiModel.isArticle(namespace, oldLnks, oldCats);
            final boolean isArticle = (namespace == 0)
                    && MyWikiModel.isArticle(namespace, newLnks, newCats);
            if (wasArticle == isArticle) {
                articleCountChange = 0;
            } else if (!wasArticle) {
                articleCountChange = 1;
            } else if (!isArticle) {
                articleCountChange = -1;
            }

            //  PAGE LISTS UPDATE, step 1: append to / remove from old lists
            executor.addAppend(ScalarisOpType.SHORTREV_LIST, getRevListKey(title0, nsObject), new ShortRevision(newRev), null);
            if (articleCountChange != 0) {
                executor.addIncrement(ScalarisOpType.ARTICLE_COUNT, getArticleCountKey(), articleCountChange);
            }

            // write differences (categories, templates, backlinks)
            catDiff.addScalarisOps(executor, title);
            tplDiff.addScalarisOps(executor, title);
            lnkDiff.addScalarisOps(executor, title);

            // new page? -> add to page/article lists
            if (oldPage == null) {
                final String pageListKey = getPageListKey(namespace);
                final String pageCountKey = getPageCountKey(namespace);
                executor.addAppend(ScalarisOpType.PAGE_LIST, pageListKey, title, pageCountKey);
            }

            executor.addWrite(ScalarisOpType.PAGE, getPageKey(title0, nsObject), newPage);
            executor.addWrite(ScalarisOpType.REVISION, getRevKey(title0, newRev.getId(), nsObject), newRev);

            //  PAGE LISTS UPDATE, step 2: execute and evaluate operations
            try {
                executor.getExecutor().run();
            } catch (Exception e) {
                return new SavePageResult(false, involvedKeys,
                        "unknown exception writing page \"" + title0
                        + "\" to Scalaris: " + e.getMessage(),
                        e instanceof ConnectionException, oldPage, newPage,
                        newShortRevs, pageEdits, statName,
                        System.currentTimeMillis() - timeAtStart);
            }
        } while (false);
        
        if (Options.getInstance().WIKI_STORE_CONTRIBUTIONS == STORE_CONTRIB_TYPE.OUTSIDE_TX) {
            addContribution(scalaris_tx, oldPage, newPage, involvedKeys);
        }
        
        increasePageEditStat(scalaris_tx, involvedKeys);
        
        return new SavePageResult(involvedKeys, oldPage, newPage, newShortRevs,
                pageEdits, statName, System.currentTimeMillis() - timeAtStart);
    }
    
    /**
     * Increases the number of overall page edits statistic.
     * 
     * @param scalaris_tx
     *            the transaction object to use
     * @param involvedKeys
     *            all keys that have been read or written during the operation
     */
    private static void increasePageEditStat(
            Transaction scalaris_tx, List<InvolvedKey> involvedKeys) {
        // increase number of page edits (for statistics)
        // as this is not that important, use a separate transaction and do not
        // fail if updating the value fails
        final MyScalarisTxOpExecutor executor0 = new MyScalarisTxOpExecutor(
                scalaris_tx, involvedKeys);
        executor0.setCommitLast(true);
        MyScalarisOpExecWrapper executor = new MyScalarisOpExecWrapper(
                executor0);
        
        executor.addIncrement(ScalarisOpType.EDIT_STAT, getStatsPageEditsKey(), 1);
        try {
            executor.getExecutor().run();
        } catch (Exception e) {
        }
    }

    /**
     * Adds a contribution to the list of contributions of the user.
     * 
     * @param scalaris_tx
     *            the transaction object to use
     * @param oldPage
     *            the old page object or <tt>null</tt> if there was no old page
     * @param newPage
     *            the newly created page object
     * @param involvedKeys
     *            all keys that have been read or written during the operation
     */
    private static void addContribution(
            Transaction scalaris_tx, Page oldPage, Page newPage, List<InvolvedKey> involvedKeys) {
        // as this is not that important, use a separate transaction and do not
        // fail if updating the value fails
        final MyScalarisTxOpExecutor executor0 = new MyScalarisTxOpExecutor(
                scalaris_tx, involvedKeys);
        executor0.setCommitLast(true);
        MyScalarisOpExecWrapper executor = new MyScalarisOpExecWrapper(
                executor0);

        String scalaris_key = getContributionListKey(newPage.getCurRev().getContributor().toString());
        executor.addAppend(ScalarisOpType.CONTRIBUTION, scalaris_key,
                Arrays.asList(new Contribution(oldPage, newPage)), null);
        try {
            executor.getExecutor().run();
        } catch (Exception e) {
        }
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
        final private ScalarisOpType opType;
        
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
         * @param opType
         *            operation type indicating what is being updated
         */
        public Difference(Set<String> oldSet, Set<String> newSet,
                GetPageListKey keyGen, ScalarisOpType opType) {
            this.onlyOld = new HashSet<String>(oldSet);
            this.onlyNew = new HashSet<String>(newSet);
            this.onlyOld.removeAll(newSet);
            this.onlyNew.removeAll(oldSet);
            this.changes[0] = this.onlyOld;
            this.changes[1] = this.onlyNew;
            this.keyGen = keyGen;
            this.opType = opType;
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
         * Adds the appropriate list append operations to the given executor.
         * 
         * @param executor  executor performing the Scalaris operations
         * @param title     (normalised) page name to update
         */
        public void addScalarisOps(MyScalarisOpExecWrapper executor,
                String title) {
            String scalaris_key;
            GetPageListAndCountKey keyCountGen = null;
            if (keyGen instanceof GetPageListAndCountKey) {
                keyCountGen = (GetPageListAndCountKey) keyGen;
            }
            // remove from old page list
            for (String name: onlyOld) {
                scalaris_key = keyGen.getPageListKey(name);
//                System.out.println(scalaris_key + " -= " + title);
                String scalaris_countKey = keyCountGen == null ? null : keyCountGen.getPageCountKey(name);
                executor.addRemove(opType, scalaris_key, title, scalaris_countKey);
            }
            // add to new page list
            for (String name: onlyNew) {
                scalaris_key = keyGen.getPageListKey(name);
//              System.out.println(scalaris_key + " += " + title);
                String scalaris_countKey = keyCountGen == null ? null : keyCountGen.getPageCountKey(name);
                executor.addAppend(opType, scalaris_key, title, scalaris_countKey);
            }
        }
    }
    
    /**
     * Updates a list of pages by removing and/or adding new page titles.
     * 
     * @param scalaris_tx
     *            connection to Scalaris
     * @param opType
     *            operation type indicating what is being updated
     * @param pageList_key
     *            Scalaris key for the page list
     * @param pageCount_key
     *            Scalaris key for the number of pages in the list (may be null
     *            if not used)
     * @param entriesToAdd
     *            list of (normalised) page names to add to the list
     * @param entriesToRemove
     *            list of (normalised) page names to remove from the list
     * @param statName
     *            name for the time measurement statistics
     * 
     * @return the result of the operation
     */
    public static ValueResult<Integer> updatePageList(Transaction scalaris_tx,
            ScalarisOpType opType, String pageList_key, String pageCount_key,
            List<String> entriesToAdd, List<String> entriesToRemove,
            final String statName) {
        final long timeAtStart = System.currentTimeMillis();
        List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();

        try {
            final MyScalarisTxOpExecutor executor0 = new MyScalarisTxOpExecutor(
                    scalaris_tx, involvedKeys);
            executor0.setCommitLast(true);
            MyScalarisOpExecWrapper executor = new MyScalarisOpExecWrapper(
                    executor0);

            executor.addAppendRemove(opType, pageList_key, entriesToAdd,
                    entriesToRemove, pageCount_key);
            
            executor.getExecutor().run();
            return new ValueResult<Integer>(involvedKeys, null, statName,
                    System.currentTimeMillis() - timeAtStart);
        } catch (Exception e) {
            return new ValueResult<Integer>(false, involvedKeys,
                    "unknown exception updating \"" + pageList_key
                            + "\" and \"" + pageCount_key + "\" in Scalaris: "
                            + e.getMessage(), e instanceof ConnectionException,
                    statName, System.currentTimeMillis() - timeAtStart);
        }
    }

    /**
     * Adds all keys from the given operation list to the list of involved keys.
     * 
     * @param involvedKeys
     *            list of involved keys
     * @param ops
     *            new operations
     */
    public static void addInvolvedKeys(List<InvolvedKey> involvedKeys, Collection<? extends Operation> ops) {
        assert involvedKeys != null;
        assert ops != null;
        for (Operation op : ops) {
            final OtpErlangString key = op.getKey();
            if (key != null) {
                if (op instanceof ReadOp) {
                    involvedKeys.add(new InvolvedKey(InvolvedKey.OP.READ, key.stringValue()));
                } else {
                    involvedKeys.add(new InvolvedKey(InvolvedKey.OP.WRITE, key.stringValue()));
                }
            }
        }
    }
}
