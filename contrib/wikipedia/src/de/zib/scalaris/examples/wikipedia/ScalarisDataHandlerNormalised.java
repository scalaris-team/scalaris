/**
 *  Copyright 2012-2013 Zuse Institute Berlin
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
import java.util.HashMap;
import java.util.List;

import de.zib.scalaris.Connection;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.Transaction;
import de.zib.scalaris.TransactionSingleOp;
import de.zib.scalaris.examples.wikipedia.InvolvedKey.OP;
import de.zib.scalaris.examples.wikipedia.bliki.NormalisedTitle;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.ShortRevision;
import de.zib.scalaris.executor.ScalarisOp;
import de.zib.scalaris.executor.ScalarisReadOp;
import de.zib.scalaris.operations.ReadOp;

/**
 * @author Nico Kruber, kruber@zib.de
 */
public class ScalarisDataHandlerNormalised extends ScalarisDataHandler {
    
    /**
     * Gets the key to store {@link Revision} objects at.
     * 
     * @param title     the title of the page
     * @param id        the id of the revision
     * 
     * @return Scalaris key
     */
    public final static String getRevKey(NormalisedTitle title, int id) {
        return title + ":rev:" + id;
    }
    
    /**
     * Gets the key to store {@link Page} objects at.
     * 
     * @param title     the title of the page
     * 
     * @return Scalaris key
     */
    public final static String getPageKey(NormalisedTitle title) {
        return title + ":page";
    }
    
    /**
     * Gets the key to store the list of revisions of a page at.
     * 
     * @param title     the title of the page
     * 
     * @return Scalaris key
     */
    public final static String getRevListKey(NormalisedTitle title) {
        return title + ":revs";
    }
    
    /**
     * Gets the key to store the list of pages belonging to a category at.
     * 
     * @param title     the category title (including <tt>Category:</tt>)
     * 
     * @return Scalaris key
     */
    public final static String getCatPageListKey(NormalisedTitle title) {
        return title + ":cpages";
    }
    
    /**
     * Gets the key to store the number of pages belonging to a category at.
     * 
     * @param title     the category title (including <tt>Category:</tt>)
     * 
     * @return Scalaris key
     */
    public final static String getCatPageCountKey(NormalisedTitle title) {
        return title + ":cpages:count";
    }
    
    /**
     * Gets the key to store the list of pages using a template at.
     * 
     * @param title     the template title (including <tt>Template:</tt>)
     * 
     * @return Scalaris key
     */
    public final static String getTplPageListKey(NormalisedTitle title) {
        return title + ":tpages";
    }
    
    /**
     * Gets the key to store the list of pages linking to the given title.
     * 
     * @param title     the page's title
     * 
     * @return Scalaris key
     */
    public final static String getBackLinksPageListKey(NormalisedTitle title) {
        return title + ":blpages";
    }
    
    /**
     * Retrieves a page's history from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param title
     *            the title of the page
     * 
     * @return a result object with the page history on success
     */
    public static PageHistoryResult getPageHistory(Connection connection,
            NormalisedTitle title) {
        final long timeAtStart = System.currentTimeMillis();
        final String statName = "HISTORY:" + title;
        List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
        if (connection == null) {
            return new PageHistoryResult(false, involvedKeys, "no connection to Scalaris",
                    true, statName, System.currentTimeMillis() - timeAtStart);
        }
        
        TransactionSingleOp scalaris_single = new TransactionSingleOp(connection);
        TransactionSingleOp.RequestList requests = new TransactionSingleOp.RequestList();
        requests.addOp(new ReadOp(getPageKey(title)));
        requests.addOp(new ReadOp(getRevListKey(title)));
        
        TransactionSingleOp.ResultList results;
        try {
            addInvolvedKeys(involvedKeys, requests.getRequests());
            results = scalaris_single.req_list(requests);
        } catch (Exception e) {
            return new PageHistoryResult(false, involvedKeys,
                    e.getClass().getCanonicalName() + " reading \"" + getPageKey(title)
                            + "\" or \"" + getRevListKey(title)
                            + "\" from Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        
        Page page;
        try {
            page = results.processReadAt(0).jsonValue(Page.class);
        } catch (NotFoundException e) {
            PageHistoryResult result = new PageHistoryResult(false,
                    involvedKeys, "page not found at \"" + getPageKey(title)
                            + "\"", false, statName, System.currentTimeMillis()
                            - timeAtStart);
            result.not_existing = true;
            return result;
        } catch (Exception e) {
            return new PageHistoryResult(false, involvedKeys,
                    e.getClass().getCanonicalName() + " reading \"" + getPageKey(title)
                            + "\" from Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, statName,
                    System.currentTimeMillis() - timeAtStart);
        }

        List<ShortRevision> revisions;
        try {
            revisions = results.processReadAt(1).jsonListValue(ShortRevision.class);
        } catch (NotFoundException e) {
            PageHistoryResult result = new PageHistoryResult(false,
                    involvedKeys, "revision list \"" + getRevListKey(title)
                            + "\" does not exist", false, statName,
                    System.currentTimeMillis() - timeAtStart);
            result.not_existing = true;
            return result;
        } catch (Exception e) {
            return new PageHistoryResult(false, involvedKeys,
                    e.getClass().getCanonicalName() + " reading \"" + getRevListKey(title)
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
     * 
     * @return a result object with the page and revision on success
     */
    public static RevisionResult getRevision(Connection connection,
            NormalisedTitle title) {
        return getRevision(connection, title, -1);
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
     * 
     * @return a result object with the page and revision on success
     */
    public static RevisionResult getRevision(Connection connection,
            NormalisedTitle title, int id) {
        final long timeAtStart = System.currentTimeMillis();
        Page page = null;
        Revision revision = null;
        List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
        final String statName = "PAGE:" + title.toString();
        if (connection == null) {
            return new RevisionResult(false, involvedKeys,
                    "no connection to Scalaris", true, title, page, revision, false,
                    false, statName, System.currentTimeMillis() - timeAtStart);
        }
        
        TransactionSingleOp scalaris_single;
        String scalaris_key;
        
        scalaris_single = new TransactionSingleOp(connection);

        scalaris_key = getPageKey(title);
        try {
            involvedKeys.add(new InvolvedKey(OP.READ, scalaris_key));
            page = scalaris_single.read(scalaris_key).jsonValue(Page.class);
        } catch (NotFoundException e) {
            return new RevisionResult(false, involvedKeys,
                    "page not found at \"" + scalaris_key + "\"", false, title,
                    page, revision, true, false, statName,
                    System.currentTimeMillis() - timeAtStart);
        } catch (Exception e) {
            return new RevisionResult(false, involvedKeys,
                    e.getClass().getCanonicalName() + " reading \"" + scalaris_key
                            + "\" from Scalaris: " + e.getMessage(),
                    e instanceof ConnectionException, title, page, revision, false,
                    false, statName, System.currentTimeMillis() - timeAtStart);
        }

        // load requested version if it is not the current one cached in the Page object
        if (id != page.getCurRev().getId() && id >= 0) {
            scalaris_key = getRevKey(title, id);
            try {
                involvedKeys.add(new InvolvedKey(OP.READ, scalaris_key));
                revision = scalaris_single.read(scalaris_key).jsonValue(Revision.class);
            } catch (NotFoundException e) {
                return new RevisionResult(false, involvedKeys,
                        "revision not found at \"" + scalaris_key + "\"",
                        false, title, page, revision, false, true, statName,
                        System.currentTimeMillis() - timeAtStart);
            } catch (Exception e) {
                return new RevisionResult(false, involvedKeys,
                        e.getClass().getCanonicalName() + " reading \"" + scalaris_key
                                + "\" from Scalaris: " + e.getMessage(),
                        e instanceof ConnectionException, title, page,
                        revision, false, false, statName,
                        System.currentTimeMillis() - timeAtStart);
            }
        } else {
            revision = page.getCurRev();
        }

        return new RevisionResult(involvedKeys, title, page, revision, statName,
                System.currentTimeMillis() - timeAtStart);
    }

    /**
     * Retrieves the current version of all given pages from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param titles
     *            the titles of the pages
     * @param statName
     *            name of the statistic to collect
     * 
     * @return a result object with the pages and revisions on success
     */
    public static ValueResult<List<RevisionResult>> getRevisions(Connection connection,
            Collection<NormalisedTitle> titles, final String statName) {
        final long timeAtStart = System.currentTimeMillis();
        List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
        if (connection == null) {
            return new ValueResult<List<RevisionResult>>(false, involvedKeys,
                    "no connection to Scalaris", true, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        
        TransactionSingleOp scalaris_single = new TransactionSingleOp(connection);
        final MyScalarisSingleOpExecutor executor0 = new MyScalarisSingleOpExecutor(
                scalaris_single, involvedKeys);
        HashMap<ScalarisReadOp, NormalisedTitle> opToTitleN = new HashMap<ScalarisReadOp, NormalisedTitle>(titles.size());
        for (NormalisedTitle title : titles) {
            ScalarisReadOp readOp = new ScalarisReadOp(getPageKey(title));
            executor0.addOp(readOp);
            opToTitleN.put(readOp, title);
        }
        try {
            executor0.run();
        } catch (Exception e) {
            return new ValueResult<List<RevisionResult>>(false, involvedKeys,
                    e.getClass().getCanonicalName() + " reading " + titles + " from Scalaris",
                    e instanceof ConnectionException, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        
        List<RevisionResult> results = new ArrayList<RevisionResult>(titles.size());
        for (ScalarisOp op : executor0.getOps()) {
            if (op instanceof ScalarisReadOp) {
                ScalarisReadOp readOp = (ScalarisReadOp) op;
                RevisionResult curResult;
                if (readOp.getValue() != null) {
                    Page page = readOp.getValue().jsonValue(Page.class);
                    curResult = new RevisionResult(involvedKeys,
                            opToTitleN.get(readOp), page, page.getCurRev());
                    
                } else {
                    curResult = new RevisionResult(false, involvedKeys,
                            "page not found at \"" + readOp.getKey() + "\"",
                            false, opToTitleN.get(readOp), null, null, true, false);
                }
                results.add(curResult);
            }
        }
        return new ValueResult<List<RevisionResult>>(involvedKeys, results,
                statName, System.currentTimeMillis() - timeAtStart);
    }

    /**
     * Retrieves a list of pages in the given category from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param title
     *            the title of the category
     * 
     * @return a result object with the page list on success
     */
    public static ValueResult<List<NormalisedTitle>> getPagesInCategory(Connection connection,
            NormalisedTitle title) {
        final long timeAtStart = System.currentTimeMillis();
        final String statName = "CAT_LIST:" + title;
        return getPageList2(connection, ScalarisOpType.CATEGORY_PAGE_LIST,
                Arrays.asList(getCatPageListKey(title)), false,
                timeAtStart, statName);
    }

    /**
     * Retrieves a list of pages using the given template from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param title
     *            the title of the template
     * 
     * @return a result object with the page list on success
     */
    public static ValueResult<List<NormalisedTitle>> getPagesInTemplate(Connection connection,
            NormalisedTitle title) {
        final long timeAtStart = System.currentTimeMillis();
        final String statName = "TPL_LIST:" + title;
        return getPageList2(connection, ScalarisOpType.TEMPLATE_PAGE_LIST,
                Arrays.asList(getTplPageListKey(title)), false,
                timeAtStart, statName);
    }

    /**
     * Retrieves a list of pages using the given templates from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param titles
     *            the titles of the templates
     * @param pageTitle
     *            the title of the page to retrieve the list for (will be
     *            included in the statname)
     * 
     * @return a result object with the page list on success
     */
    public static ValueResult<List<NormalisedTitle>> getPagesInTemplates(Connection connection,
            List<NormalisedTitle> titles, String pageTitle) {
        final long timeAtStart = System.currentTimeMillis();
        final String statName = "TPL_LISTS_FOR:" + pageTitle;
        ArrayList<String> scalarisKeys = new ArrayList<String>(titles.size());
        for (NormalisedTitle title : titles) {
            scalarisKeys.add(getTplPageListKey(title));
        }
        return getPageList2(connection, ScalarisOpType.TEMPLATE_PAGE_LIST,
                scalarisKeys, false, timeAtStart, statName);
    }

    /**
     * Retrieves a list of pages linking to the given page from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param title
     *            the title of the page
     * 
     * @return a result object with the page list on success
     */
    public static ValueResult<List<NormalisedTitle>> getPagesLinkingTo(Connection connection,
            NormalisedTitle title) {
        final long timeAtStart = System.currentTimeMillis();
        final String statName = "LINKS:" + title;
        if (Options.getInstance().WIKI_USE_BACKLINKS) {
            return getPageList2(connection, ScalarisOpType.BACKLINK_PAGE_LIST,
                    Arrays.asList(getBackLinksPageListKey(title)),
                    false, timeAtStart, statName);
        } else {
            return new ValueResult<List<NormalisedTitle>>(new ArrayList<InvolvedKey>(0),
                    new ArrayList<NormalisedTitle>(0));
        }
    }

    /**
     * Retrieves the number of pages in the given category from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param title
     *            the title of the category
     * 
     * @return a result object with the number of pages on success
     */
    public static ValueResult<BigInteger> getPagesInCategoryCount(
            Connection connection, NormalisedTitle title) {
        final long timeAtStart = System.currentTimeMillis();
        final String statName = "CAT_CNT:" + title;
        return getInteger2(connection, ScalarisOpType.CATEGORY_PAGE_COUNT,
                getCatPageCountKey(title), false, timeAtStart,
                statName);
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
     * @param countOpType
     *            operation type indicating what is being updated for
     *            updating the count key (if there is no count key, this may
     *            be <tt>null</tt>)
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
            ScalarisOpType opType, String pageList_key,
            ScalarisOpType countOpType, String pageCount_key,
            List<NormalisedTitle> entriesToAdd, List<NormalisedTitle> entriesToRemove,
            final String statName) {
        final long timeAtStart = System.currentTimeMillis();
        List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();

        try {
            final MyScalarisTxOpExecutor executor0 = new MyScalarisTxOpExecutor(
                    scalaris_tx, involvedKeys);
            executor0.setCommitLast(true);
            MyScalarisOpExecWrapper executor = new MyScalarisOpExecWrapper(
                    executor0);

            executor.addAppendRemove(opType, pageList_key,
                    normList2normStringList(entriesToAdd),
                    normList2normStringList(entriesToRemove),
                    countOpType, pageCount_key);
            
            executor.getExecutor().run();
            return new ValueResult<Integer>(involvedKeys, null, statName,
                    System.currentTimeMillis() - timeAtStart);
        } catch (Exception e) {
            return new ValueResult<Integer>(false, involvedKeys,
                    e.getClass().getCanonicalName() + " updating \"" + pageList_key
                            + "\" and \"" + pageCount_key + "\" in Scalaris: "
                            + e.getMessage(), e instanceof ConnectionException,
                    statName, System.currentTimeMillis() - timeAtStart);
        }
    }

    /**
     * Converts a list of {@link NormalisedTitle} objects to a list of
     * normalised page title strings.
     * 
     * @param list
     *            the {@link NormalisedTitle} list
     * 
     * @return a string list
     */
    public static List<String> normList2normStringList(
            Collection<? extends NormalisedTitle> list) {
        ArrayList<String> entriesToAddStr = new ArrayList<String>(list.size());
        for (NormalisedTitle nt : list) {
            entriesToAddStr.add(nt.toString());
        }
        return entriesToAddStr;
    }

}
