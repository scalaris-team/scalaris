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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.zib.scalaris.AbortException;
import de.zib.scalaris.Connection;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.Transaction;
import de.zib.scalaris.examples.wikipedia.Options.STORE_CONTRIB_TYPE;
import de.zib.scalaris.examples.wikipedia.bliki.MyNamespace;
import de.zib.scalaris.examples.wikipedia.bliki.MyWikiModel;
import de.zib.scalaris.examples.wikipedia.bliki.NormalisedTitle;
import de.zib.scalaris.examples.wikipedia.data.Contribution;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.ShortRevision;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;
import de.zib.scalaris.operations.ReadOp;

/**
 * @author Nico Kruber, kruber@zib.de
 *
 */
public class ScalarisDataHandlerUnnormalised extends ScalarisDataHandler {
    
    /**
     * Gets the key to store {@link Revision} objects at.
     * 
     * @param title     the title of the page
     * @param id        the id of the revision
     * @param nsObject  the namespace for page title normalisation
     * 
     * @return Scalaris key
     */
    public static String getRevKey(String title, int id, final MyNamespace nsObject) {
        return ScalarisDataHandlerNormalised.getRevKey(NormalisedTitle.fromUnnormalised(title, nsObject), id);
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
        return ScalarisDataHandlerNormalised.getPageKey(NormalisedTitle.fromUnnormalised(title, nsObject));
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
        return ScalarisDataHandlerNormalised.getRevListKey(NormalisedTitle.fromUnnormalised(title, nsObject));
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
        return ScalarisDataHandlerNormalised.getCatPageListKey(NormalisedTitle.fromUnnormalised(title, nsObject));
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
        return ScalarisDataHandlerNormalised.getCatPageCountKey(NormalisedTitle.fromUnnormalised(title, nsObject));
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
        return ScalarisDataHandlerNormalised.getTplPageListKey(NormalisedTitle.fromUnnormalised(title, nsObject));
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
        return ScalarisDataHandlerNormalised.getBackLinksPageListKey(NormalisedTitle.fromUnnormalised(title, nsObject));
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
        return ScalarisDataHandlerNormalised.getPageHistory(connection, NormalisedTitle.fromUnnormalised(title, nsObject));
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
        return ScalarisDataHandlerNormalised.getRevision(connection, NormalisedTitle.fromUnnormalised(title, nsObject));
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
        return ScalarisDataHandlerNormalised.getRevision(connection, NormalisedTitle.fromUnnormalised(title, nsObject), id);
    }

    /**
     * Retrieves the current version of all given pages from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param titles
     *            the titles of the pages
     * @param nsObject
     *            the namespace for page title normalisation
     * @param statName
     *            name of the statistic to collect
     * 
     * @return a result object with the pages and revisions on success
     */
    public static ValueResult<List<RevisionResult>> getRevisions(Connection connection,
            Collection<String> titles, final String statName, final MyNamespace nsObject) {
        final ArrayList<NormalisedTitle> normalisedTitles = new ArrayList<NormalisedTitle>(titles.size());
        MyWikiModel.normalisePageTitles(titles, nsObject, normalisedTitles);
        return ScalarisDataHandlerNormalised.getRevisions(connection, normalisedTitles, statName);
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
    public static ValueResult<List<NormalisedTitle>> getPagesInCategory(Connection connection,
            String title, final MyNamespace nsObject) {
        return ScalarisDataHandlerNormalised.getPagesInCategory(connection, NormalisedTitle.fromUnnormalised(title, nsObject));
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
    public static ValueResult<List<NormalisedTitle>> getPagesInTemplate(Connection connection,
            String title, final MyNamespace nsObject) {
        return ScalarisDataHandlerNormalised.getPagesInTemplate(connection, NormalisedTitle.fromUnnormalised(title, nsObject));
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
    public static ValueResult<List<NormalisedTitle>> getPagesLinkingTo(Connection connection,
            String title, final MyNamespace nsObject) {
        return ScalarisDataHandlerNormalised.getPagesLinkingTo(connection, NormalisedTitle.fromUnnormalised(title, nsObject));
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
        return ScalarisDataHandlerNormalised.getPagesInCategoryCount(connection, NormalisedTitle.fromUnnormalised(title, nsObject));
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
        final String statName = "SAVE:" + title0;
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
        
        final NormalisedTitle normTitle = NormalisedTitle.fromUnnormalised(title0, nsObject);
        final String normTitleStr = normTitle.toString();
        Transaction scalaris_tx = new Transaction(connection);

        // check that the current version is still up-to-date:
        // read old version first, then write
        String pageInfoKey = getPageKey(title0, nsObject);
        
        Transaction.RequestList requests = new Transaction.RequestList();
        requests.addOp(new ReadOp(pageInfoKey));
        
        Transaction.ResultList results;
        try {
            addInvolvedKeys(involvedKeys, requests.getRequests());
            results = scalaris_tx.req_list(requests);
        } catch (Exception e) {
            return new SavePageResult(false, involvedKeys,
                    e.getClass().getCanonicalName() + " getting page info (" + pageInfoKey
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
                    e.getClass().getCanonicalName() + " reading \"" + pageInfoKey
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
        final MyWikiModel wikiModel = new MyWikiModel("", "", nsObject);
        wikiModel.setNamespaceName(nsObject.getNamespaceByNumber(normTitle.namespace));
        wikiModel.setPageName(normTitle.title);
        Set<String> oldCats;
        Set<String> oldTpls;
        Set<String> oldLnks;
        if (oldPage != null && oldPage.getCurRev() != null) {
            // get a list of previous categories and templates:
            wikiModel.setUp();
            final long timeAtRenderStart = System.currentTimeMillis();
            wikiModel.renderPageWithCache(null, oldPage.getCurRev().unpackedText());
            timeAtStart += (System.currentTimeMillis() - timeAtRenderStart);
            // note: no need to normalise the pages, we will do so during the write/read key generation
            oldCats = wikiModel.getCategories().keySet();
            oldTpls = wikiModel.getTemplatesNoMagicWords();
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
            wikiModel.renderPageWithCache(null, newRev.unpackedText());
            timeAtStart += (System.currentTimeMillis() - timeAtRenderStart);
        } while (false);
        newPage.setRedirect(wikiModel.getRedirectLink() != null);
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
                }, ScalarisOpType.CATEGORY_PAGE_LIST, ScalarisOpType.CATEGORY_PAGE_COUNT);
        final Set<String> newTpls = wikiModel.getTemplatesNoMagicWords();
        Difference tplDiff = new Difference(oldTpls, newTpls,
                new Difference.GetPageListKey() {
                    @Override
                    public String getPageListKey(String name) {
                        return getTplPageListKey(
                                wikiModel.getTemplateNamespace() + ":" + name,
                                nsObject);
                    }
                }, ScalarisOpType.TEMPLATE_PAGE_LIST, null);
        // use empty link lists to turn back-links off
        final Set<String> newLnks = Options.getInstance().WIKI_USE_BACKLINKS ? wikiModel.getLinks() : new HashSet<String>();
        Difference lnkDiff = new Difference(oldLnks, newLnks,
                new Difference.GetPageListKey() {
                    @Override
                    public String getPageListKey(String name) {
                        return getBackLinksPageListKey(name, nsObject);
                    }
                }, ScalarisOpType.BACKLINK_PAGE_LIST, null);
        

        // now save the changes:
        do {
            final MyScalarisTxOpExecutor executor0 = new MyScalarisTxOpExecutor(
                    scalaris_tx, involvedKeys);
            executor0.setCommitLast(true);
            MyScalarisOpExecWrapper executor = new MyScalarisOpExecWrapper(
                    executor0);

            int articleCountChange = 0;
            final boolean wasArticle = (oldPage != null)
                    && MyWikiModel.isArticle(normTitle.namespace, oldLnks, oldCats);
            final boolean isArticle = (normTitle.namespace == 0)
                    && MyWikiModel.isArticle(normTitle.namespace, newLnks, newCats);
            if (wasArticle == isArticle) {
                articleCountChange = 0;
            } else if (!wasArticle) {
                articleCountChange = 1;
            } else if (!isArticle) {
                articleCountChange = -1;
            }

            //  PAGE LISTS UPDATE, step 1: append to / remove from old lists
            executor.addAppend(ScalarisOpType.SHORTREV_LIST, getRevListKey(title0, nsObject), new ShortRevision(newRev), null, null);
            if (articleCountChange != 0) {
                executor.addIncrement(ScalarisOpType.ARTICLE_COUNT, getArticleCountKey(), articleCountChange, normTitleStr);
            }

            // write differences (categories, templates, backlinks)
            catDiff.addScalarisOps(executor, normTitleStr);
            tplDiff.addScalarisOps(executor, normTitleStr);
            lnkDiff.addScalarisOps(executor, normTitleStr);

            // new page? -> add to page/article lists
            if (oldPage == null) {
                final String pageListKey = getPageListKey(normTitle.namespace);
                final String pageCountKey = getPageCountKey(normTitle.namespace);
                executor.addAppend(ScalarisOpType.PAGE_LIST, pageListKey, normTitleStr, ScalarisOpType.PAGE_COUNT, pageCountKey);
            }

            executor.addWrite(ScalarisOpType.PAGE, getPageKey(title0, nsObject), newPage);
            if (oldPage != null) {
                executor.addWrite(ScalarisOpType.REVISION, getRevKey(title0, oldPage.getCurRev().getId(), nsObject), oldPage.getCurRev());
            }

            //  PAGE LISTS UPDATE, step 2: execute and evaluate operations
            try {
                executor.getExecutor().run();
            } catch (Exception e) {
                SavePageResult result = new SavePageResult(false, involvedKeys,
                        e.getClass().getCanonicalName() + " writing page \"" + title0
                                + "\" to Scalaris: " + e.getMessage(),
                        e instanceof ConnectionException, oldPage, newPage,
                        newShortRevs, pageEdits, statName,
                        System.currentTimeMillis() - timeAtStart);
                if (e instanceof AbortException) {
                    result.failedKeys.addAll(((AbortException) e).getFailedKeys());
                }
                return result;
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
        
        executor.addIncrement(ScalarisOpType.EDIT_STAT, getStatsPageEditsKey(), 1, getStatsPageEditsKey());
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
                Arrays.asList(new Contribution(oldPage, newPage)), null, null);
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
        final private ScalarisOpType countOpType;
        
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
         * @param countOpType
         *            operation type indicating what is being updated for
         *            updating count keys (if there are no count keys, this may
         *            be <tt>null</tt>)
         */
        public Difference(Set<String> oldSet, Set<String> newSet,
                GetPageListKey keyGen, ScalarisOpType opType,
                ScalarisOpType countOpType) {
            this.onlyOld = new HashSet<String>(oldSet);
            this.onlyNew = new HashSet<String>(newSet);
            this.onlyOld.removeAll(newSet);
            this.onlyNew.removeAll(oldSet);
            this.changes[0] = this.onlyOld;
            this.changes[1] = this.onlyNew;
            this.keyGen = keyGen;
            this.opType = opType;
            this.countOpType = countOpType;
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
                assert(null != countOpType);
                keyCountGen = (GetPageListAndCountKey) keyGen;
            }
            // remove from old page list
            for (String name: onlyOld) {
                scalaris_key = keyGen.getPageListKey(name);
//                System.out.println(scalaris_key + " -= " + title);
                String scalaris_countKey = keyCountGen == null ? null : keyCountGen.getPageCountKey(name);
                executor.addRemove(opType, scalaris_key, title, countOpType, scalaris_countKey);
            }
            // add to new page list
            for (String name: onlyNew) {
                scalaris_key = keyGen.getPageListKey(name);
//              System.out.println(scalaris_key + " += " + title);
                String scalaris_countKey = keyCountGen == null ? null : keyCountGen.getPageCountKey(name);
                executor.addAppend(opType, scalaris_key, title, countOpType, scalaris_countKey);
            }
        }
    }
}
