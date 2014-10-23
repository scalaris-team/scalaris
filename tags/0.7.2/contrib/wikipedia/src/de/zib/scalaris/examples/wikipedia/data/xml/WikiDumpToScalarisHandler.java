/**
 *  Copyright 2007-2013 Zuse Institute Berlin
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
package de.zib.scalaris.examples.wikipedia.data.xml;

import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import de.zib.scalaris.AbortException;
import de.zib.scalaris.Connection;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.ConnectionFactory;
import de.zib.scalaris.Transaction;
import de.zib.scalaris.TransactionSingleOp;
import de.zib.scalaris.UnknownException;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandlerNormalised;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandlerUnnormalised;
import de.zib.scalaris.examples.wikipedia.ScalarisOpType;
import de.zib.scalaris.examples.wikipedia.ValueResult;
import de.zib.scalaris.examples.wikipedia.bliki.MyNamespace.NamespaceEnum;
import de.zib.scalaris.examples.wikipedia.bliki.NormalisedTitle;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.ShortRevision;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;
import de.zib.scalaris.operations.WriteOp;

/**
 * Provides abilities to read an xml wiki dump file and write its contents to
 * the Scalaris.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiDumpToScalarisHandler extends WikiDumpPageHandler {
    private static final int MAX_SCALARIS_CONNECTIONS = Runtime.getRuntime().availableProcessors() * 2;
    private ArrayBlockingQueue<TransactionSingleOp> scalaris_single = new ArrayBlockingQueue<TransactionSingleOp>(MAX_SCALARIS_CONNECTIONS);
    private ArrayBlockingQueue<Transaction> scalaris_tx = new ArrayBlockingQueue<Transaction>(MAX_SCALARIS_CONNECTIONS);
    private ExecutorService executor = createExecutor(MAX_SCALARIS_CONNECTIONS);
    private ExecutorService pageListExecutor = createExecutor(1);

    /**
     * Sets up a SAX XmlHandler exporting all parsed pages except the ones in a
     * blacklist to stdout.
     * 
     * @param blacklist
     *            a number of page titles to ignore
     * @param whitelist
     *            only import these pages
     * @param maxRevisions
     *            maximum number of revisions per page (starting with the most
     *            recent) - <tt>-1/tt> imports all revisions
     *            (useful to speed up the import / reduce the DB size)
     * @param minTime
     *            minimum time a revision should have (only one revision older
     *            than this will be imported) - <tt>null/tt> imports all
     *            revisions
     * @param maxTime
     *            maximum time a revision should have (newer revisions are
     *            omitted) - <tt>null/tt> imports all revisions
     *            (useful to create dumps of a wiki at a specific point in time)
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     */
    public WikiDumpToScalarisHandler(Set<String> blacklist,
            Set<String> whitelist, int maxRevisions, Calendar minTime,
            Calendar maxTime) throws RuntimeException {
        super(blacklist, whitelist, maxRevisions, minTime, maxTime);
        init(ConnectionFactory.getInstance());
    }

    /**
     * Sets up a SAX XmlHandler exporting all parsed pages except the ones in a
     * blacklist to stdout.
     * 
     * @param blacklist
     *            a number of page titles to ignore
     * @param whitelist
     *            only import these pages
     * @param maxRevisions
     *            maximum number of revisions per page (starting with the most
     *            recent) - <tt>-1/tt> imports all revisions
     *            (useful to speed up the import / reduce the DB size)
     * @param minTime
     *            minimum time a revision should have (only one revision older
     *            than this will be imported) - <tt>null/tt> imports all
     *            revisions
     * @param maxTime
     *            maximum time a revision should have (newer revisions are
     *            omitted) - <tt>null/tt> imports all revisions
     *            (useful to create dumps of a wiki at a specific point in time)
     * @param cFactory
     *            the connection factory to use for creating new connections
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     */
    public WikiDumpToScalarisHandler(Set<String> blacklist,
            Set<String> whitelist, int maxRevisions, Calendar maxTime,
            Calendar minTime, ConnectionFactory cFactory)
            throws RuntimeException {
        super(blacklist, whitelist, maxRevisions, minTime, maxTime);
        init(cFactory);
    }

    /**
     * Sets up connections to Scalaris.
     * 
     * @param cFactory
     *            the connection factory to use for creating new connections
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     */
    private void init(ConnectionFactory cFactory) throws RuntimeException {
        try {
            for (int i = 0; i < MAX_SCALARIS_CONNECTIONS; ++i) {
                Connection connection = cFactory.createConnection(
                        "wiki_import", true);
                scalaris_single.put(new TransactionSingleOp(connection));
                connection = cFactory.createConnection(
                        "wiki_import", true);
                scalaris_tx.put(new Transaction(connection));
            }
        } catch (ConnectionException e) {
            error("Connection to Scalaris failed");
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            error("Interrupted while setting up multiple connections to Scalaris");
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new {@link ThreadPoolExecutor} used during the import to
     * process import jobs.
     * 
     * @param nThreads
     *            the (fixed) number of threads to use
     * 
     * @return a {@link ThreadPoolExecutor} with a bounded queue of length
     *         <tt>nThreads * 10</tt> that runs the job in the calling task if
     *         the queue is full
     */
    public static ThreadPoolExecutor createExecutor(int nThreads) {
        return new ThreadPoolExecutor(nThreads, nThreads, 0L,
                TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(
                        nThreads * 10),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    protected void doExport(SiteInfo siteinfo) throws RuntimeException {
        String key = ScalarisDataHandler.getSiteInfoKey();
        TransactionSingleOp scalaris_single;
        try {
            scalaris_single = this.scalaris_single.take();
        } catch (InterruptedException e) {
            error("write of " + key + " interrupted while getting connection to Scalaris");
            throw new RuntimeException(e);
        }
        try {
            scalaris_single.write(key, siteinfo);
        } catch (ConnectionException e) {
            error("write of " + key + " failed with connection error");
        } catch (AbortException e) {
            error("write of " + key + " failed with abort");
        } catch (UnknownException e) {
            error("write of " + key + " failed with unknown");
        }
        if (scalaris_single != null) {
            try {
                this.scalaris_single.put(scalaris_single);
            } catch (InterruptedException e) {
                error("Interrupted while putting back a connection to Scalaris");
                throw new RuntimeException(e);
            }
        }
    }
    
    @Override
    protected void doExport(Page page, List<Revision> revisions,
            List<ShortRevision> revisions_short, NormalisedTitle title)
            throws UnsupportedOperationException {
        // do not make the translog too full -> write revisions beforehand,
        // ignore the (rest of the) page if a failure occured
        TransactionSingleOp.RequestList requests = new TransactionSingleOp.RequestList();
        for (Revision rev : revisions) {
            if (rev.getId() != page.getCurRev().getId()) {
                String key = ScalarisDataHandlerUnnormalised.getRevKey(page.getTitle(), rev.getId(), wikiModel.getNamespace());
                requests.addOp(new WriteOp(key, rev));
            }
        }
        requests.addOp(new WriteOp(ScalarisDataHandlerUnnormalised.getRevListKey(page.getTitle(), wikiModel.getNamespace()), revisions_short));
        requests.addOp(new WriteOp(ScalarisDataHandlerUnnormalised.getPageKey(page.getTitle(), wikiModel.getNamespace()), page));
        Runnable worker = new MyScalarisSingleRunnable(this, requests,
                scalaris_single, "revisions and page of " + page.getTitle());
        executor.execute(worker);
        newPages.get(NamespaceEnum.fromId(title.namespace)).add(title);
        // only export page list every UPDATE_PAGELIST_EVERY pages:
        if ((pageCount % UPDATE_PAGELIST_EVERY) == 0) {
            updatePageLists();
        }
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpHandler#tearDown()
     */
    @Override
    public void tearDown() {
        super.tearDown();
        updatePageLists();
        importEnd();
    }
    
    private void updatePageLists() {
        String scalaris_key;
        Runnable worker;
        
        // list of pages:
        for(NamespaceEnum ns : NamespaceEnum.values()) {
            scalaris_key = ScalarisDataHandler.getPageListKey(ns.getId());
            worker = new MyScalarisAddToPageListRunnable(this, scalaris_key,
                    newPages.get(ns), scalaris_tx, ScalarisOpType.PAGE_LIST,
                    ScalarisDataHandler.getPageCountKey(ns.getId()),
                    ScalarisOpType.PAGE_COUNT);
            pageListExecutor.execute(worker);
        }
        initNewPagesList();
        
        // articles count:
        TransactionSingleOp.RequestList requests = new TransactionSingleOp.RequestList();
        requests.addOp(new WriteOp(ScalarisDataHandler.getArticleCountKey(), articleCount));
        worker = new MyScalarisSingleRunnable(this, requests, scalaris_single, "ARTICLE_COUNT");
        pageListExecutor.execute(worker);
        
        // list of pages in each category:
        for (Entry<NormalisedTitle, List<NormalisedTitle>> category: newCategories.entrySet()) {
            scalaris_key = ScalarisDataHandlerNormalised.getCatPageListKey(category.getKey());
            worker = new MyScalarisAddToPageListRunnable(this, scalaris_key,
                    category.getValue(), scalaris_tx, ScalarisOpType.CATEGORY_PAGE_LIST,
                    ScalarisDataHandlerNormalised.getCatPageCountKey(category.getKey()),
                    ScalarisOpType.CATEGORY_PAGE_COUNT);
            pageListExecutor.execute(worker);
        }

        // list of pages a template is used in:
        for (Entry<NormalisedTitle, List<NormalisedTitle>> template: newTemplates.entrySet()) {
            scalaris_key = ScalarisDataHandlerNormalised.getTplPageListKey(template.getKey());
            worker = new MyScalarisAddToPageListRunnable(this, scalaris_key,
                    template.getValue(), scalaris_tx,
                    ScalarisOpType.TEMPLATE_PAGE_LIST, null, null);
            pageListExecutor.execute(worker);
        }
        
        // list of pages linking to other pages:
        for (Entry<NormalisedTitle, List<NormalisedTitle>> backlinks: newBackLinks.entrySet()) {
            scalaris_key = ScalarisDataHandlerNormalised.getBackLinksPageListKey(backlinks.getKey());
            worker = new MyScalarisAddToPageListRunnable(this, scalaris_key,
                    backlinks.getValue(), scalaris_tx,
                    ScalarisOpType.BACKLINK_PAGE_LIST, null, null);
            pageListExecutor.execute(worker);
        }
        initLinkLists();

        executor.shutdown();
        pageListExecutor.shutdown();
        boolean shutdown = false;
        while (!shutdown) {
            try {
                shutdown = executor.awaitTermination(1, TimeUnit.MINUTES)
                        && pageListExecutor.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
            }
        }
        executor = createExecutor(MAX_SCALARIS_CONNECTIONS);
        pageListExecutor = createExecutor(1);
    }

    @Override
    public boolean isErrorDuringImport() {
        return errorDuringImport;
    }

    @Override
    public void error(String message) {
        System.err.println(message);
        errorDuringImport = true;
    }

    /**
     * Processes write requests to Scalaris in a separate thread. Takes one of
     * the available {@link #scalarisSingleQueue} connections.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    static class MyScalarisSingleRunnable implements Runnable {
        private final TransactionSingleOp.RequestList requests;
        private final ArrayBlockingQueue<TransactionSingleOp> scalarisSingleQueue;
        private final String note;
        private final WikiDump importer;
        private TransactionSingleOp scalaris_single;
        
        public MyScalarisSingleRunnable(WikiDump importer,
                TransactionSingleOp.RequestList requests,
                ArrayBlockingQueue<TransactionSingleOp> scalarisSingleQueue,
                String note) {
            this.importer = importer;
            this.requests = requests;
            this.scalarisSingleQueue = scalarisSingleQueue;
            this.note = note;
            try {
                this.scalaris_single = this.scalarisSingleQueue.take();
            } catch (InterruptedException e) {
                this.importer.error("write of " + note + " interrupted while getting connection to Scalaris");
                throw new RuntimeException(e);
            }
        }
        
        @Override
        public void run() {
            try {
                TransactionSingleOp.ResultList results = scalaris_single.req_list(requests);
                for (int i = 0; i < results.size(); ++i) {
                    results.processWriteAt(i);
                }
            } catch (ConnectionException e) {
                importer.error("write of " + note + " failed with connection error");
            } catch (AbortException e) {
                importer.error("write of " + note + " failed with abort");
            }
            if (scalaris_single != null) {
                try {
                    this.scalarisSingleQueue.put(scalaris_single);
                } catch (InterruptedException e) {
                    importer.error("Interrupted while putting back a connection to Scalaris");
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * Processes page list update requests to Scalaris in a separate thread.
     * Takes one of the available {@link #scalarisTxQueue} connections.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    private static class MyScalarisAddToPageListRunnable implements Runnable {
        private final String scalaris_pageList_key;
        private final List<NormalisedTitle> newEntries;
        private final ArrayBlockingQueue<Transaction> scalarisTxQueue;
        private final String scalaris_pageCount_key;
        private final ScalarisOpType opType;
        private final ScalarisOpType countOpType;
        private final WikiDump importer;
        private Transaction scalaris_tx;
        
        public MyScalarisAddToPageListRunnable(WikiDump importer,
                String scalaris_pageList_key, List<NormalisedTitle> newEntries,
                ArrayBlockingQueue<Transaction> scalarisTxQueue,
                ScalarisOpType opType, String scalaris_pageCount_key,
                ScalarisOpType countOpType) {
            this.importer = importer;
            this.scalaris_pageList_key = scalaris_pageList_key;
            this.newEntries = newEntries;
            this.scalarisTxQueue = scalarisTxQueue;
            this.opType = opType;
            this.scalaris_pageCount_key = scalaris_pageCount_key;
            this.countOpType = countOpType;
            try {
                this.scalaris_tx = this.scalarisTxQueue.take();
            } catch (InterruptedException e) {
                this.importer.error("update of " + scalaris_pageList_key + " and " + scalaris_pageCount_key + " interrupted while getting connection to Scalaris");
                throw new RuntimeException(e);
            }
        }
        
        @Override
        public void run() {
            ValueResult<Integer> result = ScalarisDataHandlerNormalised.updatePageList(
                    scalaris_tx, opType, scalaris_pageList_key,
                    countOpType, scalaris_pageCount_key, newEntries,
                    new LinkedList<NormalisedTitle>(), "");
            if (!result.success) {
                importer.error(result.message);
            }
            
            if (scalaris_tx != null) {
                try {
                    this.scalarisTxQueue.put(scalaris_tx);
                } catch (InterruptedException e) {
                    importer.error("update of " + scalaris_pageList_key + " and " + scalaris_pageCount_key + " interrupted while putting back a connection to Scalaris");
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
