/**
 *  Copyright 2007-2011 Zuse Institute Berlin
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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import de.zib.scalaris.AbortException;
import de.zib.scalaris.Connection;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.ConnectionFactory;
import de.zib.scalaris.TimeoutException;
import de.zib.scalaris.Transaction;
import de.zib.scalaris.TransactionSingleOp;
import de.zib.scalaris.UnknownException;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler;
import de.zib.scalaris.examples.wikipedia.ValueResult;
import de.zib.scalaris.examples.wikipedia.bliki.MyNamespace.NamespaceEnum;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.ShortRevision;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;

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
    private ExecutorService executor = Executors.newFixedThreadPool(MAX_SCALARIS_CONNECTIONS);

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
            System.err.println("Connection to Scalaris failed");
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            System.err.println("Interrupted while setting up multiple connections to Scalaris");
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void doExport(SiteInfo siteinfo) throws RuntimeException {
        String key = ScalarisDataHandler.getSiteInfoKey();
        TransactionSingleOp scalaris_single;
        try {
            scalaris_single = this.scalaris_single.take();
        } catch (InterruptedException e) {
            System.err.println("write of " + key + " interrupted while getting connection to Scalaris");
            throw new RuntimeException(e);
        }
        try {
            scalaris_single.write(key, siteinfo);
        } catch (ConnectionException e) {
            System.err.println("write of " + key + " failed with connection error");
        } catch (TimeoutException e) {
            System.err.println("write of " + key + " failed with timeout");
        } catch (AbortException e) {
            System.err.println("write of " + key + " failed with abort");
        } catch (UnknownException e) {
            System.err.println("write of " + key + " failed with unknown");
        }
        if (scalaris_single != null) {
            try {
                this.scalaris_single.put(scalaris_single);
            } catch (InterruptedException e) {
                System.err.println("Interrupted while putting back a connection to Scalaris");
                throw new RuntimeException(e);
            }
        }
    }
    
    @Override
    protected void doExport(Page page, List<Revision> revisions,
            List<ShortRevision> revisions_short, NamespaceEnum namespace)
            throws UnsupportedOperationException {
        // do not make the translog too full -> write revisions beforehand,
        // ignore the (rest of the) page if a failure occured
        TransactionSingleOp.RequestList requests = new TransactionSingleOp.RequestList();
        for (Revision rev : revisions) {
            String key = ScalarisDataHandler.getRevKey(page.getTitle(), rev.getId(), wikiModel.getNamespace());
            requests.addWrite(key, rev);
        }
        requests.addWrite(ScalarisDataHandler.getRevListKey(page.getTitle(), wikiModel.getNamespace()), revisions_short);
        requests.addWrite(ScalarisDataHandler.getPageKey(page.getTitle(), wikiModel.getNamespace()), page);
        Runnable worker = new MyScalarisSingleRunnable(requests, scalaris_single, "revisions and page of " + page.getTitle());
        executor.execute(worker);
        newPages.get(namespace).add(wikiModel.normalisePageTitle(page.getTitle()));
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
            worker = new MyScalarisAddToPageListRunnable(scalaris_key,
                    newPages.get(ns), scalaris_tx,
                    ScalarisDataHandler.getPageCountKey(ns.getId()));
            executor.execute(worker);
        }
        initNewPagesList();
        
        // articles count:
        TransactionSingleOp.RequestList requests = new TransactionSingleOp.RequestList();
        requests.addWrite(ScalarisDataHandler.getArticleCountKey(), articleCount);
        worker = new MyScalarisSingleRunnable(requests, scalaris_single, "article count");
        executor.execute(worker);
        
        // list of pages in each category:
        for (Entry<String, List<String>> category: newCategories.entrySet()) {
            final String catName = category.getKey();
            scalaris_key = ScalarisDataHandler.getCatPageListKey(catName, wikiModel.getNamespace());
            worker = new MyScalarisAddToPageListRunnable(scalaris_key,
                    category.getValue(), scalaris_tx,
                    ScalarisDataHandler.getCatPageCountKey(catName, wikiModel.getNamespace()));
            executor.execute(worker);
        }

        // list of pages a template is used in:
        for (Entry<String, List<String>> template: newTemplates.entrySet()) {
            scalaris_key = ScalarisDataHandler.getTplPageListKey(template.getKey(), wikiModel.getNamespace());
            worker = new MyScalarisAddToPageListRunnable(scalaris_key, template.getValue(), scalaris_tx);
            executor.execute(worker);
        }
        
        // list of pages linking to other pages:
        for (Entry<String, List<String>> backlinks: newBackLinks.entrySet()) {
            scalaris_key = ScalarisDataHandler.getBackLinksPageListKey(backlinks.getKey(), wikiModel.getNamespace());
            worker = new MyScalarisAddToPageListRunnable(scalaris_key, backlinks.getValue(), scalaris_tx);
            executor.execute(worker);
        }
        initLinkLists();
        
        executor.shutdown();
        boolean shutdown = false;
        while (!shutdown) {
            try {
                shutdown = executor.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
            }
        }
        executor = Executors.newFixedThreadPool(MAX_SCALARIS_CONNECTIONS);
    }

    /**
     * Processes write requests to Scalaris in a separate thread. Takes one of
     * the available {@link #scalaris_single} connections.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    static class MyScalarisSingleRunnable implements Runnable {
        private TransactionSingleOp.RequestList requests;
        private ArrayBlockingQueue<TransactionSingleOp> scalaris_single;
        private String note;
        
        public MyScalarisSingleRunnable(TransactionSingleOp.RequestList requests, ArrayBlockingQueue<TransactionSingleOp> scalaris_single, String note) {
            this.requests = requests;
            this.scalaris_single = scalaris_single;
            this.note = note;
        }
        
        @Override
        public void run() {
            TransactionSingleOp scalaris_single;
            try {
                scalaris_single = this.scalaris_single.take();
            } catch (InterruptedException e) {
                System.err.println("write of " + note + " interrupted while getting connection to Scalaris");
                throw new RuntimeException(e);
            }
            try {
                TransactionSingleOp.ResultList results = scalaris_single.req_list(requests);
                for (int i = 0; i < results.size(); ++i) {
                    results.processWriteAt(i);
                }
            } catch (ConnectionException e) {
                System.err.println("write of " + note + " failed with connection error");
            } catch (TimeoutException e) {
                System.err.println("write of " + note + " failed with timeout");
            } catch (AbortException e) {
                System.err.println("write of " + note + " failed with abort");
            }
            if (scalaris_single != null) {
                try {
                    this.scalaris_single.put(scalaris_single);
                } catch (InterruptedException e) {
                    System.err.println("Interrupted while putting back a connection to Scalaris");
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * Processes page list update requests to Scalaris in a separate thread.
     * Takes one of the available {@link #scalaris_tx} connections.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    private static class MyScalarisAddToPageListRunnable implements Runnable {
        private String scalaris_pageList_key;
        private List<String> newEntries;
        private ArrayBlockingQueue<Transaction> scalaris_tx;
        private String scalaris_pageCount_key = null;
        
        public MyScalarisAddToPageListRunnable(String scalaris_key, List<String> newEntries, ArrayBlockingQueue<Transaction> scalaris_tx) {
            this.scalaris_pageList_key = scalaris_key;
            this.newEntries = newEntries;
            this.scalaris_tx = scalaris_tx;
        }
        
        public MyScalarisAddToPageListRunnable(String scalaris_pageList_key, List<String> newEntries, ArrayBlockingQueue<Transaction> scalaris_tx, String scalaris_pageCount_key) {
            this.scalaris_pageList_key = scalaris_pageList_key;
            this.newEntries = newEntries;
            this.scalaris_tx = scalaris_tx;
            this.scalaris_pageCount_key = scalaris_pageCount_key;
        }
        
        @Override
        public void run() {
            Transaction scalaris_tx;
            try {
                scalaris_tx = this.scalaris_tx.take();
            } catch (InterruptedException e) {
                System.err.println("update of " + scalaris_pageList_key + " and " + scalaris_pageCount_key + " interrupted while getting connection to Scalaris");
                throw new RuntimeException(e);
            }
            
            ValueResult<Integer> result = ScalarisDataHandler.updatePageList(
                    scalaris_tx, scalaris_pageList_key, scalaris_pageCount_key,
                    new LinkedList<String>(), newEntries, "");
            if (!result.success) {
                System.err.println(result.message);
            }
            
            if (scalaris_tx != null) {
                try {
                    this.scalaris_tx.put(scalaris_tx);
                } catch (InterruptedException e) {
                    System.err.println("update of " + scalaris_pageList_key + " and " + scalaris_pageCount_key + " interrupted while putting back a connection to Scalaris");
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
