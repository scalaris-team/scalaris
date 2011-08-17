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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
import de.zib.scalaris.examples.wikipedia.SaveResult;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler;
import de.zib.scalaris.examples.wikipedia.bliki.MyWikiModel;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.ShortRevision;

/**
 * Provides abilities to read an xml wiki dump file and write its contents to
 * the standard output.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiDumpToScalarisHandler extends WikiDumpHandler {
    private static final int UPDATE_PAGELIST_EVERY = 400;
    private static final int MAX_SCALARIS_CONNECTIONS = 4;
    private static final int NEW_CATS_HASH_DEF_SIZE = 100;
    private static final int NEW_TPLS_HASH_DEF_SIZE = 100;
    private static final int NEW_BLNKS_HASH_DEF_SIZE = 100;
    
    private ArrayBlockingQueue<TransactionSingleOp> scalaris_single = new ArrayBlockingQueue<TransactionSingleOp>(MAX_SCALARIS_CONNECTIONS);
    private ArrayBlockingQueue<Transaction> scalaris_tx = new ArrayBlockingQueue<Transaction>(MAX_SCALARIS_CONNECTIONS);
    private List<String> newPages = new LinkedList<String>();
    private List<String> newArticles = new LinkedList<String>();
    private HashMap<String, List<String>> newCategories = new HashMap<String, List<String>>(NEW_CATS_HASH_DEF_SIZE);
    private HashMap<String, List<String>> newTemplates = new HashMap<String, List<String>>(NEW_TPLS_HASH_DEF_SIZE);
    private HashMap<String, List<String>> newBackLinks = new HashMap<String, List<String>>(NEW_BLNKS_HASH_DEF_SIZE);
    
    private ExecutorService executor = Executors.newFixedThreadPool(MAX_SCALARIS_CONNECTIONS);

    /**
     * Sets up a SAX XmlHandler exporting all parsed pages except the ones in a
     * blacklist to stdout.
     * 
     * @param blacklist
     *            a number of page titles to ignore
     * @param maxRevisions
     *            maximum number of revisions per page (starting with the most
     *            recent) - <tt>-1/tt> imports all revisions
     *            (useful to speed up the import / reduce the DB size)
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     */
    public WikiDumpToScalarisHandler(Set<String> blacklist, int maxRevisions) throws RuntimeException {
        super(blacklist, maxRevisions);
        init(blacklist, maxRevisions, ConnectionFactory.getInstance());
    }

    /**
     * Sets up a SAX XmlHandler exporting all parsed pages except the ones in a
     * blacklist to stdout.
     * 
     * @param blacklist
     *            a number of page titles to ignore
     * @param maxRevisions
     *            maximum number of revisions per page (starting with the most
     *            recent) - <tt>-1/tt> imports all revisions
     *            (useful to speed up the import / reduce the DB size)
     * @param cFactory
     *            the connection factory to use for creating new connections
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     */
    public WikiDumpToScalarisHandler(Set<String> blacklist, int maxRevisions, ConnectionFactory cFactory) throws RuntimeException {
        super(blacklist, maxRevisions);
        init(blacklist, maxRevisions, cFactory);
    }

    /**
     * Sets up a SAX XmlHandler exporting all parsed pages except the ones in a
     * blacklist to stdout.
     * 
     * @param blacklist
     *            a number of page titles to ignore
     * @param maxRevisions
     *            maximum number of revisions per page (starting with the most
     *            recent) - <tt>-1/tt> imports all revisions
     *            (useful to speed up the import / reduce the DB size)
     * @param cFactory
     *            the connection factory to use for creating new connections
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     */
    public void init(Set<String> blacklist, int maxRevisions, ConnectionFactory cFactory) throws RuntimeException {
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

    /**
     * Exports the given siteinfo to Scalaris
     * 
     * @param revisions
     *            the siteinfo to export
     */
    @Override
    protected void export(XmlSiteInfo siteinfo_xml) {
        String key = ScalarisDataHandler.getSiteInfoKey();
        TransactionSingleOp scalaris_single;
        try {
            scalaris_single = this.scalaris_single.take();
        } catch (InterruptedException e) {
            System.err.println("write of " + key + " interrupted while getting connection to Scalaris");
            throw new RuntimeException(e);
        }
        try {
            scalaris_single.write(key, siteinfo_xml.getSiteInfo());
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

    /**
     * Provides a comparator for sorting {@link Revision} objects by their IDs.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    private static class byRevId implements java.util.Comparator<Revision> {
        /**
         * Compares its two arguments for order. Returns a negative integer,
         * zero, or a positive integer as the first argument is less than, equal
         * to, or greater than the second.
         * 
         * @param rev1
         *            the first revision to be compared.
         * @param rev2
         *            the second revision to be compared.
         * 
         * @return a negative integer, zero, or a positive integer as the first
         *         argument is less than, equal to, or greater than the second.
         */
        @Override
        public int compare(Revision rev1, Revision rev2) {
            return (rev1.getId() - rev2.getId());
        }
    }

    /**
     * Provides a comparator for sorting {@link Revision} objects by their IDs.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    private static class byShortRevId implements java.util.Comparator<ShortRevision> {
        /**
         * Compares its two arguments for order. Returns a negative integer,
         * zero, or a positive integer as the first argument is less than, equal
         * to, or greater than the second.
         * 
         * @param rev1
         *            the first revision to be compared.
         * @param rev2
         *            the second revision to be compared.
         * 
         * @return a negative integer, zero, or a positive integer as the first
         *         argument is less than, equal to, or greater than the second.
         */
        @Override
        public int compare(ShortRevision rev1, ShortRevision rev2) {
            return (rev1.getId() - rev2.getId());
        }
    }
    
    private static class MyScalarisSingleRunnable implements Runnable {
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
     * dumps the given page (including all revisions) to the standard output in
     * the following text format:
     * <code>
     * {title, id, revisions, props}
     * </code>
     * @param String 
     * 
     * @param revision
     *            the page to export
     */
    @Override
    protected void export(XmlPage page_xml) {
        Page page = page_xml.getPage();
//      String title = page.getTitle().replaceFirst("^Category:", "Kategorie:");

        List<Revision> revisions = page_xml.getRevisions();
        List<ShortRevision> revisions_short = page_xml.getRevisions_short();
        Collections.sort(revisions, Collections.reverseOrder(new byRevId()));
        Collections.sort(revisions_short, Collections.reverseOrder(new byShortRevId()));
        
        if (!revisions.isEmpty() && wikiModel != null) {
            wikiModel.render(null, revisions.get(0).getText());
            for (String cat_raw: wikiModel.getCategories().keySet()) {
                String category = wikiModel.getCategoryNamespace() + ":" + cat_raw;
                List<String> catPages = newCategories.get(category);
                if (catPages == null) {
                    catPages = new ArrayList<String>(UPDATE_PAGELIST_EVERY / 4);
                }
                catPages.add(page.getTitle());
                newCategories.put(category, catPages);
            }
            for (String tpl_raw: wikiModel.getTemplates()) {
                String template = wikiModel.getTemplateNamespace() + ":" + tpl_raw;
                List<String> templatePages = newTemplates.get(template);
                if (templatePages == null) {
                    templatePages = new ArrayList<String>(UPDATE_PAGELIST_EVERY / 4);
                }
                templatePages.add(page.getTitle());
                newTemplates.put(template, templatePages);
            }
            for (String link: wikiModel.getLinks()) {
                List<String> backLinks = newBackLinks.get(link);
                if (backLinks == null) {
                    backLinks = new ArrayList<String>(UPDATE_PAGELIST_EVERY / 4);
                }
                backLinks.add(page.getTitle());
                newBackLinks.put(link, backLinks);
            }
        }

        // do not make the translog too full -> write revisions beforehand,
        // ignore the (rest of the) page if a failure occured
        TransactionSingleOp.RequestList requests = new TransactionSingleOp.RequestList();
        for (Revision rev : revisions) {
            String key = ScalarisDataHandler.getRevKey(page.getTitle(), rev.getId());
            requests.addWrite(key, rev);
        }
        requests.addWrite(ScalarisDataHandler.getRevListKey(page.getTitle()), revisions_short);
        requests.addWrite(ScalarisDataHandler.getPageKey(page.getTitle()), page);
        Runnable worker = new MyScalarisSingleRunnable(requests, scalaris_single, "revisions and page of " + page.getTitle());
        executor.execute(worker);
        newPages.add(page.getTitle());
        ++pageCount;
        // simple article filter: only pages in main namespace:
        if (MyWikiModel.getNamespace(page.getTitle()).isEmpty()) {
            newArticles.add(page.getTitle());
        }
        // only export page list every UPDATE_PAGELIST_EVERY pages:
        if ((newPages.size() % UPDATE_PAGELIST_EVERY) == 0) {
            updatePageLists();
            msgOut.println("imported pages: " + pageCount);
        }
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpHandler#tearDown()
     */
    @Override
    public void tearDown() {
        updatePageLists();
        importEnd();
    }
    
    private void updatePageLists() {
        String scalaris_key;
        Runnable worker;
        
        // list of pages:
        scalaris_key = ScalarisDataHandler.getPageListKey();
        worker = new MyScalarisAddToPageListRunnable(scalaris_key, newPages, scalaris_tx, ScalarisDataHandler.getPageCountKey());
        executor.execute(worker);
        newPages = new LinkedList<String>();
        
        // list of articles:
        scalaris_key = ScalarisDataHandler.getArticleListKey();
        worker = new MyScalarisAddToPageListRunnable(scalaris_key, newArticles, scalaris_tx, ScalarisDataHandler.getArticleCountKey());
        executor.execute(worker);
        newArticles = new LinkedList<String>();
        
        // list of pages in each category:
        for (Entry<String, List<String>> category: newCategories.entrySet()) {
            scalaris_key = ScalarisDataHandler.getCatPageListKey(category.getKey());
            worker = new MyScalarisAddToPageListRunnable(scalaris_key, category.getValue(), scalaris_tx);
            executor.execute(worker);
        }
        newCategories = new HashMap<String, List<String>>(NEW_CATS_HASH_DEF_SIZE);

        // list of pages a template is used in:
        for (Entry<String, List<String>> template: newTemplates.entrySet()) {
            scalaris_key = ScalarisDataHandler.getTplPageListKey(template.getKey());
            worker = new MyScalarisAddToPageListRunnable(scalaris_key, template.getValue(), scalaris_tx);
            executor.execute(worker);
        }
        newTemplates = new HashMap<String, List<String>>(NEW_TPLS_HASH_DEF_SIZE);
        
        // list of pages linking to other pages:
        for (Entry<String, List<String>> backlinks: newBackLinks.entrySet()) {
            scalaris_key = ScalarisDataHandler.getBackLinksPageListKey(backlinks.getKey());
            worker = new MyScalarisAddToPageListRunnable(scalaris_key, backlinks.getValue(), scalaris_tx);
            executor.execute(worker);
        }
        newBackLinks = new HashMap<String, List<String>>(NEW_BLNKS_HASH_DEF_SIZE);
        
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
            
            SaveResult result = ScalarisDataHandler.updatePageList(scalaris_tx, scalaris_pageList_key, scalaris_pageCount_key, new LinkedList<String>(), newEntries);
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
