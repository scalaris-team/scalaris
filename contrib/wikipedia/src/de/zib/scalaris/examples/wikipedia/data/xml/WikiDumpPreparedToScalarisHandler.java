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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import de.zib.scalaris.Connection;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.ConnectionFactory;
import de.zib.scalaris.TransactionSingleOp;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler;
import de.zib.scalaris.examples.wikipedia.bliki.MyScalarisWikiModel;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.ShortRevision;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;
import de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpToScalarisHandler.MyScalarisSingleRunnable;

/**
 * Provides abilities to read an xml wiki dump file and write its contents to
 * Scalaris.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public abstract class WikiDumpPreparedToScalarisHandler extends WikiDumpPageHandler {
    private static final int MAX_SCALARIS_CONNECTIONS = 4;
    private static final int PRINT_SCALARIS_KV_PAIRS_EVERY = 1000;
    private ArrayBlockingQueue<TransactionSingleOp> scalaris_single = new ArrayBlockingQueue<TransactionSingleOp>(MAX_SCALARIS_CONNECTIONS);
    private ExecutorService executor = Executors.newFixedThreadPool(MAX_SCALARIS_CONNECTIONS);
    protected TransactionSingleOp.RequestList requests = new TransactionSingleOp.RequestList();
    
    protected String dirname;
    protected int lastFile = 0;
    protected int pagesScalarisImport = 0;
    
    /**
     * Sets up a SAX XmlHandler exporting all parsed pages except the ones in a
     * blacklist to Scalaris but with an additional pre-process phase.
     * 
     * @param blacklist
     *            a number of page titles to ignore
     * @param whitelist
     *            only import these pages
     * @param maxRevisions
     *            maximum number of revisions per page (starting with the most
     *            recent) - <tt>-1/tt> imports all revisions
     *            (useful to speed up the import / reduce the DB size)
     * @param maxTime
     *            maximum time a revision should have (newer revisions are
     *            omitted) - <tt>null/tt> imports all revisions
     *            (useful to create dumps of a wiki at a specific point in time)
     * @param dirname
     *            the name of the directory to write temporary files to
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     */
    public WikiDumpPreparedToScalarisHandler(Set<String> blacklist, Set<String> whitelist, int maxRevisions,
            Calendar maxTime, String dirname) throws RuntimeException {
        super(blacklist, whitelist, maxRevisions, maxTime);
        this.dirname = dirname + UUID.randomUUID().toString();
        init(ConnectionFactory.getInstance());
    }
    
    /**
     * Sets up a SAX XmlHandler exporting all parsed pages except the ones in a
     * blacklist to Scalaris but with an additional pre-process phase.
     * 
     * @param blacklist
     *            a number of page titles to ignore
     * @param whitelist
     *            only import these pages
     * @param maxRevisions
     *            maximum number of revisions per page (starting with the most
     *            recent) - <tt>-1/tt> imports all revisions
     *            (useful to speed up the import / reduce the DB size)
     * @param maxTime
     *            maximum time a revision should have (newer revisions are
     *            omitted) - <tt>null/tt> imports all revisions
     *            (useful to create dumps of a wiki at a specific point in time)
     * @param dirname
     *            the name of the directory to write temporary files to
     * @param cFactory
     *            the connection factory to use for creating new connections
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     */
    public WikiDumpPreparedToScalarisHandler(Set<String> blacklist, Set<String> whitelist, int maxRevisions,
            Calendar maxTime, String dirname, ConnectionFactory cFactory) throws RuntimeException {
        super(blacklist, whitelist, maxRevisions, maxTime);
        this.dirname = dirname + UUID.randomUUID().toString();
        init(cFactory);
    }

    /**
     * Sets up the directory to write files to as well as the Scalaris
     * connection.
     * 
     * @param cFactory
     *            the connection factory to use for creating new connections
     * 
     * @throws RuntimeException
     *             if the directory could not be created
     */
    public void init(ConnectionFactory cFactory) throws RuntimeException {
        try {
            boolean success = (new File(dirname)).mkdir();
            if (!success) {
                throw new RuntimeException("Cannot create directory: " + dirname);
            }
        } catch (Exception e) {
            System.err.println("Cannot create directory: " + dirname);
            throw new RuntimeException(e);
        }
        
        try {
            for (int i = 0; i < MAX_SCALARIS_CONNECTIONS; ++i) {
                Connection connection = cFactory.createConnection(
                        "wiki_import", true);
                scalaris_single.put(new TransactionSingleOp(connection));
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
     * @param <T>
     * @param siteinfo
     * @param key
     * @throws IOException
     * @throws FileNotFoundException
     */
    protected abstract <T> void writeObject(String key, T value)
            throws RuntimeException;

    /**
     * @param <T>
     * @param siteinfo
     * @param key
     * @throws IOException
     * @throws FileNotFoundException
     */
    protected abstract <T> T readObject(String key)
            throws RuntimeException, FileNotFoundException;

    /**
     * @param siteinfo
     * @throws RuntimeException
     */
    @Override
    protected void doExport(SiteInfo siteinfo) throws RuntimeException {
        String key = ScalarisDataHandler.getSiteInfoKey();
        writeObject(key, siteinfo);
    }
    
    /**
     * @param page
     * @param revisions
     * @param revisions_short
     * @throws UnsupportedOperationException
     */
    @Override
    protected void doExport(Page page, List<Revision> revisions,
            List<ShortRevision> revisions_short)
            throws UnsupportedOperationException {
        for (Revision rev : revisions) {
            writeObject(ScalarisDataHandler.getRevKey(page.getTitle(), rev.getId()), rev);
        }
        writeObject(ScalarisDataHandler.getRevListKey(page.getTitle()), revisions_short);
        writeObject(ScalarisDataHandler.getPageKey(page.getTitle()), page);
        
        newPages.add(page.getTitle());
        // simple article filter: only pages in main namespace:
        if (MyScalarisWikiModel.getNamespace(page.getTitle()).isEmpty()) {
            newArticles.add(page.getTitle());
        }
        // only export page list every UPDATE_PAGELIST_EVERY pages:
        if ((newPages.size() % UPDATE_PAGELIST_EVERY) == 0) {
            updatePageLists();
        }
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpHandler#setUp()
     */
    @Override
    public void setUp() {
        super.setUp();
        msgOut.println("Pre-processing pages to key/value pairs...");
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpHandler#tearDown()
     */
    @Override
    public void tearDown() {
        super.tearDown();
        updatePageLists();
        this.new ReportAtShutDown().run();
        // restart time counter (?):
        importStart();
        msgOut.println("Importing key/value pairs to Scalaris...");
        writeToScalaris();
        // some requests may be left over
        Runnable worker = new MyScalarisSingleRunnable(requests, scalaris_single, "");
        executor.execute(worker);
        requests = new TransactionSingleOp.RequestList();
        executor.shutdown();
        boolean shutdown = false;
        while (!shutdown) {
            try {
                shutdown = executor.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
            }
        }
        importEnd();

        boolean success = deleteDirectory(new File(dirname));
        if (!success) {
            msgOut.println("Cannot delete directory: " + dirname);
        }
    }
    
    /**
     * Delete a directory and all files in it.
     * 
     * Note: directories in this directory are not deleted!
     * 
     * @param path
     *            the directory to delete
     * 
     * @return success status
     */
    private static boolean deleteDirectory(File path) {
        if (path.exists()) {
            File[] files = path.listFiles();
            for (int i = 0; i < files.length; i++) {
                files[i].delete();
            }
        }
        return path.delete();
    }
    
    protected void updatePageLists() {
        String scalaris_key;
        
        // list of pages:
        scalaris_key = ScalarisDataHandler.getPageListKey();
        List<String> pageList;
        try {
            pageList = readObject(scalaris_key);
        } catch (FileNotFoundException e) {
            pageList = new LinkedList<String>();
        }
        pageList.addAll(newPages);
        writeObject(scalaris_key, pageList);
        writeObject(ScalarisDataHandler.getPageCountKey(), pageList.size());
        newPages = new LinkedList<String>();
        
        // list of articles:
        scalaris_key = ScalarisDataHandler.getArticleListKey();
        List<String> articleList;
        try {
            articleList = readObject(scalaris_key);
        } catch (FileNotFoundException e) {
            articleList = new LinkedList<String>();
        }
        articleList.addAll(newArticles);
        writeObject(scalaris_key, articleList);
        writeObject(ScalarisDataHandler.getArticleCountKey(), articleList.size());
        newArticles = new LinkedList<String>();
        
        // list of pages in each category:
        for (Entry<String, List<String>> category: newCategories.entrySet()) {
            scalaris_key = ScalarisDataHandler.getCatPageListKey(category.getKey());
            List<String> catPageList;
            try {
                catPageList = readObject(scalaris_key);
            } catch (FileNotFoundException e) {
                catPageList = new LinkedList<String>();
            }
            catPageList.addAll(category.getValue());
            writeObject(scalaris_key, catPageList);
        }
        newCategories = new HashMap<String, List<String>>(NEW_CATS_HASH_DEF_SIZE);

        // list of pages a template is used in:
        for (Entry<String, List<String>> template: newTemplates.entrySet()) {
            scalaris_key = ScalarisDataHandler.getTplPageListKey(template.getKey());
            List<String> tplPageList;
            try {
                tplPageList = readObject(scalaris_key);
            } catch (FileNotFoundException e) {
                tplPageList = new LinkedList<String>();
            }
            tplPageList.addAll(template.getValue());
            writeObject(scalaris_key, tplPageList);
        }
        newTemplates = new HashMap<String, List<String>>(NEW_TPLS_HASH_DEF_SIZE);
        
        // list of pages linking to other pages:
        for (Entry<String, List<String>> backlinks: newBackLinks.entrySet()) {
            scalaris_key = ScalarisDataHandler.getBackLinksPageListKey(backlinks.getKey());
            List<String> backLinksPageList;
            try {
                backLinksPageList = readObject(scalaris_key);
            } catch (FileNotFoundException e) {
                backLinksPageList = new LinkedList<String>();
            }
            backLinksPageList.addAll(backlinks.getValue());
            writeObject(scalaris_key, backLinksPageList);
        }
        newBackLinks = new HashMap<String, List<String>>(NEW_BLNKS_HASH_DEF_SIZE);
    }
    
    protected abstract void writeToScalaris();
    
    protected <T> void writeToScalaris(String key, T value) {
        ++pagesScalarisImport;
        requests.addWrite(key, value);
        // bundle 10 request
        if (requests.size() >= 10) {
            Runnable worker = new MyScalarisSingleRunnable(requests, scalaris_single, "keys up to " + key);
            executor.execute(worker);
            requests = new TransactionSingleOp.RequestList();
        }
        if ((pagesScalarisImport % PRINT_SCALARIS_KV_PAIRS_EVERY) == 0) {
            // wait for all threads to finish (otherwise we would take a lot of
            // memory, especially if the connection to Scalaris is slow)
            executor.shutdown();
            boolean shutdown = false;
            while (!shutdown) {
                try {
                    shutdown = executor.awaitTermination(1, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                }
            }
            executor = Executors.newFixedThreadPool(MAX_SCALARIS_CONNECTIONS);
            
            msgOut.println("imported K/V pairs to Scalaris: " + pagesScalarisImport);
        }
    }
}
