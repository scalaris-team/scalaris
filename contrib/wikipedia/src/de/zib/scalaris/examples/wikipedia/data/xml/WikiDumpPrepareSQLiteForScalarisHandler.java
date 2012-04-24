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
package de.zib.scalaris.examples.wikipedia.data.xml;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler;
import de.zib.scalaris.examples.wikipedia.bliki.MyNamespace.NamespaceEnum;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.ShortRevision;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;

/**
 * Provides abilities to read an xml wiki dump file and prepare its contents
 * for Scalaris by creating key/value pairs in a local SQLite db.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiDumpPrepareSQLiteForScalarisHandler extends WikiDumpPageHandler {
    protected SQLiteConnection db = null;
    protected SQLiteStatement stGetTmpPageId = null;
    protected SQLiteStatement stWriteTmpPages = null;
    protected SQLiteStatement stWriteTmpCategories = null;
    protected SQLiteStatement stWriteTmpTemplates = null;
    protected SQLiteStatement stWriteTmpLinks = null;
    protected SQLiteStatement stRead = null;
    protected SQLiteStatement stWrite = null;
    protected long nextPageId = 0l;
    protected String dbFileName;
    protected ArrayBlockingQueue<Runnable> sqliteJobs = new ArrayBlockingQueue<Runnable>(UPDATE_PAGELIST_EVERY);
    SQLiteWorker sqliteWorker = new SQLiteWorker();
    
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
     * @param minTime
     *            minimum time a revision should have (only one revision older
     *            than this will be imported) - <tt>null/tt> imports all
     *            revisions
     * @param maxTime
     *            maximum time a revision should have (newer revisions are
     *            omitted) - <tt>null/tt> imports all revisions
     *            (useful to create dumps of a wiki at a specific point in time)
     * @param dbFileName
     *            the name of the database file to write to
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     */
    public WikiDumpPrepareSQLiteForScalarisHandler(Set<String> blacklist,
            Set<String> whitelist, int maxRevisions, Calendar minTime,
            Calendar maxTime, String dbFileName) throws RuntimeException {
        super(blacklist, whitelist, maxRevisions, minTime, maxTime);
        this.dbFileName = dbFileName;
    }

    /**
     * Opens a connection to a database and sets some default PRAGMAs for better
     * performance in our case.
     * 
     * @param fileName
     *            the name of the DB file
     * @param readOnly
     *            whether to open the DB read-only or not
     * 
     * @return the DB connection
     * 
     * @throws SQLiteException
     *             if the connection fails or a pragma could not be set
     */
    static SQLiteConnection openDB(String fileName, boolean readOnly) throws SQLiteException {
        // set 1GB cache_size:
        return openDB(fileName, readOnly, 1024l*1024l*1024l);
    }

    /**
     * Opens a connection to a database and sets some default PRAGMAs for better
     * performance in our case.
     * 
     * @param fileName
     *            the name of the DB file
     * @param readOnly
     *            whether to open the DB read-only or not
     * 
     * @return the DB connection
     * 
     * @throws SQLiteException
     *             if the connection fails or a pragma could not be set
     */
    static SQLiteConnection openDB(String fileName, boolean readOnly, Long cacheSize) throws SQLiteException {
        SQLiteConnection db = new SQLiteConnection(new File(fileName));
        if (readOnly) {
            db.openReadonly();
        } else {
            db.open(true);
        }
        // set cache_size:
        if (cacheSize != null) {
            final SQLiteStatement stmt = db.prepare("PRAGMA page_size;");
            if (stmt.step()) {
                long pageSize = stmt.columnLong(0);
                db.exec("PRAGMA cache_size = " + (cacheSize / pageSize) + ";");
            }
            stmt.dispose();
        }
        db.exec("PRAGMA synchronous = OFF;");
        db.exec("PRAGMA journal_mode = OFF;");
//        db.exec("PRAGMA locking_mode = EXCLUSIVE;");
        db.exec("PRAGMA case_sensitive_like = true;"); 
        db.exec("PRAGMA encoding = 'UTF-8';"); 
        db.exec("PRAGMA temp_store = MEMORY;"); 
        return db;
    }

    static SQLiteStatement createReadStmt(SQLiteConnection db) throws SQLiteException {
        return db.prepare("SELECT scalaris_value FROM objects WHERE scalaris_key == ?");
    }

    static SQLiteStatement createWriteStmt(SQLiteConnection db) throws SQLiteException {
        return db.prepare("REPLACE INTO objects (scalaris_key, scalaris_value) VALUES (?, ?);");
    }

    /**
     * @param <T>
     * @param siteinfo
     * @param key
     * @throws IOException
     * @throws FileNotFoundException
     */
    static <T> void writeObject(SQLiteStatement stWrite, String key, T value)
            throws RuntimeException {
        try {
            try {
                stWrite.bind(1, key).bind(2, objectToBytes(value)).stepThrough();
            } finally {
                stWrite.reset();
            }
        } catch (SQLiteException e) {
            System.err.println("write of " + key + " failed (sqlite error: " + e.toString() + ")");
        } catch (IOException e) {
            System.err.println("write of " + key + " failed");
        }
    }

    /**
     * Converts an object to a (compressed) byte array.
     * 
     * @param value  the object to convert
     * 
     * @return the byte array
     * 
     * @throws IOException
     * 
     * @see {@link #objectFromBytes(byte[])}
     */
    static <T> byte[] objectToBytes(T value) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(bos));
        oos.writeObject(value);
        oos.flush();
        oos.close();
        return bos.toByteArray();
    }

    /**
     * Reads an object from a (compressed) byte array.
     * 
     * @param value  the byte array to get the object from
     * 
     * @return the object
     * 
     * @throws IOException
     * @throws ClassNotFoundException
     * 
     * @see #objectToBytes(Object)
     */
    static <T> T objectFromBytes(byte[] value) throws IOException,
            ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(
                new GZIPInputStream(new ByteArrayInputStream(value)));
        @SuppressWarnings("unchecked")
        T result = (T) ois.readObject();
        ois.close();
        return result;
    }

    /**
     * Note: may need to qualify static function call due to
     *  http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6302954
     * <tt>WikiDumpPrepareSQLiteForScalarisHandler.<T>readObject(stRead, key)</tt>
     * 
     * @param <T>
     * @param siteinfo
     * @param key
     * @throws IOException
     * @throws FileNotFoundException
     */
    static <T> T readObject(SQLiteStatement stRead, String key)
            throws RuntimeException, FileNotFoundException {
        try {
            try {
                stRead.bind(1, key);
                if (stRead.step()) {
                    // there should only be one result
                    byte[] value = stRead.columnBlob(0);
                    return objectFromBytes(value);
                } else {
                    throw new FileNotFoundException();
                }
            } finally {
                stRead.reset();
            }
        } catch (FileNotFoundException e) {
            throw e;
        } catch (SQLiteException e) {
            System.err.println("read of " + key + " failed (sqlite error: " + e.toString() + ")");
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            System.err.println("read of " + key + " failed (error: " + e.toString() + ")");
            throw new RuntimeException(e);
        } catch (IOException e) {
            System.err.println("read of " + key + " failed (error: " + e.toString() + ")");
            throw new RuntimeException(e);
        }
    }
    
    protected void addSQLiteJob(Runnable job) throws RuntimeException {
        try {
            sqliteJobs.put(job);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpPrepareForScalarisHandler#setUp()
     */
    @Override
    public void setUp() {
        super.setUp();
        println("Pre-processing pages to key/value pairs...");
        sqliteWorker.start();
        // wait for worker to initialise the DB and the prepared statements
        while (!sqliteWorker.initialised) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpHandler#tearDown()
     */
    @Override
    public void tearDown() {
        super.tearDown();
        updatePageList();
        updateLinkLists();
        addSQLiteJob(new SQLiteConvertTmpPageLists2Job());
        sqliteWorker.stopWhenQueueEmpty = true;
        addSQLiteJob(new SQLiteNoOpJob());
        // wait for worker to close the DB
        try {
            sqliteWorker.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        importEnd();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#finalize()
     */
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        db.dispose();
    }

    @Override
    protected void doExport(SiteInfo siteinfo) throws RuntimeException {
        addSQLiteJob(new SQLiteWriteSiteInfoJob(siteinfo, stWrite));
    }

    @Override
    protected void doExport(Page page, List<Revision> revisions,
            List<ShortRevision> revisions_short, NamespaceEnum namespace)
            throws UnsupportedOperationException {
        for (Revision rev : revisions) {
            if (rev.getId() != page.getCurRev().getId()) {
                addSQLiteJob(new SQLiteWriteObjectJob<Revision>(
                        ScalarisDataHandler.getRevKey(page.getTitle(),
                                rev.getId(), wikiModel.getNamespace()), rev,
                        stWrite));
            }
        }
        addSQLiteJob(new SQLiteWriteObjectJob<List<ShortRevision>>(
                ScalarisDataHandler.getRevListKey(page.getTitle(), wikiModel.getNamespace()),
                revisions_short, stWrite));
        addSQLiteJob(new SQLiteWriteObjectJob<Page>(
                ScalarisDataHandler.getPageKey(page.getTitle(), wikiModel.getNamespace()),
                page, stWrite));

        // note: do not normalise page titles (this will be done later)
        newPages.get(namespace).add(page.getTitle());
        // only export page list every UPDATE_PAGELIST_EVERY pages:
        if ((pageCount % UPDATE_PAGELIST_EVERY) == 0) {
            updatePageList();
        }
        // limit the number of changes in SQLiteUpdatePageLists2Job to
        // UPDATE_PAGELIST_EVERY
        if ((newCategories.size() + newTemplates.size() + newBackLinks.size()) >= UPDATE_PAGELIST_EVERY) {
            updateLinkLists();
        }
    }
    
    protected void updatePageList() {
        addSQLiteJob(new SQLiteUpdatePageLists1Job(newPages, articleCount));
        initNewPagesList();
    }
    
    protected void updateLinkLists() {
        addSQLiteJob(new SQLiteUpdateTmpPageLists2Job(newCategories, newTemplates, newBackLinks));
        initLinkLists();
    }
    
    protected class SQLiteWorker extends Thread {
        boolean stopWhenQueueEmpty = false;
        boolean initialised = false;
        
        @Override
        public void run() {
            try {
                // set up DB:
                try {
                    db = openDB(dbFileName, false);
                    db.exec("CREATE TABLE objects(scalaris_key STRING PRIMARY KEY ASC, scalaris_value);");
                    db.exec("CREATE TEMPORARY TABLE pages(id INTEGER PRIMARY KEY ASC, title STRING);");
                    db.exec("CREATE INDEX page_titles ON pages(title);");
                    db.exec("CREATE TEMPORARY TABLE categories(category INTEGER, page INTEGER);");
                    db.exec("CREATE TEMPORARY TABLE templates(template INTEGER, page INTEGER);");
                    db.exec("CREATE TEMPORARY TABLE links(lnkDest INTEGER, lnkSrc INTEGER);");
                    stGetTmpPageId = db.prepare("SELECT id FROM pages WHERE title == ?;");
                    stWriteTmpPages = db.prepare("INSERT INTO pages (id, title) VALUES (?, ?);");
                    stWriteTmpCategories = db.prepare("INSERT INTO categories (category, page) VALUES (?, ?);");
                    stWriteTmpTemplates = db.prepare("INSERT INTO templates (template, page) VALUES (?, ?);");
                    stWriteTmpLinks = db.prepare("INSERT INTO links (lnkDest, lnkSrc) VALUES (?, ?);");
                    stRead = createReadStmt(db);
                    stWrite = createWriteStmt(db);
                } catch (SQLiteException e) {
                    throw new RuntimeException(e);
                }
                initialised = true;

                // take jobs

                while(!(sqliteJobs.isEmpty() && stopWhenQueueEmpty)) {
                    Runnable job;
                    try {
                        job = sqliteJobs.take();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    job.run();
                }

            } finally {
                if (stGetTmpPageId != null) {
                    stGetTmpPageId.dispose();
                }
                if (stWriteTmpPages != null) {
                    stWriteTmpPages.dispose();
                }
                if (stWriteTmpCategories != null) {
                    stWriteTmpCategories.dispose();
                }
                if (stWriteTmpTemplates != null) {
                    stWriteTmpTemplates.dispose();
                }
                if (stWriteTmpLinks != null) {
                    stWriteTmpLinks.dispose();
                }
                if (stRead != null) {
                    stRead.dispose();
                }
                if (stWrite != null) {
                    stWrite.dispose();
                }
                try {
                    db.exec("DROP TABLE pages;");
                    db.exec("DROP TABLE categories;");
                    db.exec("DROP TABLE templates;");
                    db.exec("DROP TABLE links;");
                } catch (SQLiteException e) {
                    throw new RuntimeException(e);
                }
                if (db != null) {
                    db.dispose();
                }
                initialised = false;
            }
        }
    }
    
    static class SQLiteNoOpJob implements Runnable {
        @Override
        public void run() {
        }
    }
    
    static class SQLiteWriteObjectJob<T> implements Runnable {
        protected final String key;
        protected final T value;
        protected final SQLiteStatement stWrite;
        
        public SQLiteWriteObjectJob(String key, T value, SQLiteStatement stWrite) {
            this.key = key;
            this.value = value;
            this.stWrite = stWrite;
        }
        
        @Override
        public void run() {
            writeObject(stWrite, key, value);
        }
    }
    
    abstract protected class SQLiteUpdatePageListsJob implements Runnable {

        protected <T> T readObject(String key)
                throws RuntimeException, FileNotFoundException {
            // Note: need to qualify static function call due to
            // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6302954
            return WikiDumpPrepareSQLiteForScalarisHandler.<T>readObject(stRead, key);
        }

        protected <T> void writeObject(String key, T value)
                throws RuntimeException {
            WikiDumpPrepareSQLiteForScalarisHandler.writeObject(stWrite, key, value);
        }
        
        /**
         * Converts a page to an integer ID and inserts the name into the
         * (temporary) page table.
         * 
         * @param pageTitle
         *            (un-normalised) page title
         * 
         * @return the ID of the page
         * 
         * @throws RuntimeException
         */
        protected long pageToId(String pageTitle) throws RuntimeException {
            try {
                long pageId = -1;
                // try to find the page id in the pages table:
                try {
                    stGetTmpPageId.bind(1, pageTitle);
                    if (stGetTmpPageId.step()) {
                        pageId = stGetTmpPageId.columnLong(0);
                    }
                } finally {
                    stGetTmpPageId.reset();
                }
                // page not found yet -> add to pages table:
                if (pageId == -1) {
                    pageId = nextPageId++;
                    try {
                        stWriteTmpPages.bind(1, pageId).bind(2, pageTitle).stepThrough();
                    } finally {
                        stWriteTmpPages.reset();
                    }
                }
                return pageId;
            } catch (SQLiteException e) {
                System.err.println("write of " + pageTitle + " failed (sqlite error: " + e.toString() + ")");
                throw new RuntimeException(e);
            }
        }
    }
    
    /**
     * Updates the pages and articles page lists.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    protected class SQLiteUpdatePageLists1Job extends SQLiteUpdatePageListsJob {
        EnumMap<NamespaceEnum, ArrayList<String>> newPages;
        int articleCount;
        
        /**
         * Writes the page and article list to the DB.
         * 
         * @param newPages
         *            (un-normalised) list of page titles
         * @param articleCount
         *            number of articles
         */
        public SQLiteUpdatePageLists1Job(EnumMap<NamespaceEnum, ArrayList<String>> newPages, int articleCount) {
            // NOTE: can only use the wikiModel member here in order not to
            // influence other parsing operations
            this.newPages = createNewPagesList();
            this.articleCount = articleCount;
            for(NamespaceEnum ns : NamespaceEnum.values()) {
                wikiModel.normalisePageTitles(newPages.get(ns), this.newPages.get(ns));
            }
        }
        
        @Override
        public void run() {
            String scalaris_key;
            
            // list of pages:
            for(NamespaceEnum ns : NamespaceEnum.values()) {
                scalaris_key = ScalarisDataHandler.getPageListKey(ns.getId());
                final ArrayList<String> curNewPages = newPages.get(ns);
                List<String> pageList;
                try {
                    pageList = readObject(scalaris_key);
                    pageList.addAll(curNewPages);
                } catch (FileNotFoundException e) {
                    pageList = curNewPages;
                }
                writeObject(scalaris_key, pageList);
                writeObject(ScalarisDataHandler.getPageCountKey(ns.getId()), pageList.size());
            }
            
            // number articles:
            writeObject(ScalarisDataHandler.getArticleCountKey(), articleCount);
        }
    }

    /**
     * Updates the categories, templates and backlinks page lists.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    protected class SQLiteUpdateTmpPageLists2Job extends SQLiteUpdatePageListsJob {
        HashMap<String, List<String>> newCategories;
        HashMap<String, List<String>> newTemplates;
        HashMap<String, List<String>> newBackLinks;
        
        /**
         * Writes (temporary) page list mappings to the DB.
         * 
         * @param newCategories
         *            (un-normalised) category mappings
         * @param newTemplates
         *            (un-normalised) template mappings
         * @param newBackLinks
         *            (un-normalised) link mappings
         */
        public SQLiteUpdateTmpPageLists2Job(
                HashMap<String, List<String>> newCategories,
                HashMap<String, List<String>> newTemplates,
                HashMap<String, List<String>> newBackLinks) {
            this.newCategories = newCategories;
            this.newTemplates = newTemplates;
            this.newBackLinks = newBackLinks;
        }
        
        protected void addToList(SQLiteStatement stmt, String key, Collection<? extends String> values) {
            long key_id = pageToId(key);
            ArrayList<Long> values_id = new ArrayList<Long>(values.size());
            for (String value : values) {
                values_id.add(pageToId(value));
            }
            try {
                try {
                    stmt.bind(1, key_id);
                    for (Long value_id : values_id) {
                        stmt.bind(2, value_id).stepThrough().reset(false);
                    }
                } finally {
                    stmt.reset();
                }
            } catch (SQLiteException e) {
                System.err.println("write of " + key + " failed (sqlite error: " + e.toString() + ")");
                throw new RuntimeException(e);
            }
        }
        
        @Override
        public void run() {
            // list of pages in each category:
            for (Entry<String, List<String>> category: newCategories.entrySet()) {
                addToList(stWriteTmpCategories, category.getKey(), category.getValue());
            }
        
            // list of pages a template is used in:
            for (Entry<String, List<String>> template: newTemplates.entrySet()) {
                addToList(stWriteTmpTemplates, template.getKey(), template.getValue());
            }
            
            // list of pages linking to other pages:
            for (Entry<String, List<String>> backlinks: newBackLinks.entrySet()) {
                addToList(stWriteTmpLinks, backlinks.getKey(), backlinks.getValue());
            }
        }
    }

    /**
     * Updates the categories, templates and backlinks page lists.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    protected class SQLiteConvertTmpPageLists2Job extends SQLiteUpdatePageListsJob {
        public SQLiteConvertTmpPageLists2Job() {
        }
        
        @Override
        public void run() {
            // NOTE: need to normalise every page title!!
            SQLiteStatement stmt = null;
            try {
                // list of pages in each category:
                do {
                    println("  creating category lists");
                    stmt = db.prepare("SELECT cat.title, page.title FROM categories as categories " +
                            "INNER JOIN pages AS cat ON categories.category == cat.id " +
                            "INNER JOIN pages AS page ON categories.page == page.id;");
                    String category0 = null;
                    List<String> catPageList = new ArrayList<String>(1000);
                    while (stmt.step()) {
                        final String stmtCat0 = stmt.columnString(0);
                        final String stmtPage0 = stmt.columnString(1);
                        final String stmtPage = wikiModel.normalisePageTitle(stmtPage0);
                        if (category0 == null) {
                            category0 = stmtCat0;
                            catPageList.add(stmtPage);
                        } else if (category0.equals(stmtCat0)) {
                            catPageList.add(stmtPage);
                        } else {
                            // write old, accumulate new
                            writeCatToScalaris(category0, catPageList);
                            category0 = stmtCat0;
                            catPageList.add(stmtPage);
                        }
                    }
                    if (category0 != null && !catPageList.isEmpty()) {
                        writeCatToScalaris(category0, catPageList);
                    }
                    stmt.dispose();
                } while (false);

                // list of pages a template is used in:
                do {
                    println("  creating template lists");
                    stmt = db.prepare("SELECT tpl.title, page.title FROM templates as templates " +
                            "INNER JOIN pages AS tpl ON templates.template == tpl.id " +
                            "INNER JOIN pages AS page ON templates.page == page.id;");
                    String template0 = null;
                    List<String> tplPageList = new ArrayList<String>(1000);
                    while (stmt.step()) {
                        final String stmtTpl0 = stmt.columnString(0);
                        final String stmtPage0 = stmt.columnString(1);
                        final String stmtPage = wikiModel.normalisePageTitle(stmtPage0);
                        if (template0 == null) {
                            template0 = stmtTpl0;
                            tplPageList.add(stmtPage);
                        } else if (template0.equals(stmtTpl0)) {
                            tplPageList.add(stmtPage);
                        } else {
                            // write old, accumulate new
                            writeTplToScalaris(template0, tplPageList);
                            template0 = stmtTpl0;
                            tplPageList.add(stmtPage);
                        }
                    }
                    if (template0 != null && !tplPageList.isEmpty()) {
                        writeTplToScalaris(template0, tplPageList);
                    }
                    stmt.dispose();
                } while (false);

                // list of pages linking to other pages:
                do {
                    println("  creating backlink lists");
                    stmt = db.prepare("SELECT lnkDest.title, lnkSrc.title FROM links as links " +
                            "INNER JOIN pages AS lnkDest ON links.lnkDest == lnkDest.id " +
                            "INNER JOIN pages AS lnkSrc ON links.lnkSrc == lnkSrc.id;");
                    String linkDest0 = null;
                    List<String> backLinksPageList = new ArrayList<String>(1000);
                    while (stmt.step()) {
                        final String stmtLnkDest0 = stmt.columnString(0);
                        final String stmtLnkSrc0 = stmt.columnString(1);
                        final String stmtLnkSrc = wikiModel.normalisePageTitle(stmtLnkSrc0);
                        if (linkDest0 == null) {
                            linkDest0 = stmtLnkDest0;
                            backLinksPageList.add(stmtLnkSrc);
                        } else if (linkDest0.equals(stmtLnkDest0)) {
                            backLinksPageList.add(stmtLnkSrc);
                        } else {
                            // write old, accumulate new
                            writeLnkToScalaris(linkDest0, backLinksPageList);
                            linkDest0 = stmtLnkDest0;
                            backLinksPageList.add(stmtLnkSrc);
                        }
                    }
                    if (linkDest0 != null && !backLinksPageList.isEmpty()) {
                        writeLnkToScalaris(linkDest0, backLinksPageList);
                    }
                    stmt.dispose();
                } while (false);
            } catch (SQLiteException e) {
                System.err.println("sqlite error: " + e.toString());
                throw new RuntimeException(e);
            } finally {
                if (stmt != null) {
                    stmt.dispose();
                }
            }
        }

        /**
         * @param category0
         * @param catPageList
         * @throws RuntimeException
         */
        private void writeCatToScalaris(String category0,
                List<String> catPageList) throws RuntimeException {
            String scalaris_key;
            scalaris_key = ScalarisDataHandler.getCatPageListKey(category0, wikiModel.getNamespace());
            writeObject(scalaris_key, catPageList);
            scalaris_key = ScalarisDataHandler.getCatPageCountKey(category0, wikiModel.getNamespace());
            writeObject(scalaris_key, catPageList.size());
            catPageList.clear();
        }

        /**
         * @param template0
         * @param tplPageList
         * @throws RuntimeException
         */
        private void writeTplToScalaris(String template0,
                List<String> tplPageList) throws RuntimeException {
            String scalaris_key;
            scalaris_key = ScalarisDataHandler.getTplPageListKey(template0, wikiModel.getNamespace());
            writeObject(scalaris_key, tplPageList);
            tplPageList.clear();
        }

        /**
         * @param linkDest0
         * @param backLinksPageList
         * @throws RuntimeException
         */
        private void writeLnkToScalaris(String linkDest0,
                List<String> backLinksPageList) throws RuntimeException {
            String scalaris_key;
            scalaris_key = ScalarisDataHandler.getBackLinksPageListKey(linkDest0, wikiModel.getNamespace());
            writeObject(scalaris_key, backLinksPageList);
            backLinksPageList.clear();
        }
    }
    
    protected static class SQLiteWriteSiteInfoJob implements Runnable {
        SiteInfo siteInfo;
        protected SQLiteStatement stWrite;
        
        public SQLiteWriteSiteInfoJob(SiteInfo siteInfo, SQLiteStatement stWrite) {
            this.siteInfo = siteInfo;
            this.stWrite = stWrite;
        }
        
        @Override
        public void run() {
            String key = ScalarisDataHandler.getSiteInfoKey();
            writeObject(stWrite, key, siteInfo);
        }
    }
}
