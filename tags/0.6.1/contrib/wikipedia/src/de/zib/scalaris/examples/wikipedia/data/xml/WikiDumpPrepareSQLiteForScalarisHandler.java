/**
 *  Copyright 2011-2013 Zuse Institute Berlin
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.ericsson.otp.erlang.OtpErlangObject;

import de.zib.scalaris.CommonErlangObjects;
import de.zib.scalaris.ErlangValue;
import de.zib.scalaris.examples.wikipedia.SQLiteDataHandler;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandlerNormalised;
import de.zib.scalaris.examples.wikipedia.bliki.MyNamespace.NamespaceEnum;
import de.zib.scalaris.examples.wikipedia.bliki.NormalisedTitle;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.ShortRevision;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;
import de.zib.tools.MultiHashMap;

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
        WikiDumpXml2SQLite.writeObject(stWrite, key,
                CommonErlangObjects.encode(ErlangValue.convertToErlang(value)));
    }

    /**
     * Encodes the given object to the erlang value used by Scalaris and
     * converts this value to bytes for use by SQLite.
     * 
     * @param value
     *            the object to convert
     * 
     * @return the byte array
     * 
     * @throws IOException
     * 
     * @see {@link #objectFromBytes(byte[])}
     */
    static <T> byte[] objectToBytes(T value) throws IOException {
        return WikiDumpXml2SQLite.objectToBytes(CommonErlangObjects
                .encode(ErlangValue.convertToErlang(value)));
    }

    /**
     * Reads an encoded Scalaris value from a (compressed) byte array.
     * 
     * @param value  the byte array to get the object from
     * 
     * @return the encoded object
     * 
     * @throws IOException
     * @throws ClassNotFoundException
     * 
     * @see #objectToBytes(Object)
     */
    static OtpErlangObject objectFromBytes(byte[] value) throws IOException,
            ClassNotFoundException {
        return WikiDumpXml2SQLite.<OtpErlangObject>objectFromBytes(value);
    }

    /**
     * Reads a compressed and encoded Scalaris value from a (compressed) byte array.
     * 
     * @param value  the byte array to get the object from
     * 
     * @return the decoded object
     * 
     * @throws IOException
     * @throws ClassNotFoundException
     * 
     * @see #objectToBytes(Object)
     */
    static ErlangValue objectFromBytes2(byte[] value) throws IOException,
            ClassNotFoundException {
        OtpErlangObject value2;
        try {
            value2 = CommonErlangObjects.decode(objectFromBytes(value));
        } catch (OtpErlangDecodeException e) {
            throw new RuntimeException(e);
        }
        return new ErlangValue(value2);
    }

    /**
     * Reads an Erlang-encoded, byte-encoded object from a SQLite DB.
     * 
     * @param stRead
     *            read statement
     * @param key
     *            key to read from
     * 
     * @return the Erlang-encoded object
     * 
     * @throws IOException
     * @throws FileNotFoundException
     */
    static OtpErlangObject readObject(SQLiteStatement stRead, String key)
            throws RuntimeException, FileNotFoundException {
        return WikiDumpXml2SQLite.<OtpErlangObject>readObject(stRead, key);
    }

    /**
     * Reads an Erlang-encoded, byte-encoded object from a SQLite DB.
     * 
     * @param stRead
     *            read statement
     * @param key
     *            key to read from
     * 
     * @return the decoded object
     * 
     * @throws IOException
     * @throws FileNotFoundException
     */
    static ErlangValue readObject2(SQLiteStatement stRead, String key)
            throws RuntimeException, FileNotFoundException {
        try {
            OtpErlangObject value = CommonErlangObjects.decode(readObject(
                    stRead, key));
            return new ErlangValue(value);
        } catch (FileNotFoundException e) {
            throw e;
        } catch (OtpErlangDecodeException e) {
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
            List<ShortRevision> revisions_short, NormalisedTitle title)
            throws UnsupportedOperationException {
        for (Revision rev : revisions) {
            if (rev.getId() != page.getCurRev().getId()) {
                addSQLiteJob(new SQLiteWriteObjectJob<Revision>(
                        ScalarisDataHandlerNormalised.getRevKey(title,
                                rev.getId()), rev, stWrite));
            }
        }
        addSQLiteJob(new SQLiteWriteObjectJob<List<ShortRevision>>(
                ScalarisDataHandlerNormalised.getRevListKey(title),
                revisions_short, stWrite));
        addSQLiteJob(new SQLiteWriteObjectJob<Page>(
                ScalarisDataHandlerNormalised.getPageKey(title), page, stWrite));

        // note: do not normalise page titles (this will be done later)
        newPages.get(NamespaceEnum.fromId(title.namespace)).add(title);
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
                    // set 1GB cache_size:
                    db = SQLiteDataHandler.openDB(dbFileName, false, 1024l*1024l*1024l);
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
                if (db != null) {
                    try {
                        db.exec("DROP TABLE pages;");
                        db.exec("DROP TABLE categories;");
                        db.exec("DROP TABLE templates;");
                        db.exec("DROP TABLE links;");
                    } catch (SQLiteException e) {
                        throw new RuntimeException(e);
                    }
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

        protected OtpErlangObject readObject(String key)
                throws RuntimeException, FileNotFoundException {
            // Note: need to qualify static function call due to
            // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6302954
            return WikiDumpPrepareSQLiteForScalarisHandler.readObject(stRead, key);
        }

        protected ErlangValue readObject2(String key)
                throws RuntimeException, FileNotFoundException {
            // Note: need to qualify static function call due to
            // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6302954
            return WikiDumpPrepareSQLiteForScalarisHandler.readObject2(stRead, key);
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
         *            page title
         * 
         * @return the ID of the page
         * 
         * @throws RuntimeException
         */
        protected long pageToId(NormalisedTitle pageTitle) throws RuntimeException {
            String pageTitle2 = pageTitle.toString();
            try {
                long pageId = -1;
                // try to find the page id in the pages table:
                try {
                    stGetTmpPageId.bind(1, pageTitle2);
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
                        stWriteTmpPages.bind(1, pageId).bind(2, pageTitle2).stepThrough();
                    } finally {
                        stWriteTmpPages.reset();
                    }
                }
                return pageId;
            } catch (SQLiteException e) {
                error("write of " + pageTitle + " failed (sqlite error: " + e.toString() + ")");
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
        EnumMap<NamespaceEnum, ArrayList<NormalisedTitle>> newPages;
        int articleCount;
        
        /**
         * Writes the page and article list to the DB.
         * 
         * @param newPages
         *            list of page titles
         * @param articleCount
         *            number of articles
         */
        public SQLiteUpdatePageLists1Job(EnumMap<NamespaceEnum, ArrayList<NormalisedTitle>> newPages, int articleCount) {
            this.newPages = newPages;
            this.articleCount = articleCount;
        }
        
        @Override
        public void run() {
            String scalaris_key;
            
            // list of pages:
            for(NamespaceEnum ns : NamespaceEnum.values()) {
                scalaris_key = ScalarisDataHandler.getPageListKey(ns.getId());
                final List<String> curNewPages = ScalarisDataHandlerNormalised
                        .normList2normStringList(newPages.get(ns));
                List<String> pageList;
                try {
                    pageList = readObject2(scalaris_key).stringListValue();
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
        MultiHashMap<NormalisedTitle, NormalisedTitle> newCategories;
        MultiHashMap<NormalisedTitle, NormalisedTitle> newTemplates;
        MultiHashMap<NormalisedTitle, NormalisedTitle> newBackLinks;
        
        /**
         * Writes (temporary) page list mappings to the DB.
         * 
         * @param newCategories
         *            category mappings
         * @param newTemplates
         *            template mappings
         * @param newBackLinks
         *            link mappings
         */
        public SQLiteUpdateTmpPageLists2Job(
                MultiHashMap<NormalisedTitle, NormalisedTitle> newCategories,
                MultiHashMap<NormalisedTitle, NormalisedTitle> newTemplates,
                MultiHashMap<NormalisedTitle, NormalisedTitle> newBackLinks) {
            this.newCategories = newCategories;
            this.newTemplates = newTemplates;
            this.newBackLinks = newBackLinks;
        }
        
        protected void addToList(SQLiteStatement stmt, NormalisedTitle key, Collection<? extends NormalisedTitle> values) {
            long key_id = pageToId(key);
            ArrayList<Long> values_id = new ArrayList<Long>(values.size());
            for (NormalisedTitle value : values) {
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
                error("write of " + key + " failed (sqlite error: " + e.toString() + ")");
                throw new RuntimeException(e);
            }
        }
        
        @Override
        public void run() {
            // list of pages in each category:
            for (Entry<NormalisedTitle, List<NormalisedTitle>> category: newCategories.entrySet()) {
                addToList(stWriteTmpCategories, category.getKey(), category.getValue());
            }
        
            // list of pages a template is used in:
            for (Entry<NormalisedTitle, List<NormalisedTitle>> template: newTemplates.entrySet()) {
                addToList(stWriteTmpTemplates, template.getKey(), template.getValue());
            }
            
            // list of pages linking to other pages:
            for (Entry<NormalisedTitle, List<NormalisedTitle>> backlinks: newBackLinks.entrySet()) {
                addToList(stWriteTmpLinks, backlinks.getKey(), backlinks.getValue());
            }
        }
    }
    
    private static enum ListType {
        CAT_LIST, TPL_LIST, LNK_LIST;
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
                            "INNER JOIN pages AS page ON categories.page == page.id ORDER BY cat.title;");
                    writeToScalarisKV(stmt, ListType.CAT_LIST);
                    stmt.dispose();
                } while (false);

                // list of pages a template is used in:
                do {
                    println("  creating template lists");
                    stmt = db.prepare("SELECT tpl.title, page.title FROM templates as templates " +
                            "INNER JOIN pages AS tpl ON templates.template == tpl.id " +
                            "INNER JOIN pages AS page ON templates.page == page.id ORDER BY tpl.title;");
                    writeToScalarisKV(stmt, ListType.TPL_LIST);
                    stmt.dispose();
                } while (false);

                // list of pages linking to other pages:
                do {
                    println("  creating backlink lists");
                    stmt = db.prepare("SELECT lnkDest.title, lnkSrc.title FROM links as links " +
                            "INNER JOIN pages AS lnkDest ON links.lnkDest == lnkDest.id " +
                            "INNER JOIN pages AS lnkSrc ON links.lnkSrc == lnkSrc.id ORDER BY lnkDest.title;");
                    writeToScalarisKV(stmt, ListType.LNK_LIST);
                    stmt.dispose();
                } while (false);
            } catch (SQLiteException e) {
                error("sqlite error: " + e.toString());
                throw new RuntimeException(e);
            } finally {
                if (stmt != null) {
                    stmt.dispose();
                }
            }
        }
        
        /**
         * Aggregates page lists from a 2-column table mapping a list key to a
         * list content.
         * 
         * @param stmt
         *            the statement to read from
         * @param listType
         *            the type of list
         * 
         * @throws SQLiteException
         * @throws RuntimeException
         */
        private void writeToScalarisKV(SQLiteStatement stmt, ListType listType) throws SQLiteException, RuntimeException {
            String listKey = null;
            List<String> pageList = new ArrayList<String>(1000);
            while (stmt.step()) {
                // note: both titles are already normalised!
                final String stmtKey = stmt.columnString(0);
                final String stmtPage = stmt.columnString(1);
                if (listKey == null) {
                    // first row
                    listKey = stmtKey;
                    pageList.add(stmtPage);
                } else if (listKey.equals(stmtKey)) {
                    // next item with the same list key
                    pageList.add(stmtPage);
                } else {
                    // new item, i.e. different list key
                    // -> write old list, then accumulate new
                    writeToScalarisKV(NormalisedTitle.fromNormalised(listKey), pageList, listType);
                    listKey = stmtKey;
                    pageList.add(stmtPage);
                }
            }
            if (listKey != null && !pageList.isEmpty()) {
                writeToScalarisKV(NormalisedTitle.fromNormalised(listKey), pageList, listType);
            }
        }

        private void writeToScalarisKV(NormalisedTitle listKey, List<String> pageList,
                ListType listType) throws RuntimeException {
            switch (listType) {
            case CAT_LIST:
                writeCatToScalarisKV(listKey, pageList);
                break;
            case TPL_LIST:
                writeTplToScalarisKV(listKey, pageList);
                break;
            case LNK_LIST:
                writeLnkToScalarisKV(listKey, pageList);
                break;
            }
        }

        private void writeCatToScalarisKV(NormalisedTitle category0,
                List<String> catPageList) throws RuntimeException {
            // note: titles in the page list are already normalised!
            String scalaris_key;
            scalaris_key = ScalarisDataHandlerNormalised.getCatPageListKey(category0);
            writeObject(scalaris_key, catPageList);
            scalaris_key = ScalarisDataHandlerNormalised.getCatPageCountKey(category0);
            writeObject(scalaris_key, catPageList.size());
            catPageList.clear();
        }

        private void writeTplToScalarisKV(NormalisedTitle template0,
                List<String> tplPageList) throws RuntimeException {
            // note: titles in the page list are already normalised!
            String scalaris_key;
            scalaris_key = ScalarisDataHandlerNormalised.getTplPageListKey(template0);
            writeObject(scalaris_key, tplPageList);
            tplPageList.clear();
        }

        private void writeLnkToScalarisKV(NormalisedTitle linkDest0,
                List<String> backLinksPageList) throws RuntimeException {
            // note: titles in the page list are already normalised!
            String scalaris_key;
            scalaris_key = ScalarisDataHandlerNormalised.getBackLinksPageListKey(linkDest0);
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
