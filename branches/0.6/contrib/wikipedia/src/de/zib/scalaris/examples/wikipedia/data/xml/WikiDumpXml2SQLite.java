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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Calendar;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import de.zib.scalaris.examples.wikipedia.SQLiteDataHandler;
import de.zib.scalaris.examples.wikipedia.bliki.NormalisedTitle;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;

/**
 * Provides abilities to read an xml wiki dump file and write its contents into
 * a local SQLite db.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiDumpXml2SQLite extends WikiDumpHandler {
    private static final int PRINT_PAGES_EVERY = 500;
    protected SQLiteConnection db = null;
    protected SQLiteStatement stWritePage = null;
    protected SQLiteStatement stWriteRevision = null;
    protected SQLiteStatement stWriteText = null;
    final protected String dbFileName;
    protected long nextPageId = 0l;
    protected ArrayBlockingQueue<SQLiteJob> sqliteJobs = new ArrayBlockingQueue<SQLiteJob>(10);
    SQLiteWorker sqliteWorker = new SQLiteWorker();
    private SiteInfo siteInfo = null;
    
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
    public WikiDumpXml2SQLite(Set<String> blacklist,
            Set<String> whitelist, int maxRevisions, Calendar minTime,
            Calendar maxTime, String dbFileName) throws RuntimeException {
        super(blacklist, whitelist, maxRevisions, minTime, maxTime);
        this.dbFileName = dbFileName;
    }
    
    protected void addSQLiteJob(SQLiteJob job) throws RuntimeException {
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
        println("Pre-processing pages from XML to SQLite DB...");
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
    protected void export(XmlSiteInfo siteinfo_xml) {
        this.siteInfo  = siteinfo_xml.getSiteInfo();
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
            throw new RuntimeException(e);
        } catch (IOException e) {
            System.err.println("write of " + key + " failed");
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads a byte-encoded object from a SQLite DB.
     * 
     * Note: may need to qualify static function call due to
     * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6302954
     * <tt>WikiDumpXml2SQLite.<T>readObject(stRead, key)</tt>
     * 
     * @param <T>
     *            the type to convert the object to
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
    static <T> T readObject(SQLiteStatement stRead, String key)
            throws RuntimeException, FileNotFoundException {
        try {
            try {
                stRead.bind(1, key);
                if (stRead.step()) {
                    // there should only be one result
                    byte[] value = stRead.columnBlob(0);
                    return WikiDumpXml2SQLite.<T>objectFromBytes(value);
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

    /**
     * Converts this value to bytes for use by SQLite.
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
     * Note: may need to qualify static function call due to
     *  http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6302954
     * <tt>WikiDumpXml2SQLite.<T>objectFromBytes(value)</tt>
     * 
     * @param <T>
     *            the type to convert the object to
     * 
     * @param value
     *            the byte array to get the object from
     * 
     * @return the encoded object
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
     * Reads the site info object from the given DB.
     * 
     * @param db
     *            the DB which was previously prepared with this class
     * 
     * @return a site info object
     * 
     * @throws RuntimeException
     */
    public static SiteInfo readSiteInfo(SQLiteConnection db) throws RuntimeException {
        SQLiteStatement stmt = null;
        try {
            stmt = db.prepare("SELECT value FROM properties WHERE key == ?");
            return WikiDumpXml2SQLite.<SiteInfo>readObject(stmt, "siteinfo");
        } catch (SQLiteException e) {
            throw new RuntimeException(e);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            if (stmt != null) {
                stmt.dispose();
            }
        }
    }

    @Override
    protected void export(XmlPage page_xml) {
        ++pageCount;
        final Page page = page_xml.getPage();
        if (page.getCurRev() != null) {
            addSQLiteJob(new SQLiteWritePageJob(page, page_xml.getRevisions()));
        }
        if ((pageCount % PRINT_PAGES_EVERY) == 0) {
            println("processed pages: " + pageCount);
        }
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

                    /**
                     * Table storing the siteinfo object.
                     */
                    db.exec("CREATE TABLE properties(key STRING PRIMARY KEY ASC, value);");
                    
                    /**
                     * Core of the wiki: each page has an entry here which identifies
                     * it by title and contains some essential metadata.
                     */
                    final String createPageTable = "CREATE TABLE page ("
                            + "page_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,"
                            + "page_namespace int NOT NULL,"
                            + "page_title varchar(255) NOT NULL,"
                            + "page_restrictions tinyblob NOT NULL,"
                            + "page_is_redirect tinyint unsigned NOT NULL default 0,"
                            + "page_latest int unsigned NOT NULL,"
                            + "page_len int unsigned NOT NULL"
                            + ");";
                    db.exec(createPageTable);
                    // create index here as we need it during import:
                    db.exec("CREATE UNIQUE INDEX name_title ON page (page_namespace,page_title);");
                    
                    /**
                     * Every edit of a page creates also a revision row.
                     * This stores metadata about the revision, and a reference
                     * to the text storage backend.
                     */
                    final String createRevTable = "CREATE TABLE revision ("
                            + "rev_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,"
                            + "rev_page int unsigned NOT NULL,"
                            + "rev_text_id int unsigned NOT NULL,"
                            + "rev_comment tinyblob NOT NULL,"
                            + "rev_user_text varchar(255) NOT NULL default '',"
                            + "rev_timestamp binary(14) NOT NULL default '',"
                            + "rev_minor_edit tinyint unsigned NOT NULL default 0,"
                            + "rev_len int unsigned"
                            + ");";
                    db.exec(createRevTable);
                    
                    /**
                     * Holds text of individual page revisions.
                     */
                    final String createTextTable = "CREATE TABLE text ("
                            + "old_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,"
                            + "old_text mediumblob NOT NULL,"
                            + "old_flags tinyblob NOT NULL"
                            + ");";
                    db.exec(createTextTable);
                    
                    stWritePage = db
                            .prepare("INSERT INTO page "
                                    + "(page_id, page_namespace, page_title, page_restrictions, page_is_redirect, page_latest, page_len) "
                                    + "VALUES (?, ?, ?, ?, ?, ?, ?);");
                    stWriteRevision = db
                            .prepare("INSERT INTO revision "
                                    + "(rev_id, rev_page, rev_text_id, rev_comment, rev_user_text, rev_timestamp, rev_minor_edit, rev_len) "
                                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?);");
                    stWriteText = db.prepare("INSERT INTO text "
                            + "(old_text, old_flags) "
                            + "VALUES (?, 'utf8,gzip')");
                } catch (SQLiteException e) {
                    throw new RuntimeException(e);
                }
                initialised = true;
                
                // take jobs
                while(!(sqliteJobs.isEmpty() && stopWhenQueueEmpty)) {
                    SQLiteJob job;
                    try {
                        job = sqliteJobs.take();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    job.run();
                }

                // write siteinfo object last (may not have been initialised at the beginning):
                SQLiteStatement stmt = null;
                try {
                    stmt = db.prepare("REPLACE INTO properties (key, value) VALUES (?, ?);");
                    writeObject(stmt, "siteinfo", siteInfo);
                } catch (SQLiteException e) {
                    throw new RuntimeException(e);
                } finally {
                    if (stmt != null) {
                        stmt.dispose();
                    }
                }
                
                // create indices at last (improves performance)
                try {
                    db.exec("CREATE INDEX page_len ON page (page_len);");
                    db.exec("CREATE UNIQUE INDEX rev_page_id ON revision (rev_page, rev_id);");
                    db.exec("CREATE INDEX rev_timestamp ON revision (rev_timestamp);");
                    db.exec("CREATE INDEX page_timestamp ON revision (rev_page,rev_timestamp);");
                    db.exec("CREATE INDEX usertext_timestamp ON revision (rev_user_text,rev_timestamp);");
                } catch (SQLiteException e) {
                    throw new RuntimeException(e);
                }
            } finally {
                if (stWritePage != null) {
                    stWritePage.dispose();
                }
                if (stWriteRevision != null) {
                    stWriteRevision.dispose();
                }
                if (stWriteText != null) {
                    stWriteText.dispose();
                }
                if (db != null) {
                    db.dispose();
                }
                initialised = false;
            }
        }
    }
    
    protected static interface SQLiteJob {
        public abstract void run();
    }
    
    protected static class SQLiteNoOpJob implements SQLiteJob {
        @Override
        public void run() {
        }
    }
    
    protected class SQLiteWritePageJob implements SQLiteJob {
        Page page;
        List<Revision> revisions;
        
        public SQLiteWritePageJob(Page page, List<Revision> revisions) {
            this.page = page;
            this.revisions = revisions;
        }
        
        @Override
        public void run() {
            NormalisedTitle normTitle = NormalisedTitle.fromUnnormalised(page.getTitle(), wikiModel.getNamespace());
            // check if page exists:
            try {
                final SQLiteStatement stmt = db.prepare("SELECT page_id FROM page WHERE page_namespace == ? AND page_title = ?;");
                stmt.bind(1, normTitle.namespace).bind(2, normTitle.title);
                if (stmt.step()) {
                    // exists
                    error("duplicate page in dump: "
                            + page.getTitle() + " (=" + normTitle.toString()
                            + ")");
                    return;
                }
                stmt.dispose();
            } catch (SQLiteException e) {
                error("existance check of " + page.getTitle() + " failed (sqlite error: " + e.toString() + ")");
                throw new RuntimeException(e);
            }
            
            
            String pageRestrictions = Page.restrictionsToString(page.getRestrictions());
            final int pageId = page.getId();
            try {
                try {
                    stWritePage.bind(1, pageId).bind(2, normTitle.namespace)
                            .bind(3, normTitle.title).bind(4, pageRestrictions)
                            .bind(5, page.isRedirect() ? 1 : 0)
                            .bind(6, page.getCurRev().getId())
                            .bind(7, page.getCurRev().unpackedText().getBytes().length);
                    stWritePage.stepThrough();
                } finally {
                    stWritePage.reset();
                }
                for (Revision rev : revisions) {
                    int revTextId = -1;
                    try {
                        stWriteText.bind(1, rev.packedText());
                        stWriteText.stepThrough();
                        // get the text's ID:
                        final SQLiteStatement stmt = db.prepare("SELECT last_insert_rowid();");
                        if (stmt.step()) {
                            revTextId = stmt.columnInt(0);
                        }
                        stmt.dispose();
                    } catch (SQLiteException e) {
                        error("write of text for " + page.getTitle() + ", rev " + rev.getId() + " failed (sqlite error: " + e.toString() + ")");
                        throw new RuntimeException(e);
                    } finally {
                        stWriteText.reset();
                    }
                    try {
                        stWriteRevision.bind(1, rev.getId()).bind(2, pageId)
                                .bind(3, revTextId)
                                .bind(4, rev.getComment())
                                .bind(5, rev.getContributor().toString())
                                .bind(6, rev.getTimestamp())
                                .bind(7, rev.isMinor() ? 1 : 0)
                                .bind(8, rev.unpackedText().getBytes().length);
                        stWriteRevision.stepThrough();
                    } catch (SQLiteException e) {
                        error("write of " + page.getTitle() + ", rev " + rev.getId() + " failed (sqlite error: " + e.toString() + ")");
                        throw new RuntimeException(e);
                    } finally {
                        stWriteRevision.reset();
                    }
                }
            } catch (SQLiteException e) {
                error("write of " + page.getTitle() + " failed (sqlite error: " + e.toString() + ")");
                throw new RuntimeException(e);
            }
        }
    }
}
