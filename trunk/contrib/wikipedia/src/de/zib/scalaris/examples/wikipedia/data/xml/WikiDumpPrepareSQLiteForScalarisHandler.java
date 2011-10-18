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
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler;
import de.zib.scalaris.examples.wikipedia.bliki.MyScalarisWikiModel;
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
    protected SQLiteConnection db;
    protected SQLiteStatement stRead;
    protected SQLiteStatement stWrite;
    protected String dbFileName;
    
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
     * @param dbFileName
     *            the name of the database file to write to
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     */
    public WikiDumpPrepareSQLiteForScalarisHandler(Set<String> blacklist,
            Set<String> whitelist, int maxRevisions, Calendar maxTime,
            String dbFileName) throws RuntimeException {
        super(blacklist, whitelist, maxRevisions, maxTime);
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
        SQLiteConnection db = new SQLiteConnection(new File(fileName));
        if (readOnly) {
            db.openReadonly();
        } else {
            db.open(true);
        }
        // set 1GB cache_size:
        final SQLiteStatement stmt = db.prepare("PRAGMA page_size;");
        if (stmt.step()) {
            long cacheSize = stmt.columnLong(0);
            db.exec("PRAGMA cache_size = " + (1024l*1024l*1024l / cacheSize) + ";");
        }
        db.exec("PRAGMA synchronous = OFF;");
        db.exec("PRAGMA journal_mode = OFF;");
        db.exec("PRAGMA locking_mode = EXCLUSIVE;");
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
    protected <T> void writeObject(String key, T value)
            throws RuntimeException {
        writeObject(stWrite, key, value);
    }

    /**
     * 
     * @param <T>
     * @param siteinfo
     * @param key
     * @throws IOException
     * @throws FileNotFoundException
     */
    protected <T> T readObject(String key)
            throws RuntimeException, FileNotFoundException {
        // Note: need to qualify static function call due to
        // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6302954
        return WikiDumpPrepareSQLiteForScalarisHandler.<T>readObject(stRead, key);
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
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(bos));
            oos.writeObject(value);
            oos.flush();
            oos.close();
            try {
                stWrite.bind(1, key).bind(2, bos.toByteArray()).stepThrough();
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
                    ObjectInputStream ois = new ObjectInputStream(
                            new GZIPInputStream(new ByteArrayInputStream(value)));
                    @SuppressWarnings("unchecked")
                    T result = (T) ois.readObject();
                    ois.close();
                    return result;
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
            System.err.println("read of " + key + " failed (class not found)");
            throw new RuntimeException(e);
        } catch (IOException e) {
            System.err.println("read of " + key + " failed");
            throw new RuntimeException(e);
        }
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpPrepareForScalarisHandler#setUp()
     */
    @Override
    public void setUp() {
        super.setUp();
        msgOut.println("Pre-processing pages to key/value pairs...");
        
        try {
            db = openDB(this.dbFileName, false);
            db.exec("CREATE TABLE objects(scalaris_key STRING PRIMARY KEY ASC, scalaris_value);");
            stRead = createReadStmt(db);
            stWrite = createWriteStmt(db);
        } catch (SQLiteException e) {
            throw new RuntimeException(e);
        }
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpHandler#tearDown()
     */
    @Override
    public void tearDown() {
        super.tearDown();
        updatePageLists();
        stRead.dispose();
        stWrite.dispose();
        db.dispose();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#finalize()
     */
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        db.dispose();
    }

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
    protected void doExport(Page page, List<Revision> revisions, List<ShortRevision> revisions_short)
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
}
