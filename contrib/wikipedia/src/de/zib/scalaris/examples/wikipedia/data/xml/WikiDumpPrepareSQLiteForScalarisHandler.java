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
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

/**
 * Provides abilities to read an xml wiki dump file and prepare its contents
 * for Scalaris by creating key/value pairs in a local SQLite db.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiDumpPrepareSQLiteForScalarisHandler extends WikiDumpPrepareForScalarisHandler {
    SQLiteConnection db;
    SQLiteStatement stRead;
    SQLiteStatement stWrite;
    
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
        super(blacklist, whitelist, maxRevisions, maxTime, dbFileName);
    }

    /**
     * @return
     * @throws SQLiteException 
     */
    static SQLiteConnection openDB(String fileName) throws SQLiteException {
        SQLiteConnection db = new SQLiteConnection(new File(fileName));
        db.open(true);
        db.exec("PRAGMA cache_size = 200000;");
        db.exec("PRAGMA synchronous = OFF;");
        db.exec("PRAGMA journal_mode = OFF;");
        db.exec("PRAGMA locking_mode = EXCLUSIVE;");
        return db;
    }

    /**
     * @return
     * @throws SQLiteException
     */
    static SQLiteStatement createReadStmt(SQLiteConnection db) throws SQLiteException {
        return db.prepare("SELECT scalaris_key, scalaris_value FROM objects WHERE scalaris_key == ?");
    }

    /**
     * @return
     * @throws SQLiteException
     */
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
    @Override
    protected <T> void writeObject(String key, T value)
            throws RuntimeException {
        writeObject(stWrite, key, value);
    }

    /**
     * @param <T>
     * @param siteinfo
     * @param key
     * @throws IOException
     * @throws FileNotFoundException
     */
    @Override
    protected <T> T readObject(String key)
            throws RuntimeException, FileNotFoundException {
        return readObject(stRead, key);
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
                stWrite.bind(1, key);
                stWrite.bind(2, bos.toByteArray());
                stWrite.step();
            } finally {
                stWrite.reset();
            }
        } catch (SQLiteException e) {
            System.err.println("write of " + key + " failed (sqlite error)");
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
                    byte[] value = stRead.columnBlob(1);
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
            System.err.println("read of " + key + " failed (sqlite error)");
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
        
        try {
            db = openDB(this.dbFileName);
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
}
