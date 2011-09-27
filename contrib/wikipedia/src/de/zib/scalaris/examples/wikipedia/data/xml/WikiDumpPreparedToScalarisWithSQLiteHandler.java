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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.util.Calendar;
import java.util.Set;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import de.zib.scalaris.ConnectionFactory;

/**
 * Provides abilities to read an xml wiki dump file and write its contents to
 * Scalaris by pre-processing key/value pairs into a local SQLite db.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiDumpPreparedToScalarisWithSQLiteHandler extends WikiDumpPreparedToScalarisHandler {
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
     * @param dirname
     *            the name of the directory to write temporary files to
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     */
    public WikiDumpPreparedToScalarisWithSQLiteHandler(Set<String> blacklist,
            Set<String> whitelist, int maxRevisions, Calendar maxTime,
            String dirname) throws RuntimeException {
        super(blacklist, whitelist, maxRevisions, maxTime, dirname);
        init();
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
    public WikiDumpPreparedToScalarisWithSQLiteHandler(Set<String> blacklist,
            Set<String> whitelist, int maxRevisions, Calendar maxTime,
            String dirname, ConnectionFactory cFactory) throws RuntimeException {
        super(blacklist, whitelist, maxRevisions, maxTime, dirname, cFactory);
        init();
    }

    /**
     * Initialises the SQLite db to use during processing.
     * 
     * @throws RuntimeException
     */
    private void init() throws RuntimeException {
        try {
            db = new SQLiteConnection(new File(this.dirname + File.separatorChar + "database"));
            db.open(true);
            // create table
            db.exec("CREATE TABLE objects(scalaris_key STRING PRIMARY KEY ASC, scalaris_value);");
            db.exec("PRAGMA cache_size = 200000;");
            db.exec("PRAGMA synchronous = OFF;");
            
            // create statements for read/write
            stRead = db.prepare("SELECT scalaris_key, scalaris_value FROM objects WHERE scalaris_key == ?");
            stWrite = db.prepare("REPLACE INTO objects (scalaris_key, scalaris_value) VALUES (?, ?);");
        } catch (SQLiteException e) {
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
    @Override
    protected <T> void writeObject(String key, T value)
            throws RuntimeException {
        try {
            // note: uncompressed data performs better with SQLite than compressed data (smaller DB size)
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
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
    @Override
    protected <T> T readObject(String key)
            throws RuntimeException, FileNotFoundException {
        try {
            try {
                stRead.bind(1, key);
                if (stRead.step()) {
                    // there should only be one result
                    byte[] value = stRead.columnBlob(1);
                    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(value));
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
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpHandler#tearDown()
     */
    @Override
    public void tearDown() {
        super.tearDown();
        stRead.dispose();
        stWrite.dispose();
        db.dispose();
    }
    
    @Override
    protected void updatePageLists() {
        super.updatePageLists();
        try {
            SQLiteStatement st = db.prepare("SELECT scalaris_key FROM objects ORDER BY scalaris_key ASC");
            PrintStream ps = new PrintStream(new FileOutputStream(dirname
                    + File.separatorChar + "keys.txt"));
            while (st.step()) {
                String key = st.columnString(0);
                ps.println(key);
            }
            ps.flush();
            ps.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (SQLiteException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void writeToScalaris() {
        try {
            SQLiteStatement st = db.prepare("SELECT scalaris_key FROM objects");
            while (st.step()) {
                String key = st.columnString(0);
                writeToScalaris(key, readObject(key));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (SQLiteException e) {
            e.printStackTrace();
        }
    }
}
