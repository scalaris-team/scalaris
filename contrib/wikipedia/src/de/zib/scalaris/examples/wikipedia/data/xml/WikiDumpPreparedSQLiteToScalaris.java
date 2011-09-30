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

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import de.zib.scalaris.Connection;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.ConnectionFactory;
import de.zib.scalaris.TransactionSingleOp;

/**
 * Provides abilities to read an xml wiki dump file and write its contents to
 * Scalaris by pre-processing key/value pairs into a local SQLite db.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiDumpPreparedSQLiteToScalaris implements WikiDump {
    private static final int MAX_SCALARIS_CONNECTIONS = Runtime.getRuntime().availableProcessors() * 2;
    private static final int PRINT_SCALARIS_KV_PAIRS_EVERY = 5000;
    private ArrayBlockingQueue<TransactionSingleOp> scalaris_single = new ArrayBlockingQueue<TransactionSingleOp>(MAX_SCALARIS_CONNECTIONS);
    private ExecutorService executor = Executors.newFixedThreadPool(MAX_SCALARIS_CONNECTIONS);
    protected TransactionSingleOp.RequestList requests = new TransactionSingleOp.RequestList();
    
    /**
     * The time at the start of an import operation.
     */
    private long timeAtStart = 0;
    /**
     * The time at the end of an import operation.
     */
    private long timeAtEnd = 0;
    /**
     * The number of (successfully) processed K/V pairs.
     */
    protected int importedKeys = 0;
    
    protected PrintStream msgOut = System.out;
    
    protected boolean stop = false;
    
    SQLiteConnection db;
    SQLiteStatement stRead;
    
    String dbFileName;
    ConnectionFactory cFactory;
    
    /**
     * Sets up a SAX XmlHandler exporting all parsed pages except the ones in a
     * blacklist to Scalaris but with an additional pre-process phase.
     * 
     * @param dbFileName
     *            the name of the database file to read from
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     */
    public WikiDumpPreparedSQLiteToScalaris(String dbFileName) throws RuntimeException {
        this.dbFileName = dbFileName;
        this.cFactory = ConnectionFactory.getInstance();
    }

    /**
     * Sets up a SAX XmlHandler exporting all parsed pages except the ones in a
     * blacklist to Scalaris but with an additional pre-process phase.
     * 
     * @param dbFileName
     *            the name of the database file to read from
     * @param cFactory
     *            the connection factory to use for creating new connections
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     */
    public WikiDumpPreparedSQLiteToScalaris(String dbFileName, ConnectionFactory cFactory) throws RuntimeException {
        this.dbFileName = dbFileName;
        this.cFactory = cFactory;
    }

    /**
     * Sets the time the import started.
     */
    final protected void importStart() {
        timeAtStart = System.currentTimeMillis();
    }

    /**
     * Sets the time the import finished.
     */
    final protected void importEnd() {
        timeAtEnd = System.currentTimeMillis();
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDump#getTimeAtStart()
     */
    @Override
    public long getTimeAtStart() {
        return timeAtStart;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDump#getTimeAtEnd()
     */
    @Override
    public long getTimeAtEnd() {
        return timeAtEnd;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDump#getPageCount()
     */
    @Override
    public int getImportCount() {
        return importedKeys;
    }

    /**
     * Reports the speed of the import (pages/s) and may be used as a shutdown
     * handler.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public class ReportAtShutDown extends Thread {
        public void run() {
            // import may have been interrupted - get an end time in this case
            if (timeAtEnd == 0) {
                importEnd();
            }
            final long timeTaken = timeAtEnd - timeAtStart;
            final long speed = (importedKeys * 1000) / timeTaken;
            msgOut.println("Finished import (" + speed + " values/s)");
        }
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDump#setMsgOut(java.io.PrintStream)
     */
    @Override
    public void setMsgOut(PrintStream msgOut) {
        this.msgOut = msgOut;
    }

    /**
     * Tells the import to stop (not supported since this may not result in a
     * consistent view).
     */
    @Override
    public void stopParsing() {
//        this.stop = true;
    }

    /**
     * Writes all K/V pairs to Scalaris.
     * 
     * Note that this process can not be stopped as the resulting view may not
     * be consistent.
     */
    public void writeToScalaris() {
        msgOut.println("Importing key/value pairs to Scalaris...");
        try {
            importStart();
            SQLiteStatement st = db.prepare("SELECT scalaris_key FROM objects");
            while (st.step()) {
                String key = st.columnString(0);
                writeToScalaris(key, WikiDumpPrepareSQLiteForScalarisHandler.readObject(stRead, key));
            }
            // some requests may be left over
            Runnable worker = new WikiDumpToScalarisHandler.MyScalarisSingleRunnable(requests, scalaris_single, "");
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
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (SQLiteException e) {
            e.printStackTrace();
        }
    }
    
    protected <T> void writeToScalaris(String key, T value) {
        ++importedKeys;
        requests.addWrite(key, value);
        // bundle 10 request
        if (requests.size() >= 10) {
            Runnable worker = new WikiDumpToScalarisHandler.MyScalarisSingleRunnable(requests, scalaris_single, "keys up to " + key);
            executor.execute(worker);
            requests = new TransactionSingleOp.RequestList();
        }
        if ((importedKeys % PRINT_SCALARIS_KV_PAIRS_EVERY) == 0) {
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
            
            msgOut.println("imported K/V pairs to Scalaris: " + importedKeys);
        }
    }

    /* (non-Javadoc)
     * @see java.lang.Object#finalize()
     */
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        stRead.dispose();
        db.dispose();
    }

    /**
     * Sets up the directory to write files to as well as the Scalaris
     * connection.
     * 
     * @throws RuntimeException
     *             if the directory could not be created
     */
    @Override
    public void setUp() {
        try {
            db = WikiDumpPrepareSQLiteForScalarisHandler.openDB(dbFileName);
            stRead = WikiDumpPrepareSQLiteForScalarisHandler.createReadStmt(db);
        } catch (SQLiteException e) {
            System.err.println("Cannot read database: " + dbFileName);
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

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDump#tearDown()
     */
    @Override
    public void tearDown() {
    }
}
