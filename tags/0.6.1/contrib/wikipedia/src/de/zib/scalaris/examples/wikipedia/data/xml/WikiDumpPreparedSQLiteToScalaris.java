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

import java.io.IOException;
import java.io.PrintStream;
import java.math.BigInteger;
import java.text.NumberFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

import de.zib.scalaris.CommonErlangObjects;
import de.zib.scalaris.Connection;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.ConnectionFactory;
import de.zib.scalaris.ErlangValue;
import de.zib.scalaris.RoundRobinConnectionPolicy;
import de.zib.scalaris.TransactionSingleOp;
import de.zib.scalaris.examples.wikipedia.Options;
import de.zib.scalaris.examples.wikipedia.SQLiteDataHandler;
import de.zib.scalaris.examples.wikipedia.Options.IBuckets;
import de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpConvertPreparedSQLite.ConvertOp;
import de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpConvertPreparedSQLite.KVPair;
import de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpConvertPreparedSQLite.ListOrCountOp;
import de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpConvertPreparedSQLite.SQLiteCopyList;
import de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpConvertPreparedSQLite.SQLiteWriteBucketCounterJob;
import de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpConvertPreparedSQLite.SQLiteWriteBucketListJob;
import de.zib.scalaris.operations.WriteOp;

/**
 * Provides abilities to read an xml wiki dump file and write its contents to
 * Scalaris by pre-processing key/value pairs into a local SQLite db.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiDumpPreparedSQLiteToScalaris implements WikiDump {
    private static final int MAX_SCALARIS_CONNECTIONS = Runtime.getRuntime().availableProcessors() * 4;
    private static final int REQUEST_BUNDLE_SIZE = 10;
    private static final int PRINT_SCALARIS_KV_PAIRS_EVERY = 5000;
    private ArrayBlockingQueue<TransactionSingleOp> scalaris_single = new ArrayBlockingQueue<TransactionSingleOp>(MAX_SCALARIS_CONNECTIONS);
    
    private ExecutorService executor = WikiDumpToScalarisHandler
            .createExecutor(MAX_SCALARIS_CONNECTIONS);
    
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
    
    protected SQLiteConnection db = null;
    
    protected final String dbFileName;
    protected final int numberOfImporters;
    protected final int myNumber;
    protected final ConnectionFactory cFactory;
    protected boolean errorDuringImport = false;
    protected final Options dbWriteOptions;

    /**
     * Sets up a SAX XmlHandler exporting all parsed pages except the ones in a
     * blacklist to Scalaris but with an additional pre-process phase.
     * 
     * @param dbFileName
     *            the name of the database file to read from
     * @param dbWriteOptions
     *            options that specify which optimisations should be applied in
     *            the Scalaris DB
     * @param numberOfImporters
     *            number of (independent) import jobs
     * @param myNumber
     *            my own import job number (1 &lt;= <tt>myNumber</tt> &lt;= <tt>numberOfImporters</tt>)
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     * @throws IllegalArgumentException
     *             if <tt>myNumber</tt> is not greater than 0 or is greater than
     *             <tt>numberOfImporters</tt>
     */
    public WikiDumpPreparedSQLiteToScalaris(String dbFileName, Options dbWriteOptions, int numberOfImporters, int myNumber) throws RuntimeException {
        this.dbFileName = dbFileName;
        this.cFactory = new ConnectionFactory();
        Random random = new Random();
        String clientName = new BigInteger(128, random).toString(16);
        this.cFactory.setClientName("wiki_import_" + clientName);
        this.cFactory.setClientNameAppendUUID(true);
        this.cFactory.setConnectionPolicy(
                new RoundRobinConnectionPolicy(this.cFactory.getNodes()));
        this.numberOfImporters = numberOfImporters;
        this.myNumber = myNumber;
        if (myNumber > numberOfImporters || myNumber < 1) {
            throw new IllegalArgumentException("not 1 <= myNumber (" + myNumber + ") <= numberOfImporters (" + numberOfImporters + ")");
        }
        this.dbWriteOptions = dbWriteOptions;
    }

    /**
     * Sets up a SAX XmlHandler exporting all parsed pages except the ones in a
     * blacklist to Scalaris but with an additional pre-process phase.
     * 
     * @param dbFileName
     *            the name of the database file to read from
     * @param dbWriteOptions
     *            options that specify which optimisations should be applied in
     *            the Scalaris DB
     * @param numberOfImporters
     *            number of (independent) import jobs
     * @param myNumber
     *            my own import job number (1 &lt;= <tt>myNumber</tt> &lt;= <tt>numberOfImporters</tt>)
     * @param cFactory
     *            the connection factory to use for creating new connections
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     * @throws IllegalArgumentException
     *             if <tt>myNumber</tt> is not greater than 0 or is greater than
     *             <tt>numberOfImporters</tt>
     */
    public WikiDumpPreparedSQLiteToScalaris(String dbFileName, Options dbWriteOptions, int numberOfImporters, int myNumber, ConnectionFactory cFactory) throws RuntimeException {
        this.dbFileName = dbFileName;
        this.cFactory = cFactory;
        this.numberOfImporters = numberOfImporters;
        this.myNumber = myNumber;
        if (myNumber > numberOfImporters || myNumber < 1) {
            throw new IllegalArgumentException("not 1 <= myNumber (" + myNumber + ") <= numberOfImporters (" + numberOfImporters + ")");
        }
        this.dbWriteOptions = dbWriteOptions;
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
            reportAtEnd();
        }

        /**
         * Sets the import end time and reports the overall speed.
         */
        public void reportAtEnd() {
            // import may have been interrupted - get an end time in this case
            if (timeAtEnd == 0) {
                importEnd();
            }
            final long timeTaken = timeAtEnd - timeAtStart;
            final double speed = (((double) importedKeys) * 1000) / timeTaken;
            NumberFormat nf = NumberFormat.getNumberInstance(Locale.ENGLISH);
            nf.setGroupingUsed(true);
            println("Finished import (" + nf.format(speed) + " pages/s)");
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
     * Whether {@link #stopParsing()} is supported or not.
     * 
     * @return no stop parsing support
     */
    @Override
    public boolean hasStopSupport() {
        return false;
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
        println("Importing key/value pairs to Scalaris...");
        SQLiteStatement st = null;
        try {
            importStart();
            if (numberOfImporters > 1) {
                final SQLiteStatement countStmt = db.prepare("SELECT COUNT(*) FROM objects;");
                try {
                    if (countStmt.step()) {
                        final long stepSize = countStmt.columnLong(0) / numberOfImporters;
                        final long offset = (myNumber - 1) * stepSize;
                        final long limit = (myNumber == numberOfImporters) ? -1 : stepSize;
                        st = db.prepare("SELECT scalaris_key, scalaris_value FROM objects LIMIT " + limit + " OFFSET " + offset + ";");
                    } else {
                        throw new RuntimeException("cannot count table objects");
                    }
                } finally {
                    countStmt.dispose();
                }
                
            } else {
                st = db.prepare("SELECT scalaris_key, scalaris_value FROM objects;");
            }
            while (st.step()) {
                String key = st.columnString(0);
                byte[] value = st.columnBlob(1);
                try {
                    writeToScalaris(key, value);
                } catch (ClassNotFoundException e) {
                    error("read of " + key + " failed (error: " + e.toString() + ")");
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    error("read of " + key + " failed (error: " + e.toString() + ")");
                    throw new RuntimeException(e);
                }
            }
            // some requests may be left over
            Runnable worker = new WikiDumpToScalarisHandler.MyScalarisSingleRunnable(
                    this, requests, scalaris_single, "");
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
        } catch (SQLiteException e) {
            e.printStackTrace();
        } finally {
            if (st != null) {
                st.dispose();
            }
        }
    }
    
    protected void writeToScalaris(String key, byte[] value) throws ClassNotFoundException, IOException {
        ++importedKeys;
        if (dbWriteOptions == null) {
            OtpErlangObject valueOtp = WikiDumpPrepareSQLiteForScalarisHandler
                    .objectFromBytes(value);
            requests.addOp(new WriteCompressedOp(key, valueOtp));
        } else {
            ConvertOp convOp = WikiDumpConvertPreparedSQLite.getConvertOp(key, dbWriteOptions);
            if (convOp == null) {
                println("unknown key: " + key);
                // use defaults and continue anyway...
                convOp = new ConvertOp();
            }

            if (convOp.optimisation instanceof IBuckets) {
                IBuckets optimisation = (IBuckets) convOp.optimisation;
                switch (convOp.listOrCount) {
                    case LIST:
                        try {
                            List<ErlangValue> listVal = WikiDumpPrepareSQLiteForScalarisHandler
                                    .objectFromBytes2(value).listValue();
                            HashMap<String, List<ErlangValue>> newLists = SQLiteWriteBucketListJob
                                    .splitList(optimisation, listVal);
                            for (Entry<String, List<ErlangValue>> newList : newLists.entrySet()) {
                                // write list
                                final String key2 = key + newList.getKey();
                                requests.addOp(new WriteOp(key2, newList.getValue()));
                                // write count (if available)
                                if (convOp.countKey != null && convOp.countKeyOptimisation == null) {
                                    // integrated counter
                                    final String countKey2 = convOp.countKey + newList.getKey();
                                    requests.addOp(new WriteOp(countKey2, newList.getValue().size()));
                                }
                            }
                            if (convOp.countKey != null && convOp.countKeyOptimisation != null) {
                                // separate counter:
                                int listSize = listVal.size();
                                if (convOp.countKeyOptimisation instanceof IBuckets) {
                                    // similar to "case COUNTER" below:
                                    Collection<KVPair<Integer>> newCounters = SQLiteWriteBucketCounterJob
                                            .splitCounter(
                                                    (IBuckets) convOp.countKeyOptimisation,
                                                    convOp.countKey, listSize);
                                    for (KVPair<Integer> kvPair : newCounters) {
                                        requests.addOp(new WriteOp(kvPair.key, kvPair.value));
                                    }
                                } else {
                                    // copy counter
                                    requests.addOp(new WriteOp(convOp.countKey, listSize));
                                }
                            }
                        } catch (Exception e) {
                            println("write of " + key + " failed (error: " + e.toString() + ")");
                            return;
                        }
                        break;
                    case COUNTER:
                        int counter = WikiDumpPrepareSQLiteForScalarisHandler
                                .objectFromBytes2(value).intValue();
                        Collection<KVPair<Integer>> newCounters = SQLiteWriteBucketCounterJob
                                .splitCounter(optimisation, key, counter);
                        for (KVPair<Integer> kvPair : newCounters) {
                            requests.addOp(new WriteOp(kvPair.key, kvPair.value));
                        }
                        break;
                    default:
                        break;
                }
            } else if (convOp.optimisation != null ) {
                assert (convOp.countKeyOptimisation == null);
                if (convOp.listOrCount == ListOrCountOp.LIST) {
                    Collection<KVPair<Object>> operations = SQLiteCopyList.splitOp(key, convOp.countKey, value);
                    for (KVPair<Object> kvPair : operations) {
                        OtpErlangObject valueOtpCompressed;
                        if (kvPair.value instanceof byte[]) {
                            valueOtpCompressed = WikiDumpPrepareSQLiteForScalarisHandler
                                    .objectFromBytes((byte[]) kvPair.value);
                        } else {
                            valueOtpCompressed = CommonErlangObjects
                                    .encode(ErlangValue.convertToErlang(kvPair.value));
                        }
                        requests.addOp(new WriteCompressedOp(kvPair.key, valueOtpCompressed));
                    }
                } else {
                    // write object as is
                    OtpErlangObject valueOtp = WikiDumpPrepareSQLiteForScalarisHandler
                            .objectFromBytes(value);
                    requests.addOp(new WriteCompressedOp(key, valueOtp));
                }
            }
        }
        // bundle requests:
        if (requests.size() >= REQUEST_BUNDLE_SIZE) {
            Runnable worker = new WikiDumpToScalarisHandler.MyScalarisSingleRunnable(
                    this, requests, scalaris_single, "keys up to " + key);
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
            executor = WikiDumpToScalarisHandler.createExecutor(MAX_SCALARIS_CONNECTIONS);
            
            println("imported K/V pairs to Scalaris: " + importedKeys);
        }
    }
    
    /**
     * Similar to {@link WriteOp} but assumes that the value is already encoded
     * by {@link CommonErlangObjects#encode(OtpErlangObject)} and that the
     * connection to Scalaris which uses this operation uses compressed values.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static class WriteCompressedOp extends WriteOp {
        /**
         * Constructor
         *
         * @param key
         *            the key to write the value to
         * @param value
         *            the value to write
         */
        public WriteCompressedOp(OtpErlangString key, OtpErlangObject value) {
            super(key, value);
        }
        /**
         * Constructor
         *
         * @param key
         *            the key to write the value to
         * @param value
         *            the value to write
         */
        public WriteCompressedOp(String key, OtpErlangObject value) {
            super(new OtpErlangString(key), value);
        }

        /* (non-Javadoc)
         * @see de.zib.scalaris.operations.WriteOp#getErlang(boolean)
         */
        @Override
        public OtpErlangObject getErlang(boolean compressed) {
            assert compressed;
            return new OtpErlangTuple(new OtpErlangObject[] {
                    CommonErlangObjects.writeAtom, key, value });
        }
        
    }

    /* (non-Javadoc)
     * @see java.lang.Object#finalize()
     */
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        tearDown();
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
            db = SQLiteDataHandler.openDB(dbFileName, true, null);
        } catch (SQLiteException e) {
            error("Cannot read database: " + dbFileName);
            throw new RuntimeException(e);
        }

        try {
            for (int i = 0; i < MAX_SCALARIS_CONNECTIONS; ++i) {
                Connection connection = cFactory.createConnection(
                        "wiki_import_" + myNumber, true);
                scalaris_single.put(new TransactionSingleOp(connection));
            }
        } catch (ConnectionException e) {
            error("Connection to Scalaris failed");
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            error("Interrupted while setting up multiple connections to Scalaris");
            throw new RuntimeException(e);
        }
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDump#tearDown()
     */
    @Override
    public void tearDown() {
        if (db != null) {
            db.dispose();
        }
    }

    /**
     * Prints a message to the chosen output stream (includes a timestamp).
     * 
     * @param message
     *            the message to print
     * 
     * @see #setMsgOut(PrintStream)
     */
    public void print(String message) {
        WikiDumpHandler.print(msgOut, message);
    }

    /**
     * Prints a message to the chosen output stream (includes a timestamp).
     * Includes a newline character at the end.
     * 
     * @param message
     *            the message to print
     * 
     * @see #setMsgOut(PrintStream)
     */
    public void println(String message) {
        WikiDumpHandler.println(msgOut, message);
    }

    @Override
    public boolean isErrorDuringImport() {
        return errorDuringImport;
    }

    @Override
    public void error(String message) {
        System.err.println(message);
        errorDuringImport = true;
    }
}
