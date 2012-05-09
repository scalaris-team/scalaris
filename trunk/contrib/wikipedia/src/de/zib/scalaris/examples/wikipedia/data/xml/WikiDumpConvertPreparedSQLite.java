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
import java.io.IOException;
import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import de.zib.scalaris.ErlangValue;
import de.zib.scalaris.examples.wikipedia.Options;
import de.zib.scalaris.examples.wikipedia.Options.APPEND_INCREMENT_BUCKETS_WITH_HASH;
import de.zib.scalaris.examples.wikipedia.Options.Optimisation;
import de.zib.scalaris.examples.wikipedia.Options.STORE_CONTRIB_TYPE;
import de.zib.scalaris.examples.wikipedia.SQLiteDataHandler;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandlerUnnormalised;
import de.zib.scalaris.examples.wikipedia.ScalarisOpType;
import de.zib.scalaris.examples.wikipedia.bliki.MyNamespace;

/**
 * Provides abilities to read a prepared SQLite wiki dump file and convert its
 * contents to a different optimisation scheme writing the converted data to a
 * new SQLite db.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiDumpConvertPreparedSQLite implements WikiDump {
    private static final int PRINT_SCALARIS_KV_PAIRS_EVERY = 5000;
    protected ArrayBlockingQueue<Runnable> sqliteJobs = new ArrayBlockingQueue<Runnable>(PRINT_SCALARIS_KV_PAIRS_EVERY);
    SQLiteWorker sqliteWorker = new SQLiteWorker();
    
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

    SQLiteConnection dbRead = null;
    SQLiteConnection dbWrite = null;
    protected SQLiteStatement stRead = null;
    protected SQLiteStatement stWrite = null;
    
    final String dbReadFileName;
    final String dbWriteFileName;

    final Options dbWriteOptions;
    
    /**
     * Constructor.
     * 
     * @param dbReadFileName
     *            the name of the database file to read from
     * @param dbWriteFileName
     *            the name of the database file to write to
     * @param dbWriteOptions
     *            optimisation scheme of the DB to write
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     */
    public WikiDumpConvertPreparedSQLite(String dbReadFileName,
            String dbWriteFileName, Options dbWriteOptions)
            throws RuntimeException {
        this.dbReadFileName = dbReadFileName;
        this.dbWriteFileName = dbWriteFileName;
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
            // import may have been interrupted - get an end time in this case
            if (timeAtEnd == 0) {
                importEnd();
            }
            final long timeTaken = timeAtEnd - timeAtStart;
            final double speed = (((double) importedKeys) * 1000) / timeTaken;
            NumberFormat nf = NumberFormat.getNumberInstance(Locale.ENGLISH);
            nf.setGroupingUsed(true);
            println("Finished conversion (" + nf.format(speed) + " pages/s)");
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
    
    protected class SQLiteWorker extends Thread {
        boolean stopWhenQueueEmpty = false;
        boolean initialised = false;
        
        @Override
        public void run() {
            try {
                // set up DB:
                try {
                    dbWrite = SQLiteDataHandler.openDB(dbWriteFileName, false);
                    dbWrite.exec("CREATE TABLE objects(scalaris_key STRING PRIMARY KEY ASC, scalaris_value);");
                    stWrite = WikiDumpPrepareSQLiteForScalarisHandler.createWriteStmt(dbWrite);
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
                if (stWrite != null) {
                    stWrite.dispose();
                }
                if (dbWrite != null) {
                    dbWrite.dispose();
                }
                initialised = false;
            }
        }
    }
    
    protected void addSQLiteJob(Runnable job) throws RuntimeException {
        try {
            sqliteJobs.put(job);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    
    static class SQLiteWriteBytesJob implements Runnable {
        final String key;
        final byte[] value;
        protected final SQLiteStatement stWrite;
        
        public SQLiteWriteBytesJob(String key, byte[] value, SQLiteStatement stWrite) {
            this.key = key;
            this.value = value;
            this.stWrite = stWrite;
        }
        
        @Override
        public void run() {
            try {
                try {
                    stWrite.bind(1, key).bind(2, value).stepThrough();
                } finally {
                    stWrite.reset();
                }
            } catch (SQLiteException e) {
                System.err.println("write of " + key + " failed (sqlite error: " + e.toString() + ")");
            }
        }
    }
    
    static class SQLiteCopyList implements Runnable {
        protected final String key;
        protected final String countKey;
        protected final byte[] value;
        protected final SQLiteStatement stWrite;
        
        public SQLiteCopyList(String key, byte[] value, String countKey, SQLiteStatement stWrite) {
            this.key = key;
            this.value = value;
            this.stWrite = stWrite;
            this.countKey = countKey;
        }
        
        @Override
        public void run() {
            // split lists:
            try {
                // write list
                try {
                    stWrite.bind(1, key).bind(2, value).stepThrough();
                } catch (SQLiteException e) {
                    System.err.println("write of " + key + " failed (sqlite error: " + e.toString() + ")");
                }
                // write count (if available)
                if (countKey != null) {
                    try {
                        stWrite.reset();
                        int listSize = WikiDumpPrepareSQLiteForScalarisHandler.objectFromBytes2(value).listValue().size();
                        stWrite.bind(1, countKey).bind(2, WikiDumpPrepareSQLiteForScalarisHandler.objectToBytes(listSize)).stepThrough();
                    } catch (SQLiteException e) {
                        System.err.println("write of " + countKey + " failed (sqlite error: " + e.toString() + ")");
                    } catch (IOException e) {
                        System.err.println("write of " + countKey + " failed (error: " + e.toString() + ")");
                    } catch (ClassNotFoundException e) {
                        System.err.println("write of " + countKey + " failed (error: " + e.toString() + ")");
                    }
                }
            } finally {
                try {
                    stWrite.reset();
                } catch (SQLiteException e) {
                    System.err.println("failed to reset write statement (error: " + e.toString() + ")");
                }
            }
        }
    }
    
    static class SQLiteWriteBucketListWithHashJob implements Runnable {
        protected final String key;
        protected final String countKey;
        protected final List<ErlangValue> value;
        protected final SQLiteStatement stWrite;
        protected final Options.APPEND_INCREMENT_BUCKETS_WITH_HASH optimisation;
        
        public SQLiteWriteBucketListWithHashJob(String key, List<ErlangValue> value, String countKey, SQLiteStatement stWrite, Options.APPEND_INCREMENT_BUCKETS_WITH_HASH optimisation) {
            this.key = key;
            this.value = value;
            this.stWrite = stWrite;
            this.optimisation = optimisation;
            this.countKey = countKey;
        }
        
        @Override
        public void run() {
            // split lists:
            HashMap<String, List<ErlangValue>> newLists = new HashMap<String, List<ErlangValue>>(optimisation.getBuckets());
            for (ErlangValue obj : value) {
                final String keyAppend2 = optimisation.getBucketString(obj);
                List<ErlangValue> valueAtKey2 = newLists.get(keyAppend2);
                if (valueAtKey2 == null) {
                    valueAtKey2 = new ArrayList<ErlangValue>();
                    newLists.put(keyAppend2, valueAtKey2);
                }
                valueAtKey2.add(obj);
            }

            for (Entry<String, List<ErlangValue>> newList : newLists.entrySet()) {
                try {
                    // write list
                    final String key2 = key + newList.getKey();
                    try {
                        stWrite.bind(1, key2).bind(2, WikiDumpPrepareSQLiteForScalarisHandler.objectToBytes(newList.getValue())).stepThrough();
                    } catch (SQLiteException e) {
                        System.err.println("write of " + key2 + " failed (sqlite error: " + e.toString() + ")");
                    } catch (IOException e) {
                        System.err.println("write of " + key2 + " failed (error: " + e.toString() + ")");
                    }
                    // write count (if available)
                    if (countKey != null) {
                        final String countKey2 = countKey + newList.getKey();
                        try {
                            stWrite.reset();
                            stWrite.bind(1, countKey2).bind(2, WikiDumpPrepareSQLiteForScalarisHandler.objectToBytes(newList.getValue().size())).stepThrough();
                        } catch (SQLiteException e) {
                            System.err.println("write of " + countKey2 + " failed (sqlite error: " + e.toString() + ")");
                        } catch (IOException e) {
                            System.err.println("write of " + countKey2 + " failed (error: " + e.toString() + ")");
                        }
                    }
                } finally {
                    try {
                        stWrite.reset();
                    } catch (SQLiteException e) {
                        System.err.println("failed to reset write statement (error: " + e.toString() + ")");
                    }
                }
            }
        }
    }
    
    static class SQLiteWriteBucketCounterWithHashJob implements Runnable {
        protected final String key;
        protected final int value;
        protected final SQLiteStatement stWrite;
        protected final Options.APPEND_INCREMENT_BUCKETS_WITH_HASH optimisation;
        
        public SQLiteWriteBucketCounterWithHashJob(String key, int value, SQLiteStatement stWrite, Options.APPEND_INCREMENT_BUCKETS_WITH_HASH optimisation) {
            this.key = key;
            this.value = value;
            this.stWrite = stWrite;
            this.optimisation = optimisation;
            assert optimisation.getBuckets() > 1;
        }
        
        @Override
        public void run() {
            // cannot partition a counter without its original values,
            // however, it does not matter which counter is how large
            // -> make all bucket counters (almost) the same value
            
            final int avg = value / optimisation.getBuckets();
            int rest = value; 
            
            for (int i = 0; i < optimisation.getBuckets(); ++i) {
                try {
                    // write count (if available)
                    final String key2 = key + ":" + i;
                    final int curValue = (i == optimisation.getBuckets() - 1) ? rest : avg;
                    rest -= curValue;
                    try {
                        stWrite.reset();
                        stWrite.bind(1, key2).bind(2, WikiDumpPrepareSQLiteForScalarisHandler.objectToBytes(curValue)).stepThrough();
                    } catch (SQLiteException e) {
                        System.err.println("write of " + key2 + " failed (sqlite error: " + e.toString() + ")");
                    } catch (IOException e) {
                        System.err.println("write of " + key2 + " failed (error: " + e.toString() + ")");
                    }
                } finally {
                    try {
                        stWrite.reset();
                    } catch (SQLiteException e) {
                        System.err.println("failed to reset write statement (error: " + e.toString() + ")");
                    }
                }
            }
        }
    }

    private static enum ListOrCountOp {
        NONE, LIST, COUNTER;
    }
    
    /**
     * Starts the conversion.
     * 
     * @throws RuntimeException
     * @throws FileNotFoundException
     */
    public void convertObjects() throws RuntimeException, FileNotFoundException {
        try {
            try {
                // BEWARE: keep in sync with ScalarisDataHandler!
                assert ScalarisDataHandler.getPageListKey(0).equals("pages:0");
                assert ScalarisDataHandler.getPageListKey(-2).equals("pages:-2");
                assert ScalarisDataHandler.getPageCountKey(0).equals("pages:0:count");
                assert ScalarisDataHandlerUnnormalised.getRevKey("foobar", 0, new MyNamespace()).equals("foobar:rev:0");
                assert ScalarisDataHandlerUnnormalised.getPageKey("foobar", new MyNamespace()).equals("foobar:page");
                assert ScalarisDataHandlerUnnormalised.getRevListKey("foobar", new MyNamespace()).equals("foobar:revs");
                assert ScalarisDataHandlerUnnormalised.getCatPageListKey("foobar", new MyNamespace()).equals("foobar:cpages");
                assert ScalarisDataHandlerUnnormalised.getCatPageCountKey("foobar", new MyNamespace()).equals("foobar:cpages:count");
                assert ScalarisDataHandlerUnnormalised.getTplPageListKey("foobar", new MyNamespace()).equals("foobar:tpages");
                assert ScalarisDataHandlerUnnormalised.getBackLinksPageListKey("foobar", new MyNamespace()).equals("foobar:blpages");
                assert ScalarisDataHandler.getContributionListKey("foobar").equals("foobar:user:contrib");
                
                final Pattern pageListPattern = Pattern.compile("^pages:([+-]?[0-9]+)$", Pattern.DOTALL);
                final Pattern pageCountPattern = Pattern.compile("^pages:([+-]?[0-9]+):count$", Pattern.DOTALL);
                final Pattern revPattern = Pattern.compile("^(.*):rev:([0-9]+)$", Pattern.DOTALL);
                final Pattern pagePattern = Pattern.compile("^(.*):page$", Pattern.DOTALL);
                final Pattern revListPattern = Pattern.compile("^(.*):revs$", Pattern.DOTALL);
                final Pattern catPageListPattern = Pattern.compile("^(.*):cpages$", Pattern.DOTALL);
                final Pattern catPageCountPattern = Pattern.compile("^(.*):cpages:count$", Pattern.DOTALL);
                final Pattern tplPageListPattern = Pattern.compile("^(.*):tpages$", Pattern.DOTALL);
                final Pattern backLinksPageListPattern = Pattern.compile("^(.*):blpages$", Pattern.DOTALL);
                final Pattern contributionListPattern = Pattern.compile("^(.*):user:contrib$", Pattern.DOTALL);
                
                while (stRead.step()) {
                    ++importedKeys;
                    String key = stRead.columnString(0);
                    byte[] value = stRead.columnBlob(1);

                    final Matcher pageListMatcher = pageListPattern.matcher(key);
                    final Matcher pageCountMatcher = pageCountPattern.matcher(key);
                    final Matcher revMatcher = revPattern.matcher(key);
                    final Matcher pageMatcher = pagePattern.matcher(key);
                    final Matcher revListMatcher = revListPattern.matcher(key);
                    final Matcher catPageListMatcher = catPageListPattern.matcher(key);
                    final Matcher catPageCountMatcher = catPageCountPattern.matcher(key);
                    final Matcher tplPageListMatcher = tplPageListPattern.matcher(key);
                    final Matcher backLinksPageListMatcher = backLinksPageListPattern.matcher(key);
                    final Matcher contributionListMatcher = contributionListPattern.matcher(key);
                    
                    boolean copyValue = false;
                    ScalarisOpType opType = null;
                    ListOrCountOp listOrCount = ListOrCountOp.NONE;
                    String countKey = null;
                    
                    if (key.equals(ScalarisDataHandler.getSiteInfoKey())) {
                        copyValue = true;
                    } else if (pageListMatcher.matches()) {
                        int namespace = Integer.parseInt(pageListMatcher.group(1));
                        countKey = ScalarisDataHandler.getPageCountKey(namespace);
                        opType = ScalarisOpType.PAGE_LIST;
                        listOrCount = ListOrCountOp.LIST;
                    } else if (pageCountMatcher.matches()) {
                        // ignore (written during page list partitioning (see below)
                        listOrCount = ListOrCountOp.COUNTER;
                    } else if (key.equals(ScalarisDataHandler.getArticleCountKey())) {
                        countKey = null;
                        opType = ScalarisOpType.ARTICLE_COUNT;
                        listOrCount = ListOrCountOp.COUNTER;
                    } else if (revMatcher.matches()) {
                        copyValue = true;
                    } else if (pageMatcher.matches()) {
                        opType = ScalarisOpType.PAGE;
                        copyValue = true;
                    } else if (revListMatcher.matches()) {
                        opType = ScalarisOpType.REVISION;
                        countKey = null;
                        opType = ScalarisOpType.SHORTREV_LIST;
                        listOrCount = ListOrCountOp.LIST;
                    } else if (catPageListMatcher.matches()) {
                        String title = catPageListMatcher.group(1);
                        countKey = title + ":cpages:count";
                        opType = ScalarisOpType.CATEGORY_PAGE_LIST;
                        listOrCount = ListOrCountOp.LIST;
                    } else if (catPageCountMatcher.matches()) {
                        // ignore (written during page list partitioning (see below)
                        listOrCount = ListOrCountOp.COUNTER;
                    } else if (tplPageListMatcher.matches()) {
                        countKey = null;
                        opType = ScalarisOpType.TEMPLATE_PAGE_LIST;
                        listOrCount = ListOrCountOp.LIST;
                    } else if (backLinksPageListMatcher.matches()) {
                        // omit if disabled
                        if (dbWriteOptions.WIKI_USE_BACKLINKS) {
                            countKey = null;
                            opType = ScalarisOpType.BACKLINK_PAGE_LIST;
                            listOrCount = ListOrCountOp.LIST;
                        }
                    } else if (key.matches(ScalarisDataHandler.getStatsPageEditsKey())) {
                        countKey = null;
                        opType = ScalarisOpType.EDIT_STAT;
                        listOrCount = ListOrCountOp.COUNTER;
                    } else if (contributionListMatcher.matches()) {
                        // omit if disabled
                        if (dbWriteOptions.WIKI_STORE_CONTRIBUTIONS != STORE_CONTRIB_TYPE.NONE) {
                            countKey = null;
                            opType = ScalarisOpType.CONTRIBUTION;
                            listOrCount = ListOrCountOp.LIST;
                        }
                    } else {
                        println("unknown key: " + key);
                    }

                    APPEND_INCREMENT_BUCKETS_WITH_HASH optimisation2 = null;
                    if (opType != null) {
                        final Optimisation optimisation = dbWriteOptions.OPTIMISATIONS.get(opType);
                        if (optimisation instanceof APPEND_INCREMENT_BUCKETS_WITH_HASH) {
                            optimisation2 = (APPEND_INCREMENT_BUCKETS_WITH_HASH) optimisation;
                        } else {
                            copyValue = true;
                        }
                    }
                    
                    if (copyValue) {
                        if (listOrCount == ListOrCountOp.LIST) {
                            addSQLiteJob(new SQLiteCopyList(key, value, countKey, stWrite));
                        } else {
                            addSQLiteJob(new SQLiteWriteBytesJob(key, value, stWrite));
                        }
                    } else if (optimisation2 != null) {
                        switch (listOrCount) {
                            case LIST:
                                addSQLiteJob(new SQLiteWriteBucketListWithHashJob(
                                        key,
                                        WikiDumpPrepareSQLiteForScalarisHandler.objectFromBytes2(value).listValue(),
                                        countKey,
                                        stWrite, optimisation2));
                                break;
                            case COUNTER:
                                if (optimisation2.getBuckets() > 1) {
                                    addSQLiteJob(new SQLiteWriteBucketCounterWithHashJob(
                                            key,
                                            WikiDumpPrepareSQLiteForScalarisHandler.objectFromBytes2(value).intValue(),
                                            stWrite, optimisation2));
                                } else {
                                    addSQLiteJob(new SQLiteWriteBytesJob(key, value, stWrite));
                                }
                                break;
                            default:
                                break;
                        }
                    }
                }
            } finally {
                stRead.reset();
            }
        } catch (FileNotFoundException e) {
            throw e;
        } catch (SQLiteException e) {
            System.err.println("read failed (sqlite error: " + e.toString() + ")");
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            System.err.println("read failed (sqlite error: " + e.toString() + ")");
            throw new RuntimeException(e);
        } catch (IOException e) {
            System.err.println("read failed (error: " + e.toString() + ")");
            throw new RuntimeException(e);
        }
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
            dbRead = SQLiteDataHandler.openDB(dbReadFileName, true, null);
            stRead = dbRead.prepare("SELECT scalaris_key, scalaris_value FROM objects");
        } catch (SQLiteException e) {
            System.err.println("Cannot read database: " + dbReadFileName);
            throw new RuntimeException(e);
        }
        println("Converting prepared SQLite wiki dump ...");
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
     * @see java.lang.Object#finalize()
     */
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        tearDown();
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDump#tearDown()
     */
    @Override
    public void tearDown() {
        sqliteWorker.stopWhenQueueEmpty = true;
        addSQLiteJob(new WikiDumpPrepareSQLiteForScalarisHandler.SQLiteNoOpJob());
        // wait for worker to close the DB
        try {
            sqliteWorker.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (stRead != null) {
            stRead.dispose();
        }
        if (dbRead != null) {
            dbRead.dispose();
        }
        importEnd();
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
}
