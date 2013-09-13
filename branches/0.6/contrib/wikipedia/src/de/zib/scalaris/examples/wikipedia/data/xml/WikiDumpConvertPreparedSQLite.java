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
import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
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
import de.zib.scalaris.examples.wikipedia.Options.APPEND_INCREMENT_BUCKETS;
import de.zib.scalaris.examples.wikipedia.Options.APPEND_INCREMENT_BUCKETS_WITH_WCACHE;
import de.zib.scalaris.examples.wikipedia.Options.APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY;
import de.zib.scalaris.examples.wikipedia.Options.IBuckets;
import de.zib.scalaris.examples.wikipedia.Options.IReadBuckets;
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
    protected ArrayBlockingQueue<Runnable> sqliteJobs = new ArrayBlockingQueue<Runnable>(10);
    SQLiteWorker sqliteWorker = new SQLiteWorker();
    protected boolean errorDuringImport = false;
    
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

    @Override
    public boolean isErrorDuringImport() {
        return errorDuringImport;
    }

    @Override
    public void error(String message) {
        System.err.println(message);
        errorDuringImport = true;
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
            println("Finished conversion (" + nf.format(speed) + " keys/s)");
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
        private final WikiDump importer;
        
        public SQLiteWriteBytesJob(WikiDump importer, String key, byte[] value,
                SQLiteStatement stWrite) {
            this.importer = importer;
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
                importer.error("write of " + key + " failed (sqlite error: " + e.toString() + ")");
            }
        }
    }
    
    protected static class KVPair<T> {
        public final String key;
        public final T value;
        
        public KVPair(String key, T value) {
            this.key = key;
            this.value = value;
        }
    }
    
    static class SQLiteCopyList implements Runnable {
        protected final String key;
        protected final String countKey;
        protected final byte[] value;
        protected final SQLiteStatement stWrite;
        private final WikiDump importer;
        
        public SQLiteCopyList(WikiDump importer, String key, byte[] value,
                String countKey, SQLiteStatement stWrite) {
            this.importer = importer;
            this.key = key;
            this.value = value;
            this.stWrite = stWrite;
            this.countKey = countKey;
        }
        
        public static Collection<KVPair<Object>> splitOp(String key, String countKey, byte[] value) throws ClassNotFoundException, ClassCastException, IOException {
            List<KVPair<Object>> result = new ArrayList<KVPair<Object>>(2);
            // write list
            result.add(new KVPair<Object>(key, value));
            // write count (if available)
            if (countKey != null) {
                int listSize = ErlangValue.otpObjectToOtpList(
                        WikiDumpPrepareSQLiteForScalarisHandler
                                .objectFromBytes2(value).value()).arity();
                result.add(new KVPair<Object>(countKey, listSize));
            }
            
            return result;
        }
        
        @Override
        public void run() {
            try {
                Collection<KVPair<Object>> operations = splitOp(key, countKey, value);
                for (KVPair<Object> kvPair : operations) {
                    try {
                        byte[] valueB;
                        if (kvPair.value instanceof byte[]) {
                            valueB = (byte[]) kvPair.value;
                        } else {
                            valueB = WikiDumpPrepareSQLiteForScalarisHandler.objectToBytes(kvPair.value);
                        }
                        stWrite.bind(1, kvPair.key).bind(2, valueB).stepThrough();
                    } catch (SQLiteException e) {
                        importer.error("write of " + kvPair.key + " failed (sqlite error: " + e.toString() + ")");
                    } finally {
                        try {
                            stWrite.reset();
                        } catch (SQLiteException e) {
                            importer.error("failed to reset write statement (error: " + e.toString() + ")");
                        }
                    }
                }
            } catch (IOException e) {
                importer.error("split of " + key + " failed (error: " + e.toString() + ")");
            } catch (ClassNotFoundException e) {
                importer.error("split of " + key + " failed (error: " + e.toString() + ")");
            } catch (ClassCastException e) {
                importer.error("split of " + key + " failed (error: " + e.toString() + ")");
            }
        }
    }
    
    static class SQLiteWriteBucketListJob implements Runnable {
        protected final String key;
        protected final String countKey;
        protected List<ErlangValue> value;
        protected final SQLiteStatement stWrite;
        protected final IBuckets optimisation;
        private final WikiDump importer;
        
        public SQLiteWriteBucketListJob(WikiDump importer, String key,
                List<ErlangValue> value, String countKey,
                SQLiteStatement stWrite,
                IBuckets optimisation) {
            this.importer = importer;
            this.key = key;
            this.value = value;
            this.stWrite = stWrite;
            this.optimisation = optimisation;
            this.countKey = countKey;
        }

        protected static HashMap<String, List<ErlangValue>> splitList(IBuckets optimisation, List<ErlangValue> value)
                throws RuntimeException, ClassNotFoundException, IOException {
            // split lists:
            int bucketsToUse;
            if (optimisation instanceof IReadBuckets) {
                bucketsToUse = ((IReadBuckets) optimisation).getReadBuckets();
            } else {
                bucketsToUse = optimisation.getBuckets();
            }
            HashMap<String, List<ErlangValue>> newLists = new HashMap<String, List<ErlangValue>>(bucketsToUse);
            for (ErlangValue obj : value) {
                String keyAppend2;
                if (optimisation instanceof APPEND_INCREMENT_BUCKETS) {
                    keyAppend2 = ((APPEND_INCREMENT_BUCKETS) optimisation).getBucketString(obj);
                } else if (optimisation instanceof APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY) {
                    keyAppend2 = ((APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY) optimisation).getReadBucketString();
                } else if (optimisation instanceof APPEND_INCREMENT_BUCKETS_WITH_WCACHE) {
                    keyAppend2 = ((APPEND_INCREMENT_BUCKETS_WITH_WCACHE) optimisation).getReadBucketString(obj);
                } else {
                    throw new RuntimeException("unsupported optimisation: " + optimisation);
                }
                List<ErlangValue> valueAtKey2 = newLists.get(keyAppend2);
                if (valueAtKey2 == null) {
                    // ideal size of each bucket: value.size() / bucketsToUse
                    // ->  add 10% to include variance without having to increase the array list
                    valueAtKey2 = new ArrayList<ErlangValue>(value.size() / bucketsToUse
                            + ((value.size() / bucketsToUse) / 10));
                    newLists.put(keyAppend2, valueAtKey2);
                }
                valueAtKey2.add(obj);
            }
            return newLists;
        }
        
        @Override
        public void run() {
            try {
                HashMap<String, List<ErlangValue>> newLists = splitList(optimisation, value);
                for (Entry<String, List<ErlangValue>> newList : newLists.entrySet()) {
                    try {
                        // write list
                        final String key2 = key + newList.getKey();
                        try {
                            stWrite.bind(1, key2).bind(2, WikiDumpPrepareSQLiteForScalarisHandler.objectToBytes(newList.getValue())).stepThrough();
                        } catch (SQLiteException e) {
                            importer.error("write of " + key2 + " failed (sqlite error: " + e.toString() + ")");
                        } catch (IOException e) {
                            importer.error("write of " + key2 + " failed (error: " + e.toString() + ")");
                        }
                        // write count (if available)
                        if (countKey != null) {
                            final String countKey2 = countKey + newList.getKey();
                            try {
                                stWrite.reset();
                                stWrite.bind(1, countKey2).bind(2, WikiDumpPrepareSQLiteForScalarisHandler.objectToBytes(newList.getValue().size())).stepThrough();
                            } catch (SQLiteException e) {
                                importer.error("write of " + countKey2 + " failed (sqlite error: " + e.toString() + ")");
                            } catch (IOException e) {
                                importer.error("write of " + countKey2 + " failed (error: " + e.toString() + ")");
                            }
                        }
                    } finally {
                        try {
                            stWrite.reset();
                        } catch (SQLiteException e) {
                            importer.error("failed to reset write statement (error: " + e.toString() + ")");
                        }
                    }
                }
            } catch (Exception e) {
                importer.error("write of " + key + " failed (error: " + e.toString() + ")");
                return;
            }
        }
    }
    
    static class SQLiteWriteBucketCounterJob implements Runnable {
        protected final String key;
        protected final int value;
        protected final SQLiteStatement stWrite;
        protected final IBuckets optimisation;
        private final WikiDump importer;
        
        public SQLiteWriteBucketCounterJob(WikiDump importer, String key,
                int value, SQLiteStatement stWrite,
                IBuckets optimisation) {
            this.importer = importer;
            this.key = key;
            this.value = value;
            this.stWrite = stWrite;
            this.optimisation = optimisation;
        }
        
        public static Collection<KVPair<Integer>> splitCounter(IBuckets optimisation, String key, int value) {
            // cannot partition a counter without its original values,
            // however, it does not matter which counter is how large
            // -> make all bucket counters (almost) the same value
            int avg = value;
            int bucketsToUse;
            if (optimisation instanceof IReadBuckets) {
                bucketsToUse = ((IReadBuckets) optimisation).getReadBuckets();
            } else if (optimisation instanceof APPEND_INCREMENT_BUCKETS) {
                bucketsToUse = optimisation.getBuckets();
            } else {
                throw new RuntimeException("unsupported optimisation: " + optimisation);
            }
            List<KVPair<Integer>> result = new ArrayList<KVPair<Integer>>(bucketsToUse);
            avg = value / bucketsToUse;
            int rest = value; 
            
            for (int i = 0; i < bucketsToUse; ++i) {
                final String key2 = key + ":" + i;
                int curValue;
                curValue = (i == bucketsToUse - 1) ? rest : avg;
                rest -= curValue;
                result.add(new KVPair<Integer>(key2, curValue));
            }
            assert(rest == 0);
            
            return result;
        }
        
        @Override
        public void run() {
            Collection<KVPair<Integer>> operations = splitCounter(optimisation, key, value);
            for (KVPair<Integer> kvPair : operations) {
                try {
                    stWrite.bind(1, kvPair.key).bind(2, WikiDumpPrepareSQLiteForScalarisHandler.objectToBytes(kvPair.value)).stepThrough();
                } catch (SQLiteException e) {
                    importer.error("write of " + kvPair.key + " failed (sqlite error: " + e.toString() + ")");
                } catch (IOException e) {
                    importer.error("write of " + kvPair.key + " failed (error: " + e.toString() + ")");
                } finally {
                    try {
                        stWrite.reset();
                    } catch (SQLiteException e) {
                        importer.error("failed to reset write statement (error: " + e.toString() + ")");
                    }
                }
            }
        }
    }

    protected static enum ListOrCountOp {
        NONE, LIST, COUNTER;
    }
    
    static {
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
    }
    
    protected static final Pattern pageListPattern = Pattern.compile("^pages:([+-]?[0-9]+)$", Pattern.DOTALL);
    protected static final Pattern pageCountPattern = Pattern.compile("^pages:([+-]?[0-9]+):count$", Pattern.DOTALL);
    protected static final Pattern revPattern = Pattern.compile("^(.*):rev:([0-9]+)$", Pattern.DOTALL);
    protected static final Pattern pagePattern = Pattern.compile("^(.*):page$", Pattern.DOTALL);
    protected static final Pattern revListPattern = Pattern.compile("^(.*):revs$", Pattern.DOTALL);
    protected static final Pattern catPageListPattern = Pattern.compile("^(.*):cpages$", Pattern.DOTALL);
    protected static final Pattern catPageCountPattern = Pattern.compile("^(.*):cpages:count$", Pattern.DOTALL);
    protected static final Pattern tplPageListPattern = Pattern.compile("^(.*):tpages$", Pattern.DOTALL);
    protected static final Pattern backLinksPageListPattern = Pattern.compile("^(.*):blpages$", Pattern.DOTALL);
    protected static final Pattern contributionListPattern = Pattern.compile("^(.*):user:contrib$", Pattern.DOTALL);
    
    protected static class ConvertOp {
        ListOrCountOp listOrCount = ListOrCountOp.NONE;
        String countKey = null;
        Optimisation optimisation = null;
        Optimisation countKeyOptimisation = null;
    }
    
    /**
     * Starts the conversion.
     * 
     * @throws RuntimeException
     * @throws FileNotFoundException
     */
    public void convertObjects() throws RuntimeException, FileNotFoundException {
        importStart();
        try {
            try {                
                while (stRead.step()) {
                    ++importedKeys;
                    String key = stRead.columnString(0);
                    byte[] value = stRead.columnBlob(1);
                    
                    ConvertOp convOp = getConvertOp(key, dbWriteOptions);
                    if (convOp == null) {
                        println("unknown key: " + key);
                        // use defaults and continue anyway...
                        convOp = new ConvertOp();
                    }
                    
                    if (convOp.optimisation instanceof IBuckets) {
                        IBuckets optimisation = (IBuckets) convOp.optimisation;
                        switch (convOp.listOrCount) {
                            case LIST:
                                List<ErlangValue> listVal = WikiDumpPrepareSQLiteForScalarisHandler
                                        .objectFromBytes2(value).listValue();
                                String countKey;
                                if (convOp.countKeyOptimisation == null) {
                                    // integrated counter
                                    countKey = convOp.countKey;
                                } else {
                                    // separate counter
                                    countKey = null;
                                    int listSize = listVal.size();
                                    if (convOp.countKeyOptimisation instanceof IBuckets) {
                                        // similar to "case COUNTER" below:
                                        final int countValue = listSize;
                                        addSQLiteJob(new SQLiteWriteBucketCounterJob(
                                                this, convOp.countKey, countValue, stWrite,
                                                (IBuckets) convOp.countKeyOptimisation));
                                    } else {
                                        // copy counter
                                        final byte[] countValue = WikiDumpPrepareSQLiteForScalarisHandler
                                                .objectToBytes(listSize);
                                        addSQLiteJob(new SQLiteWriteBytesJob(
                                                this, convOp.countKey,
                                                countValue, stWrite));
                                    }
                                }
                                addSQLiteJob(new SQLiteWriteBucketListJob(
                                        this, key, listVal, countKey,
                                        stWrite, optimisation));
                                break;
                            case COUNTER:
                                final int countValue = WikiDumpPrepareSQLiteForScalarisHandler
                                        .objectFromBytes2(value).intValue();
                                addSQLiteJob(new SQLiteWriteBucketCounterJob(
                                        this, key, countValue, stWrite,
                                        optimisation));
                                break;
                            default:
                                break;
                        }
                    } else if (convOp.optimisation != null ) {
                        assert (convOp.countKeyOptimisation == null);
                        if (convOp.listOrCount == ListOrCountOp.LIST) {
                            addSQLiteJob(new SQLiteCopyList(this, key, value, convOp.countKey, stWrite));
                        } else {
                            addSQLiteJob(new SQLiteWriteBytesJob(this, key, value, stWrite));
                        }
                    }
                    if ((importedKeys % PRINT_SCALARIS_KV_PAIRS_EVERY) == 0) {
                        println("processed keys: " + importedKeys);
                    }
                }
            } finally {
                stRead.reset();
            }
        } catch (FileNotFoundException e) {
            throw e;
        } catch (SQLiteException e) {
            error("read failed (sqlite error: " + e.toString() + ")");
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            error("read failed (sqlite error: " + e.toString() + ")");
            throw new RuntimeException(e);
        } catch (IOException e) {
            error("read failed (error: " + e.toString() + ")");
            throw new RuntimeException(e);
        }
    }

    /**
     * Parses the key and gets the conversion op needed for a plain DB dump to a
     * dump with the given optimisations.
     * 
     * @param key
     *            the key to write to
     * @param dbWriteOptions
     *            optimisation options
     * @return convert operation or <tt>null</tt> if the key signature is
     *         unknown
     * 
     * @throws NumberFormatException
     */
    public static ConvertOp getConvertOp(String key, Options dbWriteOptions) throws NumberFormatException {
        ConvertOp convOp = new ConvertOp();
        ScalarisOpType opType = null;
        ScalarisOpType opTypeCount = null;

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
        
        if (key.equals(ScalarisDataHandler.getSiteInfoKey())) {
            convOp.optimisation = new Options.TRADITIONAL();
        } else if (pageListMatcher.matches()) {
            int namespace = Integer.parseInt(pageListMatcher.group(1));
            convOp.countKey = ScalarisDataHandler.getPageCountKey(namespace);
            opType = ScalarisOpType.PAGE_LIST;
            opTypeCount = ScalarisOpType.PAGE_COUNT;
            convOp.listOrCount = ListOrCountOp.LIST;
        } else if (pageCountMatcher.matches()) {
            // ignore (written during page list partitioning (see above)
            convOp.listOrCount = ListOrCountOp.COUNTER;
        } else if (key.equals(ScalarisDataHandler.getArticleCountKey())) {
            convOp.countKey = null;
            opType = ScalarisOpType.ARTICLE_COUNT;
            convOp.listOrCount = ListOrCountOp.COUNTER;
        } else if (revMatcher.matches()) {
            opType = ScalarisOpType.REVISION;
        } else if (pageMatcher.matches()) {
            opType = ScalarisOpType.PAGE;
        } else if (revListMatcher.matches()) {
            convOp.countKey = null;
            opType = ScalarisOpType.SHORTREV_LIST;
            convOp.listOrCount = ListOrCountOp.LIST;
        } else if (catPageListMatcher.matches()) {
            String title = catPageListMatcher.group(1);
            convOp.countKey = title + ":cpages:count";
            opType = ScalarisOpType.CATEGORY_PAGE_LIST;
            opTypeCount = ScalarisOpType.CATEGORY_PAGE_COUNT;
            convOp.listOrCount = ListOrCountOp.LIST;
        } else if (catPageCountMatcher.matches()) {
            // ignore (written during page list partitioning (see above)
            convOp.listOrCount = ListOrCountOp.COUNTER;
        } else if (tplPageListMatcher.matches()) {
            convOp.countKey = null;
            opType = ScalarisOpType.TEMPLATE_PAGE_LIST;
            convOp.listOrCount = ListOrCountOp.LIST;
        } else if (backLinksPageListMatcher.matches()) {
            // omit if disabled
            if (dbWriteOptions.WIKI_USE_BACKLINKS) {
                convOp.countKey = null;
                opType = ScalarisOpType.BACKLINK_PAGE_LIST;
                convOp.listOrCount = ListOrCountOp.LIST;
            }
        } else if (key.matches(ScalarisDataHandler.getStatsPageEditsKey())) {
            convOp.countKey = null;
            opType = ScalarisOpType.EDIT_STAT;
            convOp.listOrCount = ListOrCountOp.COUNTER;
        } else if (contributionListMatcher.matches()) {
            // omit if disabled
            if (dbWriteOptions.WIKI_STORE_CONTRIBUTIONS != STORE_CONTRIB_TYPE.NONE) {
                convOp.countKey = null;
                opType = ScalarisOpType.CONTRIBUTION;
                convOp.listOrCount = ListOrCountOp.LIST;
            }
        } else {
            return null;
        }

        if (opType != null) {
            convOp.optimisation = dbWriteOptions.OPTIMISATIONS.get(opType);
        }
        if (opTypeCount != null) {
            convOp.countKeyOptimisation = dbWriteOptions.OPTIMISATIONS.get(opTypeCount);
        }
        return convOp;
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
            error("Cannot read database: " + dbReadFileName);
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
