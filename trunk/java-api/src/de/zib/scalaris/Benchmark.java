/**
 *  Copyright 2007-2011 Zuse Institute Berlin
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
package de.zib.scalaris;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;

/**
 * Provides methods to run benchmarks and print the results.
 *
 * Also provides some default benchmarks.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.6
 * @since 2.0
 */
public class Benchmark {
    /**
     * The size of a single data item that is send to scalaris.
     */
    protected static final int BENCH_DATA_SIZE = 1000;
    /**
     * The time when the (whole) benchmark suite was started.
     *
     * This is used to create different erlang keys for each run.
     */
    protected static final long benchTime = System.currentTimeMillis();
    /**
     * Cut 5% off of both ends of the result list.
     */
    protected static final int percentToRemove = 5;
    /**
     * Number of transactions per test run.
     */
    protected static final int transactionsPerTestRun = 10;

    /**
     * Number of parallel tests per test run, i.e. number of parallel clients.
     */
    protected static int parallelRuns;

    /**
     * Default minimal benchmark.
     *
     * Tests some strategies for writing key/value pairs to scalaris:
     * <ol>
     * <li>writing {@link OtpErlangBinary} objects (random data, size =
     * {@link #BENCH_DATA_SIZE})</li>
     * <li>writing {@link String} objects (random data, size =
     * {@link #BENCH_DATA_SIZE})</li>
     * </ol>
     * each <tt>testruns</tt> times
     * <ul>
     * <li>first using a new {@link Transaction} or {@link TransactionSingleOp}
     * for each test,</li>
     * <li>then using a new {@link Transaction} or {@link TransactionSingleOp}
     * but re-using a single {@link Connection},</li>
     * <li>and finally re-using a single {@link Transaction} or
     * {@link TransactionSingleOp} object.</li>
     * </ul>
     *
     * @param testruns
     *            the number of test runs to execute
     * @param benchmarks
     *            the benchmarks to run
     */
    public static void minibench(final int testruns, final Set<Integer> benchmarks) {
        final ConnectionFactory cf = ConnectionFactory.getInstance();
        final List<PeerNode> nodes = cf.getNodes();
        parallelRuns = nodes.size();
        // set a connection policy that goes through the available nodes in a round-robin fashion:
        cf.setConnectionPolicy(new RoundRobinConnectionPolicy(nodes));
        System.out.println("Number of available nodes: " + nodes.size());
        System.out.println("-> Using " + parallelRuns + " parallel instances per test run...");
        long[][] results;
        String[] columns;
        String[] rows;
        @SuppressWarnings("rawtypes")
        Class[] testTypes;
        String[] testTypesStr;
        @SuppressWarnings("rawtypes")
        Class[] testBench;
        String testGroup;

        System.out.println("Benchmark of de.zib.scalaris.TransactionSingleOp:");
        results = getResultArray(3, 2);
        testTypes = new Class[] {OtpErlangBinary.class, String.class};
        testTypesStr = new String[] {"OEB", "S"};
        columns = new String[] {
                "TransactionSingleOp.write(OtpErlangString, OtpErlangBinary)",
                "TransactionSingleOp.write(String, String)" };
        testBench = new Class[] {TransSingleOpBench1.class, TransSingleOpBench2.class, TransSingleOpBench3.class};
        rows = new String[] {
                "separate connection",
                "re-use connection",
                "re-use object" };
        testGroup = "transsinglebench";
        runBenchAndPrintResults(testruns, benchmarks, results, columns, rows,
                testTypes, testTypesStr, testBench, testGroup, 1);

        System.out.println("-----");
        System.out.println("Benchmark of de.zib.scalaris.Transaction:");
        results = getResultArray(3, 2);
        testTypes = new Class[] {OtpErlangBinary.class, String.class};
        testTypesStr = new String[] {"OEB", "S"};
        columns = new String[] {
                "Transaction.write(OtpErlangString, OtpErlangBinary)",
                "Transaction.write(String, String)" };
        testBench = new Class[] {TransBench1.class, TransBench2.class, TransBench3.class};
        rows = new String[] {
                "separate connection",
                "re-use connection",
                "re-use object" };
        testGroup = "transbench";
        runBenchAndPrintResults(testruns, benchmarks, results, columns, rows,
                testTypes, testTypesStr, testBench, testGroup, 1);

        System.out.println("-----");
        System.out.println("Benchmark incrementing an integer key (read+write):");
        results = getResultArray(3, 1);
        testTypes = new Class[] {null};
        testTypesStr = new String[] {"null"};
        columns = new String[] {
                "Transaction.read(String).intValue() + Transaction.write(String, Integer)" };
        testBench = new Class[] {TransIncrementBench1.class, TransIncrementBench2.class, TransIncrementBench3.class};
        rows = new String[] {
                "separate connection",
                "re-use connection",
                "re-use object" };
        testGroup = "transbench_inc";
        runBenchAndPrintResults(testruns, benchmarks, results, columns, rows,
                testTypes, testTypesStr, testBench, testGroup, 7);

        System.out.println("-----");
        System.out.println("Benchmark read 5 + write 5:");
        results = getResultArray(3, 2);
        testTypes = new Class[] {OtpErlangBinary.class, String.class};
        testTypesStr = new String[] {"OEB", "S"};
        columns = new String[] {
                "Transaction.read(OtpErlangString) + Transaction.write(OtpErlangString, OtpErlangBinary)",
                "Transaction.read(String) + Transaction.write(String, String)" };
        testBench = new Class[] {TransRead5Write5Bench1.class, TransRead5Write5Bench2.class, TransRead5Write5Bench3.class};
        rows = new String[] {
                "separate connection",
                "re-use connection",
                "re-use object" };
        testGroup = "transbench_r5w5";
        runBenchAndPrintResults(testruns, benchmarks, results, columns, rows,
                testTypes, testTypesStr, testBench, testGroup, 10);
    }

    protected static final class TransSingleOpBench1<T> extends BenchRunnable<T> {
        public TransSingleOpBench1(final String key, final T value) {
            super(key, value);
        }

        @Override
        protected void operation(final int j) throws Exception {
            final TransactionSingleOp transaction = new TransactionSingleOp();
            if (value instanceof OtpErlangObject) {
                transaction.write(new OtpErlangString(key + '_' + j), (OtpErlangObject) value);
            } else {
                transaction.write(key + '_' + j, value);
            }
            transaction.closeConnection();
        }
    }

    protected static final class TransSingleOpBench2<T> extends BenchRunnable2<T> {
        public TransSingleOpBench2(final String key, final T value) {
            super(key, value);
        }

        @Override
        protected void operation(final int j) throws Exception {
            final TransactionSingleOp transaction = new TransactionSingleOp(connection);
            if (value instanceof OtpErlangObject) {
                transaction.write(new OtpErlangString(key + '_' + j), (OtpErlangObject) value);
            } else {
                transaction.write(key + '_' + j, value);
            }
        }
    }

    protected static final class TransSingleOpBench3<T> extends BenchRunnable<T> {
        TransactionSingleOp transaction;

        public TransSingleOpBench3(final String key, final T value) {
            super(key, value);
        }

        @Override
        protected void init() throws Exception {
            transaction = new TransactionSingleOp();
        }

        @Override
        protected void cleanup() throws Exception {
            transaction.closeConnection();
        }

        @Override
        protected void operation(final int j) throws Exception {
            if (value instanceof OtpErlangObject) {
                transaction.write(new OtpErlangString(key + '_' + j), (OtpErlangObject) value);
            } else {
                transaction.write(key + '_' + j, value);
            }
        }
    }

    protected static final class TransBench1<T> extends BenchRunnable<T> {
        public TransBench1(final String key, final T value) {
            super(key, value);
        }

        @Override
        protected void operation(final int j) throws Exception {
            final Transaction transaction = new Transaction();
            if (value instanceof OtpErlangObject) {
                transaction.write(new OtpErlangString(key + '_' + j), (OtpErlangObject) value);
            } else {
                transaction.write(key + '_' + j, value);
            }
            transaction.commit();
            transaction.closeConnection();
        }
    }

    protected static final class TransBench2<T> extends BenchRunnable2<T> {
        public TransBench2(final String key, final T value) {
            super(key, value);
        }

        @Override
        protected void operation(final int j) throws Exception {
            final Transaction transaction = new Transaction(connection);
            if (value instanceof OtpErlangObject) {
                transaction.write(new OtpErlangString(key + '_' + j), (OtpErlangObject) value);
            } else {
                transaction.write(key + '_' + j, value);
            }
            transaction.commit();
        }
    }

    protected static final class TransBench3<T> extends BenchRunnable<T> {
        Transaction transaction;

        public TransBench3(final String key, final T value) {
            super(key, value);
        }

        @Override
        protected void init() throws Exception {
            transaction = new Transaction();
        }

        @Override
        protected void cleanup() throws Exception {
            transaction.closeConnection();
        }

        @Override
        protected void operation(final int j) throws Exception {
            if (value instanceof OtpErlangObject) {
                transaction.write(new OtpErlangString(key + '_' + j), (OtpErlangObject) value);
            } else {
                transaction.write(key + '_' + j, value);
            }
            transaction.commit();
        }
    }

    protected static abstract class TransIncrementBench extends BenchRunnable<Object> {
        public TransIncrementBench(final String key, final Object value) {
            super(key, value);
        }

        @Override
        protected void pre_init() throws Exception {
            final Transaction tx_init = new Transaction();
            tx_init.write(key, 0);
            tx_init.commit();
            tx_init.closeConnection();
        }

        protected void operation(final Transaction tx, final int j) throws Exception {
            final int value_old = tx.read(key).intValue();
            final Transaction.RequestList reqs = new Transaction.RequestList();
            reqs.addWrite(key, value_old + 1).addCommit();
            final Transaction.ResultList results = tx.req_list(reqs);
            results.processWriteAt(0);
        }
    }

    protected static final class TransIncrementBench1 extends TransIncrementBench {
        public TransIncrementBench1(final String key, final Object value) {
            super(key, value);
        }

        @Override
        protected void operation(final int j) throws Exception {
            final Transaction transaction = new Transaction();
            operation(transaction, j);
            transaction.closeConnection();
        }
    }

    protected static final class TransIncrementBench2 extends TransIncrementBench {
        protected Connection connection;

        public TransIncrementBench2(final String key, final Object value) {
            super(key, value);
        }

        @Override
        protected void init() throws Exception {
            connection = ConnectionFactory.getInstance().createConnection();
        }

        @Override
        protected void cleanup() throws Exception {
            connection.close();
        }

        @Override
        protected void operation(final int j) throws Exception {
            final Transaction transaction = new Transaction(connection);
            operation(transaction, j);
        }
    }

    protected static final class TransIncrementBench3 extends TransIncrementBench {
        Transaction transaction;

        public TransIncrementBench3(final String key, final Object value) {
            super(key, value);
        }

        @Override
        protected void init() throws Exception {
            transaction = new Transaction();
        }

        @Override
        protected void cleanup() throws Exception {
            transaction.closeConnection();
        }

        @Override
        protected void operation(final int j) throws Exception {
            operation(transaction, j);
        }
    }

    protected static abstract class TransReadXWriteXBench<T> extends BenchRunnable<T> {
        private final String[] keys;
        private final T[] valueWrite;

        @SuppressWarnings("unchecked")
        public TransReadXWriteXBench(final String key, final T value, final int nrKeys)
                throws IllegalArgumentException, SecurityException,
                InstantiationException, IllegalAccessException,
                InvocationTargetException, NoSuchMethodException {
            super(key, value);
            keys = new String[nrKeys];
            valueWrite = (T[]) Array.newInstance(value.getClass(), nrKeys);
            for (int i = 0; i < keys.length; ++i) {
                keys[i] = key + "_" + i;
                valueWrite[i] = (T) getRandom(BENCH_DATA_SIZE, value.getClass());
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        protected void pre_init() throws Exception {
            final T[] valueInit;
            valueInit = (T[]) Array.newInstance(value.getClass(), keys.length);
            for (int i = 0; i < keys.length; ++i) {
                valueInit[i] = (T) getRandom(BENCH_DATA_SIZE, value.getClass());
            }
            final Transaction tx_init = new Transaction();
            final Transaction.RequestList reqs = new Transaction.RequestList();
            for (int i = 0; i < keys.length; ++i) {
                reqs.addWrite(keys[i], valueInit[i]);
            }
            reqs.addCommit();
            final Transaction.ResultList results = tx_init.req_list(reqs);
            for (int i = 0; i < keys.length; ++i) {
                results.processWriteAt(i);
            }
            tx_init.closeConnection();
        }

        protected void operation(final Transaction tx, final int j) throws Exception {
            Transaction.RequestList reqs;
            Transaction.ResultList results;
            reqs = new Transaction.RequestList();
            // read old values into the transaction
            for (int i = 0; i < keys.length; ++i) {
                if (value instanceof OtpErlangObject) {
                    reqs.addRead(new OtpErlangString(keys[i]));
                } else {
                    reqs.addRead(keys[i]);
                }
            }
            reqs.addCommit();
            results = tx.req_list(reqs);
            for (int i = 0; i < keys.length; ++i) {
                results.processReadAt(i);
            }

            // write new values...
            reqs = new Transaction.RequestList();
            for (int i = 0; i < keys.length; ++i) {
                final T value = valueWrite[j % valueWrite.length];
                if (value instanceof OtpErlangObject) {
                    reqs.addWrite(new OtpErlangString(keys[i]), (OtpErlangObject) value);
                } else {
                    reqs.addWrite(keys[i], value);
                }
            }
            reqs.addCommit();
            results = tx.req_list(reqs);
            for (int i = 0; i < keys.length; ++i) {
                results.processWriteAt(i);
            }
        }
    }

    protected static final class TransRead5Write5Bench1<T> extends TransReadXWriteXBench<T> {
        public TransRead5Write5Bench1(final String key, final T value)
                throws IllegalArgumentException, SecurityException,
                InstantiationException, IllegalAccessException,
                InvocationTargetException, NoSuchMethodException {
            super(key, value, 5);
        }

        @Override
        protected void operation(final int j) throws Exception {
            final Transaction tx = new Transaction();
            operation(tx, j);
            tx.closeConnection();
        }
    }

    protected static final class TransRead5Write5Bench2<T> extends TransReadXWriteXBench<T> {
        protected Connection connection;

        public TransRead5Write5Bench2(final String key, final T value)
                throws IllegalArgumentException, SecurityException,
                InstantiationException, IllegalAccessException,
                InvocationTargetException, NoSuchMethodException {
            super(key, value, 5);
        }

        @Override
        protected void init() throws Exception {
            connection = ConnectionFactory.getInstance().createConnection();
        }

        @Override
        protected void cleanup() throws Exception {
            connection.close();
        }

        @Override
        protected void operation(final int j) throws Exception {
            final Transaction tx = new Transaction(connection);
            operation(tx, j);
        }
    }

    protected static final class TransRead5Write5Bench3<T> extends TransReadXWriteXBench<T> {
        Transaction transaction;

        public TransRead5Write5Bench3(final String key, final T value)
                throws IllegalArgumentException, SecurityException,
                InstantiationException, IllegalAccessException,
                InvocationTargetException, NoSuchMethodException {
            super(key, value, 5);
        }

        @Override
        protected void init() throws Exception {
            transaction = new Transaction();
        }

        @Override
        protected void cleanup() throws Exception {
            transaction.closeConnection();
        }

        @Override
        protected void operation(final int j) throws Exception {
            operation(transaction, j);
        }
    }

    /**
     * Abstract base class of a test run that is to be run in a thread.
     *
     * @author Nico Kruber, kruber@zib.de
     * @since 3.6
     *
     * @param <T> type of the value to write
     */
    private abstract static class BenchRunnable<T> extends Thread {
        /**
         * Tells the thread to stop.
         */
        public boolean stop = false;

        /**
         * The time at the start of a single benchmark.
         */
        private long timeAtStart = 0;
        /**
         * The speed of the benchmark in operations/s.
         */
        private long speed = -1;

        /**
         * The key to operate on.
         */
        protected final String key;
        /**
         * The value to use.
         */
        protected final T value;

        /**
         * Creates a new runnable.
         *
         * @param key
         *            the key to operate on
         * @param value
         *            the value to use
         */
        public BenchRunnable(final String key, final T value) {
            this.key = key;
            this.value = value;
        }

        /**
         * Call this method when a benchmark is started.
         *
         * Sets the time the benchmark was started.
         */
        final private void testBegin() {
            timeAtStart = System.currentTimeMillis();
        }

        /**
         * Call this method when a benchmark is finished.
         *
         * Calculates the time the benchmark took and the number of transactions
         * performed during this time.
         */
        final private long testEnd(final int testRuns) {
            final long timeTaken = System.currentTimeMillis() - timeAtStart;
            final long speed = (testRuns * 1000) / timeTaken;
            return speed;
        }

        /**
         * Will be called before the benchmark starts.
         *
         * @throws Exception
         */
        protected void pre_init() throws Exception {
        }

        /**
         * Will be called before the benchmark starts with all possible
         * variations of "j" in the {@link #operation(int)} call.
         *
         * @throws Exception
         */
        protected void pre_init(final int j) throws Exception {
        }

        /**
         * Will be called at the start of the benchmark.
         *
         * @throws Exception
         */
        protected void init() throws Exception {
        }

        /**
         * Will be called before the end of the benchmark.
         *
         * @throws Exception
         */
        protected void cleanup() throws Exception {
        }

        /**
         * The operation to execute during the benchmark.
         *
         * @param j
         *            transaction number
         *
         * @throws Exception
         */
        abstract protected void operation(int j) throws Exception;

        @Override
        public void run() {
            for (int retry = 0; (retry < 3) && !stop; ++retry) {
                try {
                    pre_init();
                    for (int j = 0; j < transactionsPerTestRun; ++j) {
                        pre_init(j);
                    }
                    testBegin();
                    init();
                    for (int j = 0; j < transactionsPerTestRun; ++j) {
                        operation(j);
                    }
                    cleanup();
                    this.speed = testEnd(transactionsPerTestRun);
                    break;
                } catch (final Exception e) {
                    // e.printStackTrace();
                }
            }
        }

        /**
         * @return the speed
         */
        public long getSpeed() {
            return speed;
        }
    }

    protected static abstract class BenchRunnable2<T> extends BenchRunnable<T> {
        protected Connection connection;

        protected BenchRunnable2(final String key, final T value) {
            super(key, value);
        }

        @Override
        protected void init() throws Exception {
            connection = ConnectionFactory.getInstance().createConnection();
        }

        @Override
        protected void cleanup() throws Exception {
            connection.close();
        }
    }

    /**
     * Returns a pre-initialized results array with values <tt>-1</tt>.
     *
     * @param rows
     *            number of rows to create
     * @param columns
     *            number of columns to create
     *
     * @return the 2d results array
     */
    protected static long[][] getResultArray(final int rows, final int columns) {
        final long[][] results = new long[rows][columns];
        for (int i = 0; i < rows; ++i) {
            for (int j = 0; j < columns; ++j) {
                results[i][j] = -1;
            }
        }
        return results;
    }

    /**
     * Creates an object T from <tt>size</tt> random bytes. Uses either a
     * constructor that expects a <tt>byte[]</tt> or a {@link String} parameter.
     *
     * @param <T>
     *            the type of the object to create
     * @param size
     *            the number of (random) bytes to create
     * @param c
     *            the class of the object to create (needed due to type erasure)
     *
     * @return the created object
     *
     * @throws IllegalAccessException
     *             - if this Constructor object enforces Java language access
     *             control and the underlying constructor is inaccessible.
     * @throws IllegalArgumentException
     *             - if the number of actual and formal parameters differ; if an
     *             unwrapping conversion for primitive arguments fails; or if,
     *             after possible unwrapping, a parameter value cannot be
     *             converted to the corresponding formal parameter type by a
     *             method invocation conversion; if this constructor pertains to
     *             an enum type.
     * @throws InstantiationException
     *             - if the class that declares the underlying constructor
     *             represents an abstract class.
     * @throws InvocationTargetException
     *             - if the underlying constructor throws an exception.
     * @throws ExceptionInInitializerError
     *             - if the initialization provoked by this method fails.
     * @throws NoSuchMethodException
     *             - if a matching method is not found.
     * @throws SecurityException
     *             - If a security manager, s, is present and any of the
     *             following conditions is met: invocation of
     *             s.checkMemberAccess(this, Member.PUBLIC) denies access to the
     *             constructor the caller's class loader is not the same as or
     *             an ancestor of the class loader for the current class and
     *             invocation of s.checkPackageAccess() denies access to the
     *             package of this class
     */
    protected static <T> T getRandom(final int size, final Class<T> c)
            throws IllegalArgumentException, SecurityException,
            InstantiationException, IllegalAccessException,
            InvocationTargetException, NoSuchMethodException {
        if (c == null) {
            return null;
        }
        final Random r = new Random();
        if (Integer.class.isAssignableFrom(c)) {
            final int par = r.nextInt();
            return c.getConstructor(int.class).newInstance(par);
        }

        final byte[] data = new byte[size];
        r.nextBytes(data);
        try {
            return c.getConstructor(byte[].class).newInstance(data);
        } catch (final NoSuchMethodException e) {
            return c.getConstructor(String.class).newInstance(new String(data));
        }
    }

    /**
     * Integrates the workers' results into the result array.
     *
     * @param <T>
     *            type of the value
     * @param results
     *            results array with operations/s
     * @param i
     *            current number of the test run
     * @param worker
     *            array of worker threads
     * @param failed
     *            number of previously failed threads
     *
     * @return (new) number of failed threads
     */
    private static <T> int integrateResults(final long[] results, final int i,
            final BenchRunnable<T>[] worker, int failed) {
        for (final BenchRunnable<T> benchThread : worker) {
            if (failed >= 3) {
                benchThread.stop = true;
                try {
                    benchThread.join();
                } catch (final InterruptedException e) {
                }
            } else {
                long speed;
                try {
                    benchThread.join();
                    speed = benchThread.getSpeed();
                } catch (final InterruptedException e) {
                    speed = -1;
                }
                if (speed < 0) {
                    ++failed;
                } else {
                    results[i] += speed;
                }
            }
        }
        return failed;
    }

    /**
     * Calculates the average number of transactions per second from the results
     * of executing 10 transactions per test run. Will remove the top and bottom
     * {@link #percentToRemove} percent of the sorted results array.
     *
     * @param results
     *            the average number of transactions per second using 10
     *            transactions
     *
     * @return the average number of transactions per second
     */
    protected static long getAvgSpeed(final long[] results) {
        Arrays.sort(results);
        final int toRemove = (results.length * percentToRemove) / 100;
        long avgSpeed = 0;
        for (int i = toRemove; i < (results.length - toRemove); ++i) {
            avgSpeed += results[i];
        }
        avgSpeed /= results.length - (2 * toRemove);
        return avgSpeed;
    }

    @SuppressWarnings("unchecked")
    protected static void runBenchAndPrintResults(final int testruns,
            final Set<Integer> benchmarks, final long[][] results, final String[] columns,
            final String[] rows, @SuppressWarnings("rawtypes") final Class[] testTypes, final String[] testTypesStr,
            @SuppressWarnings("rawtypes") final Class[] testBench, final String testGroup,
            final int firstBenchId) {
        for (int test = 0; test < (results.length * results[0].length); ++test) {
            try {
                if (benchmarks.contains(test + firstBenchId)) {
                    final int i = test % results.length;
                    final int j = test / results.length;
                    results[i][j] =
                        runBench(testruns, getRandom(BENCH_DATA_SIZE, testTypes[j]), testGroup + "_" + testTypesStr[j] + "_" + (i+1), testBench[i]);
                    TimeUnit.SECONDS.sleep(1);
                }
            } catch (final Exception e) {
                 e.printStackTrace();
            }
        }
        printResults(columns, rows, results, testruns);
    }

    /**
     * Performs a benchmark writing objects using a new
     * {@link TransactionSingleOp} object for each test.
     *
     * @param <T>
     *            type of the value to write
     * @param testRuns
     *            the number of times to write the value
     * @param value
     *            the value to write
     * @param name
     *            the name of the benchmark (will be used as part of the key and
     *            must therefore be unique)
     *
     * @return the number of achieved transactions per second
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws SecurityException
     * @throws IllegalArgumentException
     */
    @SuppressWarnings("unchecked")
    protected static <T> long runBench(final int testRuns, final T value,
            final String name,
            @SuppressWarnings("rawtypes") final Class<? extends BenchRunnable> clazz)
            throws IllegalArgumentException, SecurityException,
            InstantiationException, IllegalAccessException,
            InvocationTargetException, NoSuchMethodException {
        final String key = benchTime + name;
        final long[] results = new long[testRuns];

        for (int i = 0; i < testRuns; ++i) {
            final BenchRunnable<T> worker[] = new BenchRunnable[parallelRuns];
            for (int thread = 0; thread < parallelRuns; ++thread) {
                final Class<? extends Object> valueClazz = (value == null) ? Object.class : value.getClass();
                try {
                    worker[thread] = clazz.getConstructor(String.class, valueClazz)
                            .newInstance(key + '_' + i + '_' + thread, value);
                } catch (final NoSuchMethodException e) {
                    worker[thread] = clazz.getConstructor(String.class, Object.class)
                            .newInstance(key + '_' + i + '_' + thread, value);
                }
                worker[thread].start();
            }
            int failed = 0;
            failed = integrateResults(results, i, worker, failed);
            if (failed >= 3) {
                return -1;
            }
        }

        return getAvgSpeed(results);
    }

    /**
     * Prints a result table.
     *
     * @param columns
     *            names of the columns
     * @param rows
     *            names of the rows (max 25 chars to protect the layout)
     *
     * @param results
     *            the results to print (results[i][j]: i = row, j = column)
     */
    protected static void printResults(final String[] columns, final String[] rows,
            final long[][] results, final int testruns) {
        System.out.println("Test runs: " + testruns + ", each using " + transactionsPerTestRun + " transactions");
        System.out
                .println("                         \tspeed (transactions / second)");
        final String firstColumn = "                         ";
        System.out.print(firstColumn);
        for (int i = 0; i < columns.length; ++i) {
            System.out.print("\t(" + (i + 1) + ")");
        }
        System.out.println();

        for (int i = 0; i < rows.length; ++i) {
            System.out.print(rows[i]
                    + firstColumn.substring(0,
                            firstColumn.length() - rows[i].length() - 1));
            for (int j = 0; j < columns.length; j++) {
                if (results[i][j] == -1) {
                    System.out.print("\tn/a");
                } else {
                    System.out.print("\t" + results[i][j]);
                }
            }
            System.out.println();
        }

        for (int i = 0; i < columns.length; i++) {
            System.out.println("(" + (i + 1) + ") " + columns[i]);
        }
    }
}
