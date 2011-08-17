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
     * The time at the start of a single benchmark.
     */
    protected static long timeAtStart = 0;
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
        ConnectionFactory cf = ConnectionFactory.getInstance();
        List<PeerNode> nodes = cf.getNodes();
        parallelRuns = nodes.size();
        // set a connection policy that goes through the available nodes in a round-robin fashion:
        cf.setConnectionPolicy(new RoundRobinConnectionPolicy(nodes));
        System.out.println("Number of available nodes: " + nodes.size());
        System.out.println("-> Using " + parallelRuns + " parallel instances per test run...");
        long[][] results = getResultArray(3, 2);
        String[] columns;
        String[] rows;

        System.out.println("Benchmark of de.zib.scalaris.TransactionSingleOp:");

        try {
            if (benchmarks.contains(1)) {
                results[0][0] =
                    transSingleOpBench1(testruns, getRandom(BENCH_DATA_SIZE, OtpErlangBinary.class), "transsinglebench_OEB_1");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (final Exception e) {
            // e.printStackTrace();
        }
        try {
            if (benchmarks.contains(2)) {
                results[1][0] =
                    transSingleOpBench2(testruns, getRandom(BENCH_DATA_SIZE, OtpErlangBinary.class), "transsinglebench_OEB_2");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (final Exception e) {
            // e.printStackTrace();
        }
        try {
            if (benchmarks.contains(3)) {
                results[2][0] =
                    transSingleOpBench3(testruns, getRandom(BENCH_DATA_SIZE, OtpErlangBinary.class), "transsinglebench_OEB_3");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (final Exception e) {
            // e.printStackTrace();
        }
        try {
            if (benchmarks.contains(4)) {
                results[0][1] =
                    transSingleOpBench1(testruns, getRandom(BENCH_DATA_SIZE, String.class), "transsinglebench_S_1");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (final Exception e) {
            // e.printStackTrace();
        }
        try {
            if (benchmarks.contains(5)) {
                results[1][1] =
                    transSingleOpBench2(testruns, getRandom(BENCH_DATA_SIZE, String.class), "transsinglebench_S_2");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (final Exception e) {
            // e.printStackTrace();
        }
        try {
            if (benchmarks.contains(6)) {
                results[2][1] =
                    transSingleOpBench3(testruns, getRandom(BENCH_DATA_SIZE, String.class), "transsinglebench_S_3");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (final Exception e) {
            // e.printStackTrace();
        }

        columns = new String[] {
                "TransactionSingleOp.write(OtpErlangString, OtpErlangBinary)",
                "TransactionSingleOp.write(String, String)" };
        rows = new String[] {
                "separate connection",
                "re-use connection",
                "re-use object" };
        printResults(columns, rows, results, testruns);


        results = getResultArray(3, 2);
        System.out.println("-----");
        System.out.println("Benchmark of de.zib.scalaris.Transaction:");

        try {
            if (benchmarks.contains(1)) {
                results[0][0] =
                    transBench1(testruns, getRandom(BENCH_DATA_SIZE, OtpErlangBinary.class), "transbench_OEB_1");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (final Exception e) {
            // e.printStackTrace();
        }
        try {
            if (benchmarks.contains(2)) {
                results[1][0] =
                    transBench2(testruns, getRandom(BENCH_DATA_SIZE, OtpErlangBinary.class), "transbench_OEB_2");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (final Exception e) {
            // e.printStackTrace();
        }
        try {
            if (benchmarks.contains(3)) {
                results[2][0] =
                    transBench3(testruns, getRandom(BENCH_DATA_SIZE, OtpErlangBinary.class), "transbench_OEB_3");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (final Exception e) {
            // e.printStackTrace();
        }
        try {
            if (benchmarks.contains(4)) {
                results[0][1] =
                    transBench1(testruns, getRandom(BENCH_DATA_SIZE, String.class), "transbench_S_1");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (final Exception e) {
            // e.printStackTrace();
        }
        try {
            if (benchmarks.contains(5)) {
                results[1][1] =
                    transBench2(testruns, getRandom(BENCH_DATA_SIZE, String.class), "transbench_S_2");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (final Exception e) {
            // e.printStackTrace();
        }
        try {
            if (benchmarks.contains(6)) {
                results[2][1] =
                    transBench3(testruns, getRandom(BENCH_DATA_SIZE, String.class), "transbench_S_3");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (final Exception e) {
             // e.printStackTrace();
        }

        columns = new String[] {
                "Transaction.write(OtpErlangString, OtpErlangBinary)",
                "Transaction.write(String, String)" };
        rows = new String[] {
                "separate connection",
                "re-use connection",
                "re-use object" };
        printResults(columns, rows, results, testruns);


        results = getResultArray(3, 1);
        System.out.println("-----");
        System.out.println("Benchmark incrementing an integer key (read+write):");

        if (benchmarks.contains(7)) {
            try {
                results[0][0] = transIncrementBench1(testruns, "transbench_inc_1");
                TimeUnit.SECONDS.sleep(1);
            } catch (final Exception e) {
                // e.printStackTrace();
            }
        }
        if (benchmarks.contains(8)) {
            try {
                results[1][0] = transIncrementBench2(testruns, "transbench_inc_2");
                TimeUnit.SECONDS.sleep(1);
            } catch (final Exception e) {
                // e.printStackTrace();
            }
        }
        if (benchmarks.contains(9)) {
            try {
                results[2][0] = transIncrementBench3(testruns, "transbench_inc_3");
            } catch (final Exception e) {
                // e.printStackTrace();
            }
        }

        columns = new String[] {
                "Transaction.read(String).intValue() + Transaction.write(String, Integer)" };
        rows = new String[] {
                "separate connection",
                "re-use connection",
                "re-use object" };
        printResults(columns, rows, results, testruns);
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
        protected long timeAtStart = 0;
        /**
         * The speed of the benchmark in operations/s.
         */
        public long speed = -1;

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
        public BenchRunnable(String key, T value) {
            this.key = key;
            this.value = value;
        }

        /**
         * Call this method when a benchmark is started.
         *
         * Sets the time the benchmark was started.
         */
        final protected void testBegin() {
            timeAtStart = System.currentTimeMillis();
        }

        /**
         * Call this method when a benchmark is finished.
         *
         * Calculates the time the benchmark took and the number of transactions
         * performed during this time.
         */
        final protected long testEnd(final int testRuns) {
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
         * @param key
         *            the key to operate on
         * @param value
         *            the value to use
         * @param j
         *            transaction number
         *
         * @throws Exception
         */
        abstract protected void operation(String key, T value, int j) throws Exception;

        @Override
        public void run() {
            for (int retry = 0; retry < 3 && !stop; ++retry) {
                try {
                    pre_init();
                    testBegin();
                    init();
                    for (int j = 0; j < transactionsPerTestRun; ++j) {
                        operation(key, value, j);
                    }
                    cleanup();
                    this.speed = testEnd(transactionsPerTestRun);
                    break;
                } catch (final Exception e) {
                }
            }
        }
    }

    /**
     * Call this method when a benchmark is started.
     *
     * Sets the time the benchmark was started.
     */
    final protected static void testBegin() {
        timeAtStart = System.currentTimeMillis();
    }

    /**
     * Call this method when a benchmark is finished.
     *
     * Calculates the time the benchmark took and the number of transactions
     * performed during this time.
     *
     * @return the number of achieved transactions per second
     */
    final protected static long testEnd(final int testRuns) {
        final long timeTaken = System.currentTimeMillis() - timeAtStart;
        final long speed = (testRuns * 1000) / timeTaken;
        return speed;
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
        final byte[] data = new byte[size];
        final Random r = new Random();
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
    private static <T> int integrateResults(final long[] results, int i,
            BenchRunnable<T>[] worker, int failed) {
        for (BenchRunnable<T> benchThread : worker) {
            if (failed >= 3) {
                benchThread.stop = true;
                try {
                    benchThread.join();
                } catch (InterruptedException e) {
                }
            } else {
                long speed;
                try {
                    benchThread.join();
                    speed = benchThread.speed;
                } catch (InterruptedException e) {
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
     */
    protected static <T> long transSingleOpBench1(final int testRuns, final T value, final String name) {
        final String key = benchTime + name;
        final long[] results = new long[testRuns];

        for (int i = 0; i < testRuns; ++i) {
            @SuppressWarnings("unchecked")
            BenchRunnable<T> worker[] = new BenchRunnable[parallelRuns];
            for (int thread = 0; thread < parallelRuns; ++thread) {
                worker[thread] = new BenchRunnable<T>(key + '_' + i + '_' + thread, value) {
                    @Override
                    protected void operation(String key, T value, int j) throws Exception {
                        final TransactionSingleOp transaction = new TransactionSingleOp();
                        if (value instanceof OtpErlangObject) {
                            transaction.write(new OtpErlangString(key + '_' + j), (OtpErlangObject) value);
                        } else {
                            transaction.write(key + '_' + j, value);
                        }
                        transaction.closeConnection();
                    }
                };
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
     * Performs a benchmark writing objects using a new
     * {@link TransactionSingleOp} but re-using a single {@link Connection} for
     * each test.
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
     */
    protected static <T> long transSingleOpBench2(final int testRuns, final T value, final String name) {
        final String key = benchTime + name;
        final long[] results = new long[testRuns];

        for (int i = 0; i < testRuns; ++i) {
            @SuppressWarnings("unchecked")
            BenchRunnable<T> worker[] = new BenchRunnable[parallelRuns];
            for (int thread = 0; thread < parallelRuns; ++thread) {
                worker[thread] = new BenchRunnable<T>(key + '_' + i + '_' + thread, value) {
                    Connection connection;
                    @Override
                    protected void init() throws Exception {
                        connection = ConnectionFactory.getInstance().createConnection();
                    }

                    @Override
                    protected void cleanup() throws Exception {
                        connection.close();
                    }

                    @Override
                    protected void operation(String key, T value, int j) throws Exception {
                        final TransactionSingleOp transaction = new TransactionSingleOp(connection);
                        if (value instanceof OtpErlangObject) {
                            transaction.write(new OtpErlangString(key + '_' + j), (OtpErlangObject) value);
                        } else {
                            transaction.write(key + '_' + j, value);
                        }
                    }
                };
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
     * Performs a benchmark writing objects using a single
     * {@link TransactionSingleOp} object for all tests.
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
     */
    protected static <T> long transSingleOpBench3(final int testRuns, final T value, final String name) {
        final String key = benchTime + name;
        final long[] results = new long[testRuns];

        for (int i = 0; i < testRuns; ++i) {
            @SuppressWarnings("unchecked")
            BenchRunnable<T> worker[] = new BenchRunnable[parallelRuns];
            for (int thread = 0; thread < parallelRuns; ++thread) {
                worker[thread] = new BenchRunnable<T>(key + '_' + i + '_' + thread, value) {
                    TransactionSingleOp transaction;
                    @Override
                    protected void init() throws Exception {
                        transaction = new TransactionSingleOp();
                    }

                    @Override
                    protected void cleanup() throws Exception {
                        transaction.closeConnection();
                    }

                    @Override
                    protected void operation(String key, T value, int j) throws Exception {
                        if (value instanceof OtpErlangObject) {
                            transaction.write(new OtpErlangString(key + '_' + j), (OtpErlangObject) value);
                        } else {
                            transaction.write(key + '_' + j, value);
                        }
                    }
                };
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
     * Performs a benchmark writing objects using a new {@link Transaction} for
     * each test.
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
     */
    protected static <T> long transBench1(final int testRuns, final T value, final String name) {
        final String key = benchTime + name;
        final long[] results = new long[testRuns];

        for (int i = 0; i < testRuns; ++i) {
            @SuppressWarnings("unchecked")
            BenchRunnable<T> worker[] = new BenchRunnable[parallelRuns];
            for (int thread = 0; thread < parallelRuns; ++thread) {
                worker[thread] = new BenchRunnable<T>(key + '_' + i + '_' + thread, value) {
                    @Override
                    protected void operation(String key, T value, int j) throws Exception {
                        final Transaction transaction = new Transaction();
                        if (value instanceof OtpErlangObject) {
                            transaction.write(new OtpErlangString(key + '_' + j), (OtpErlangObject) value);
                        } else {
                            transaction.write(key + '_' + j, value);
                        }
                        transaction.commit();
                        transaction.closeConnection();
                    }
                };
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
     * Performs a benchmark writing objects using a new {@link Transaction} but
     * re-using a single {@link Connection} for each test.
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
     */
    protected static <T> long transBench2(final int testRuns, final T value, final String name) {
        final String key = benchTime + name;
        final long[] results = new long[testRuns];

        for (int i = 0; i < testRuns; ++i) {
            @SuppressWarnings("unchecked")
            BenchRunnable<T> worker[] = new BenchRunnable[parallelRuns];
            for (int thread = 0; thread < parallelRuns; ++thread) {
                worker[thread] = new BenchRunnable<T>(key + '_' + i + '_' + thread, value) {
                    Connection connection;
                    @Override
                    protected void init() throws Exception {
                        connection = ConnectionFactory.getInstance().createConnection();
                    }

                    @Override
                    protected void cleanup() throws Exception {
                        connection.close();
                    }

                    @Override
                    protected void operation(String key, T value, int j) throws Exception {
                        final Transaction transaction = new Transaction(connection);
                        if (value instanceof OtpErlangObject) {
                            transaction.write(new OtpErlangString(key + '_' + j), (OtpErlangObject) value);
                        } else {
                            transaction.write(key + '_' + j, value);
                        }
                        transaction.commit();
                    }
                };
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
     * Performs a benchmark writing objects using a single {@link Transaction}
     * object for all tests.
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
     */
    protected static <T> long transBench3(final int testRuns, final T value, final String name) {
        final String key = benchTime + name;
        final long[] results = new long[testRuns];

        for (int i = 0; i < testRuns; ++i) {
            @SuppressWarnings("unchecked")
            BenchRunnable<T> worker[] = new BenchRunnable[parallelRuns];
            for (int thread = 0; thread < parallelRuns; ++thread) {
                worker[thread] = new BenchRunnable<T>(key + '_' + i + '_' + thread, value) {
                    Transaction transaction;
                    @Override
                    protected void init() throws Exception {
                        transaction = new Transaction();
                    }

                    @Override
                    protected void cleanup() throws Exception {
                        transaction.closeConnection();
                    }

                    @Override
                    protected void operation(String key, T value, int j) throws Exception {
                        if (value instanceof OtpErlangObject) {
                            transaction.write(new OtpErlangString(key + '_' + j), (OtpErlangObject) value);
                        } else {
                            transaction.write(key + '_' + j, value);
                        }
                        transaction.commit();
                    }
                };
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
     * Performs a benchmark writing {@link Integer} numbers on a single key and
     * increasing them using a new {@link Transaction} for each test.
     *
     * @param testRuns
     *            the number of times to write the value
     * @param name
     *            the name of the benchmark (will be used as part of the key and
     *            must therefore be unique)
     *
     * @return the number of achieved transactions per second
     */
    protected static long transIncrementBench1(final int testRuns, final String name) {
        final String key = benchTime + name;
        final long[] results = new long[testRuns];

        for (int i = 0; i < testRuns; ++i) {
            @SuppressWarnings("unchecked")
            BenchRunnable<Object> worker[] = new BenchRunnable[parallelRuns];
            for (int thread = 0; thread < parallelRuns; ++thread) {
                worker[thread] = new BenchRunnable<Object>(key + '_' + i + '_' + thread, null) {
                    @Override
                    protected void pre_init() throws Exception {
                        final Transaction tx_init = new Transaction();
                        tx_init.write(key, 0);
                        tx_init.commit();
                        tx_init.closeConnection();
                    }
                    @Override
                    protected void operation(String key, Object value, int j) throws Exception {
                        final Transaction transaction = new Transaction();
                        final int value_old = transaction.read(key).intValue();
                        transaction.write(key, value_old + 1);
                        transaction.commit();
                        transaction.closeConnection();
                    }
                };
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
     * Performs a benchmark writing {@link Integer} numbers on a single key and
     * increasing them using a new {@link Transaction} but re-using a single
     * {@link Connection} for each test.
     *
     * @param testRuns
     *            the number of times to write the value
     * @param name
     *            the name of the benchmark (will be used as part of the key and
     *            must therefore be unique)
     *
     * @return the number of achieved transactions per second
     */
    protected static long transIncrementBench2(final int testRuns, final String name) {
        final String key = benchTime + name;
        final long[] results = new long[testRuns];

        for (int i = 0; i < testRuns; ++i) {
            @SuppressWarnings("unchecked")
            BenchRunnable<Object> worker[] = new BenchRunnable[parallelRuns];
            for (int thread = 0; thread < parallelRuns; ++thread) {
                worker[thread] = new BenchRunnable<Object>(key + '_' + i + '_' + thread, null) {
                    Connection connection;
                    @Override
                    protected void pre_init() throws Exception {
                        final Transaction tx_init = new Transaction();
                        tx_init.write(key, 0);
                        tx_init.commit();
                        tx_init.closeConnection();
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
                    protected void operation(String key, Object value, int j) throws Exception {
                        final Transaction transaction = new Transaction(connection);
                        final int value_old = transaction.read(key).intValue();
                        transaction.write(key, value_old + 1);
                        transaction.commit();
                    }
                };
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
     * Performs a benchmark writing objects using a single {@link Transaction}
     * object for all tests.
     *
     * @param testRuns
     *            the number of times to write the value
     * @param name
     *            the name of the benchmark (will be used as part of the key and
     *            must therefore be unique)
     *
     * @return the number of achieved transactions per second
     */
    protected static long transIncrementBench3(final int testRuns, final String name) {
        final String key = benchTime + name;
        final long[] results = new long[testRuns];

        for (int i = 0; i < testRuns; ++i) {
            @SuppressWarnings("unchecked")
            BenchRunnable<Object> worker[] = new BenchRunnable[parallelRuns];
            for (int thread = 0; thread < parallelRuns; ++thread) {
                worker[thread] = new BenchRunnable<Object>(key + '_' + i + '_' + thread, null) {
                    Transaction transaction;
                    @Override
                    protected void pre_init() throws Exception {
                        final Transaction tx_init = new Transaction();
                        tx_init.write(key, 0);
                        tx_init.commit();
                        tx_init.closeConnection();
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
                    protected void operation(String key, Object value, int j) throws Exception {
                        final int value_old = transaction.read(key).intValue();
                        transaction.write(key, value_old + 1);
                        transaction.commit();
                    }
                };
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
}
