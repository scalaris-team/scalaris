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
 * @version 3.0
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
    public static void minibench(int testruns, Set<Integer> benchmarks) {
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
        } catch (Exception e) {
            // e.printStackTrace();
        }
        try {
            if (benchmarks.contains(2)) {
                results[1][0] = 
                    transSingleOpBench2(testruns, getRandom(BENCH_DATA_SIZE, OtpErlangBinary.class), "transsinglebench_OEB_2");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            // e.printStackTrace();
        }
        try {
            if (benchmarks.contains(3)) {
                results[2][0] = 
                    transSingleOpBench3(testruns, getRandom(BENCH_DATA_SIZE, OtpErlangBinary.class), "transsinglebench_OEB_3");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            // e.printStackTrace();
        }
        try {
            if (benchmarks.contains(4)) {
                results[0][1] = 
                    transSingleOpBench1(testruns, getRandom(BENCH_DATA_SIZE, String.class), "transsinglebench_S_1");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            // e.printStackTrace();
        }
        try {
            if (benchmarks.contains(5)) {
                results[1][1] = 
                    transSingleOpBench2(testruns, getRandom(BENCH_DATA_SIZE, String.class), "transsinglebench_S_2");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            // e.printStackTrace();
        }
        try {
            if (benchmarks.contains(6)) {
                results[2][1] = 
                    transSingleOpBench3(testruns, getRandom(BENCH_DATA_SIZE, String.class), "transsinglebench_S_3");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
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
        } catch (Exception e) {
            // e.printStackTrace();
        }
        try {
            if (benchmarks.contains(2)) {
                results[1][0] = 
                    transBench2(testruns, getRandom(BENCH_DATA_SIZE, OtpErlangBinary.class), "transbench_OEB_2");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            // e.printStackTrace();
        }
        try {
            if (benchmarks.contains(3)) {
                results[2][0] = 
                    transBench3(testruns, getRandom(BENCH_DATA_SIZE, OtpErlangBinary.class), "transbench_OEB_3");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            // e.printStackTrace();
        }
        try {
            if (benchmarks.contains(4)) {
                results[0][1] = 
                    transBench1(testruns, getRandom(BENCH_DATA_SIZE, String.class), "transbench_S_1");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            // e.printStackTrace();
        }
        try {
            if (benchmarks.contains(5)) {
                results[1][1] = 
                    transBench2(testruns, getRandom(BENCH_DATA_SIZE, String.class), "transbench_S_2");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            // e.printStackTrace();
        }
        try {
            if (benchmarks.contains(6)) {
                results[2][1] = 
                    transBench3(testruns, getRandom(BENCH_DATA_SIZE, String.class), "transbench_S_3");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
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
            } catch (Exception e) {
                // e.printStackTrace();
            }
        }
        if (benchmarks.contains(8)) {
            try {
                results[1][0] = transIncrementBench2(testruns, "transbench_inc_2");
                TimeUnit.SECONDS.sleep(1);
            } catch (Exception e) {
                // e.printStackTrace();
            }
        }
        if (benchmarks.contains(9)) {
            try {
                results[2][0] = transIncrementBench3(testruns, "transbench_inc_3");
            } catch (Exception e) {
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
    protected static void printResults(String[] columns, String[] rows,
            long[][] results, int testruns) {
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
    final protected static long testEnd(int testRuns) {
        long timeTaken = System.currentTimeMillis() - timeAtStart;
        long speed = (testRuns * 1000) / timeTaken;
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
    protected static long[][] getResultArray(int rows, int columns) {
        long[][] results = new long[rows][columns];
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
    protected static <T> T getRandom(int size, Class<T> c)
            throws IllegalArgumentException, SecurityException,
            InstantiationException, IllegalAccessException,
            InvocationTargetException, NoSuchMethodException {
        byte[] data = new byte[size];
        Random r = new Random();
        r.nextBytes(data);
        try {
            return c.getConstructor(byte[].class).newInstance(data);
        } catch (NoSuchMethodException e) {
            return c.getConstructor(String.class).newInstance(new String(data));
        }
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
    protected static long getAvgSpeed(long[] results) {
        Arrays.sort(results);
        int toRemove = (results.length * percentToRemove) / 100;
        long avgSpeed = 0;
        for (int i = toRemove; i < (results.length - toRemove); ++i) {
            avgSpeed += results[i];
        }
        avgSpeed /= results.length - 2 * toRemove;
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
    protected static <T> long transSingleOpBench1(int testRuns, T value, String name) {
        String key = benchTime + name;
        long[] results = new long[testRuns];

        for (int i = 0; i < testRuns; ++i) {
            for (int retry = 0; retry < 3; ++retry) {
                try {
                    testBegin();
                    for (int j = 0; j < transactionsPerTestRun; ++j) {
                        TransactionSingleOp transaction = new TransactionSingleOp();
                        if (value instanceof OtpErlangObject) {
                            transaction.write(new OtpErlangString(key + i + j), (OtpErlangObject) value);
                        } else {
                            transaction.write(key + i + j, value);
                        }
                        transaction.closeConnection();
                    }
                    results[i] = testEnd(transactionsPerTestRun);
                    break;
                } catch (Exception e) {
                    // e.printStackTrace();
                    if (retry == 2) {
                        return -1;
                    }
                }
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
    protected static <T> long transSingleOpBench2(int testRuns, T value, String name) {
        String key = benchTime + name;
        long[] results = new long[testRuns];

        for (int i = 0; i < testRuns; ++i) {
            for (int retry = 0; retry < 3; ++retry) {
                try {
                    testBegin();
                    Connection connection = ConnectionFactory.getInstance()
                    .createConnection();
                    for (int j = 0; j < transactionsPerTestRun; ++j) {
                        TransactionSingleOp transaction = new TransactionSingleOp(connection);
                        if (value instanceof OtpErlangObject) {
                            transaction.write(new OtpErlangString(key + i + j), (OtpErlangObject) value);
                        } else {
                            transaction.write(key + i + j, value);
                        }
                    }
                    connection.close();
                    results[i] = testEnd(transactionsPerTestRun);
                    break;
                } catch (Exception e) {
                    // e.printStackTrace();
                    if (retry == 2) {
                        return -1;
                    }
                }
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
    protected static <T> long transSingleOpBench3(int testRuns, T value, String name) {
        String key = benchTime + name;
        long[] results = new long[testRuns];

        for (int i = 0; i < testRuns; ++i) {
            for (int retry = 0; retry < 3; ++retry) {
                try {
                    testBegin();
                    TransactionSingleOp transaction = new TransactionSingleOp();
                    for (int j = 0; j < transactionsPerTestRun; ++j) {
                        if (value instanceof OtpErlangObject) {
                            transaction.write(new OtpErlangString(key + i + j), (OtpErlangObject) value);
                        } else {
                            transaction.write(key + i + j, value);
                        }
                    }
                    transaction.closeConnection();
                    results[i] = testEnd(transactionsPerTestRun);
                    break;
                } catch (Exception e) {
                    // e.printStackTrace();
                    if (retry == 2) {
                        return -1;
                    }
                }
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
    protected static <T> long transBench1(int testRuns, T value, String name) {
        String key = benchTime + name;
        long[] results = new long[testRuns];

        for (int i = 0; i < testRuns; ++i) {
            for (int retry = 0; retry < 3; ++retry) {
                try {
                    testBegin();
                    for (int j = 0; j < transactionsPerTestRun; ++j) {
                        Transaction transaction = new Transaction();
                        if (value instanceof OtpErlangObject) {
                            transaction.write(new OtpErlangString(key + i + j), (OtpErlangObject) value);
                        } else {
                            transaction.write(key + i + j, value);
                        }
                        transaction.commit();
                        transaction.closeConnection();
                    }
                    results[i] = testEnd(transactionsPerTestRun);
                    break;
                } catch (Exception e) {
                    // e.printStackTrace();
                    if (retry == 2) {
                        return -1;
                    }
                }
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
    protected static <T> long transBench2(int testRuns, T value, String name) {
        String key = benchTime + name;
        long[] results = new long[testRuns];

        for (int i = 0; i < testRuns; ++i) {
            for (int retry = 0; retry < 3; ++retry) {
                try {
                    testBegin();
                    Connection connection = ConnectionFactory.getInstance()
                    .createConnection();
                    for (int j = 0; j < transactionsPerTestRun; ++j) {
                        Transaction transaction = new Transaction(connection);
                        if (value instanceof OtpErlangObject) {
                            transaction.write(new OtpErlangString(key + i + j), (OtpErlangObject) value);
                        } else {
                            transaction.write(key + i + j, value);
                        }
                        transaction.commit();
                    }
                    connection.close();
                    results[i] = testEnd(transactionsPerTestRun);
                    break;
                } catch (Exception e) {
                    // e.printStackTrace();
                    if (retry == 2) {
                        return -1;
                    }
                }
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
    protected static <T> long transBench3(int testRuns, T value, String name) {
        String key = benchTime + name;
        long[] results = new long[testRuns];

        for (int i = 0; i < testRuns; ++i) {
            for (int retry = 0; retry < 3; ++retry) {
                try {
                    testBegin();
                    Transaction transaction = new Transaction();
                    for (int j = 0; j < transactionsPerTestRun; ++j) {
                        if (value instanceof OtpErlangObject) {
                            transaction.write(new OtpErlangString(key + i + j), (OtpErlangObject) value);
                        } else {
                            transaction.write(key + i + j, value);
                        }
                        transaction.commit();
                    }
                    transaction.closeConnection();
                    results[i] = testEnd(transactionsPerTestRun);
                    break;
                } catch (Exception e) {
                    // e.printStackTrace();
                    if (retry == 2) {
                        return -1;
                    }
                }
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
    protected static long transIncrementBench1(int testRuns, String name) {
        String key = benchTime + name;
        long[] results = new long[testRuns];

        for (int i = 0; i < testRuns; ++i) {
            for (int retry = 0; retry < 3; ++retry) {
                try {
                    String key_i = key + i;
                    Transaction tx_init = new Transaction();
                    tx_init.write(key_i, 0);
                    tx_init.commit();
                    tx_init.closeConnection();
                    testBegin();
                    for (int j = 0; j < transactionsPerTestRun; ++j) {
                        Transaction transaction = new Transaction();
                        int value_old = transaction.read(key_i).intValue();
                        transaction.write(key_i, value_old + 1);
                        transaction.commit();
                        transaction.closeConnection();
                    }
                    results[i] = testEnd(transactionsPerTestRun);
                    break;
                } catch (Exception e) {
                    // e.printStackTrace();
                    if (retry == 2) {
                        return -1;
                    }
                }
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
    protected static long transIncrementBench2(int testRuns, String name) {
        String key = benchTime + name;
        long[] results = new long[testRuns];

        for (int i = 0; i < testRuns; ++i) {
            for (int retry = 0; retry < 3; ++retry) {
                try {
                    String key_i = key + i;
                    Transaction tx_init = new Transaction();
                    tx_init.write(key_i, 0);
                    tx_init.commit();
                    tx_init.closeConnection();
                    testBegin();
                    Connection connection = ConnectionFactory.getInstance()
                    .createConnection();
                    for (int j = 0; j < transactionsPerTestRun; ++j) {
                        Transaction transaction = new Transaction(connection);
                        int value_old = transaction.read(key_i).intValue();
                        transaction.write(key_i, value_old + 1);
                        transaction.commit();
                    }
                    connection.close();
                    results[i] = testEnd(transactionsPerTestRun);
                    break;
                } catch (Exception e) {
                    // e.printStackTrace();
                    if (retry == 2) {
                        return -1;
                    }
                }
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
    protected static long transIncrementBench3(int testRuns, String name) {
        String key = benchTime + name;
        long[] results = new long[testRuns];

        for (int i = 0; i < testRuns; ++i) {
            for (int retry = 0; retry < 3; ++retry) {
                try {
                    String key_i = key + i;
                    Transaction tx_init = new Transaction();
                    tx_init.write(key_i, 0);
                    tx_init.commit();
                    tx_init.closeConnection();
                    testBegin();
                    Transaction transaction = new Transaction();
                    for (int j = 0; j < transactionsPerTestRun; ++j) {
                        int value_old = transaction.read(key_i).intValue();
                        transaction.write(key_i, value_old + 1);
                        transaction.commit();
                    }
                    transaction.closeConnection();
                    results[i] = testEnd(transactionsPerTestRun);
                    break;
                } catch (Exception e) {
                    // e.printStackTrace();
                    if (retry == 2) {
                        return -1;
                    }
                }
            }
        }

        return getAvgSpeed(results);
    }
}
