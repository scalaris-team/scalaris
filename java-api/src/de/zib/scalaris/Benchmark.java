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
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;

import de.zib.scalaris.operations.AddDelOnListOp;
import de.zib.scalaris.operations.AddOnNrOp;
import de.zib.scalaris.operations.ReadOp;
import de.zib.scalaris.operations.WriteOp;

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
     * Number of test runs (accumulates results over all test runs).
     */
    protected static final int testRuns = 1;

    /**
     * UTF-8 charset object.
     *
     * StandardCharsets.UTF_8 is only available for Java >= 7
     */
    private static final Charset UTF_8 = Charset.forName("UTF-8");

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
     * each with the given number of consecutive operations and parallel
     * threads per Scalaris node,
     * <ul>
     * <li>first using a new {@link Transaction} or {@link TransactionSingleOp}
     * for each test,</li>
     * <li>then using a new {@link Transaction} or {@link TransactionSingleOp}
     * but re-using a single {@link Connection},</li>
     * <li>and finally re-using a single {@link Transaction} or
     * {@link TransactionSingleOp} object.</li>
     * </ul>
     *
     * @param operations
     *            the number of test runs to execute
     * @param threadsPerNode
     *            number of threads to spawn for each existing Scalaris node
     * @param benchmarks
     *            the benchmarks to run (1-18 or -1 for all benchmarks)
     */
    public static void minibench(final int operations, final int threadsPerNode, final Set<Integer> benchmarks) {
        final ConnectionFactory cf = ConnectionFactory.getInstance();
        final List<PeerNode> nodes = cf.getNodes();
        final int parallelRuns = nodes.size() * threadsPerNode;
        // set a connection policy that goes through the available nodes in a round-robin fashion:
        cf.setConnectionPolicy(new RoundRobinConnectionPolicy(nodes));
        System.out.println("Number of available nodes: " + nodes.size());
        System.out.println("-> Using " + parallelRuns + " parallel instances per test run...");
        System.out.flush();
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
        System.out.flush();
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
        runBenchAndPrintResults(benchmarks, results, columns, rows, testTypes,
                testTypesStr, testBench, testGroup, 1, operations, parallelRuns);

        System.out.println("-----");
        System.out.println("Benchmark of de.zib.scalaris.Transaction:");
        System.out.flush();
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
        runBenchAndPrintResults(benchmarks, results, columns, rows, testTypes,
                testTypesStr, testBench, testGroup, 1, operations, parallelRuns);

        System.out.println("-----");
        System.out.println("Benchmark incrementing an integer key (read+write):");
        System.out.flush();
        results = getResultArray(3, 1);
        testTypes = new Class[] {null};
        testTypesStr = new String[] {"null"};
        columns = new String[] {
                "Transaction.addOnNr(String, Integer)" };
        testBench = new Class[] {TransIncrementBench1.class, TransIncrementBench2.class, TransIncrementBench3.class};
        rows = new String[] {
                "separate connection",
                "re-use connection",
                "re-use object" };
        testGroup = "transbench_inc";
        runBenchAndPrintResults(benchmarks, results, columns, rows, testTypes,
                testTypesStr, testBench, testGroup, 7, operations, parallelRuns);

        System.out.println("-----");
        System.out.println("Benchmark read 5 + write 5:");
        System.out.flush();
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
        runBenchAndPrintResults(benchmarks, results, columns, rows, testTypes,
                testTypesStr, testBench, testGroup, 10, operations,
                parallelRuns);

        System.out.println("-----");
        System.out.println("Benchmark appending to a String list (read+write):");
        System.out.flush();
        results = getResultArray(3, 1);
        testTypes = new Class[] {String.class};
        testTypesStr = new String[] {"S"};
        columns = new String[] {
                "Transaction.addDelOnList(String, StringList, [])" };
        testBench = new Class[] {TransAppendToListBench1.class, TransAppendToListBench2.class, TransAppendToListBench3.class};
        rows = new String[] {
                "separate connection",
                "re-use connection",
                "re-use object" };
        testGroup = "transbench_append";
        runBenchAndPrintResults(benchmarks, results, columns, rows, testTypes,
                testTypesStr, testBench, testGroup, 16, operations,
                parallelRuns);
    }

    /**
     * Performs a benchmark writing objects using a new TransactionSingleOp
     * object for each test.
     *
     * @author Nico Kruber, kruber@zib.de
     *
     * @param <T>
     *            type to bench on
     */
    protected static final class TransSingleOpBench1<T> extends BenchRunnable<T> {
        public TransSingleOpBench1(final String key, final T value, final int operations) {
            super(key, value, operations);
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

    /**
     * Performs a benchmark writing objects using a new TransactionSingleOp but
     * re-using a single connection for each test.
     *
     * @author Nico Kruber, kruber@zib.de
     *
     * @param <T> type to bench on
     */
    protected static final class TransSingleOpBench2<T> extends BenchRunnable2<T> {
        public TransSingleOpBench2(final String key, final T value, final int operations) {
            super(key, value, operations);
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

    /**
     * Performs a benchmark writing objects using a single TransactionSingleOp
     * object for all tests.
     *
     * @author Nico Kruber, kruber@zib.de
     *
     * @param <T> type to bench on
     */
    protected static final class TransSingleOpBench3<T> extends BenchRunnable<T> {
        TransactionSingleOp transaction;

        public TransSingleOpBench3(final String key, final T value, final int operations) {
            super(key, value, operations);
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

    /**
     * Performs a benchmark writing objects using a new Transaction for each test.
     *
     * @author Nico Kruber, kruber@zib.de
     *
     * @param <T> type to bench on
     */
    protected static final class TransBench1<T> extends BenchRunnable<T> {
        public TransBench1(final String key, final T value, final int operations) {
            super(key, value, operations);
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

    /**
     * Performs a benchmark writing objects using a new Transaction but re-using a
     * single connection for each test.
     *
     * @author Nico Kruber, kruber@zib.de
     *
     * @param <T> type to bench on
     */
    protected static final class TransBench2<T> extends BenchRunnable2<T> {
        public TransBench2(final String key, final T value, final int operations) {
            super(key, value, operations);
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

    /**
     * Performs a benchmark writing objects using a single Transaction object
     * for all tests.
     *
     * @author Nico Kruber, kruber@zib.de
     *
     * @param <T> type to bench on
     */
    protected static final class TransBench3<T> extends BenchRunnable<T> {
        Transaction transaction;

        public TransBench3(final String key, final T value, final int operations) {
            super(key, value, operations);
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

    /**
     * Performs a benchmark writing integer numbers on a single key and
     * increasing them.
     *
     * Provides convenience methods for the full increment benchmark
     * implementations.
     *
     * @author Nico Kruber, kruber@zib.de
     */
    protected static abstract class TransIncrementBench extends BenchRunnable<Object> {
        public TransIncrementBench(final String key, final Object value, final int operations) {
            super(key, value, operations);
        }

        @Override
        protected void pre_init(final Connection conn) throws Exception {
            final Transaction tx_init = new Transaction(conn);
            tx_init.write(key, 0);
            tx_init.commit();
        }

        protected void operation(final Transaction tx, final int j) throws Exception {
            final Transaction.RequestList reqs = new Transaction.RequestList();
            reqs.addOp(new AddOnNrOp(key, 1)).addCommit();
//            final int value_old = tx.read(key).intValue();
//            reqs.addWrite(key, value_old + 1).addCommit();
            final Transaction.ResultList results = tx.req_list(reqs);
//            results.processWriteAt(0);
            results.processAddOnNrAt(0);
        }
    }

    /**
     * Performs a benchmark writing integer numbers on a single key and
     * increasing them using a new Transaction for each test.
     *
     * @author Nico Kruber, kruber@zib.de
     */
    protected static final class TransIncrementBench1 extends TransIncrementBench {
        public TransIncrementBench1(final String key, final Object value, final int operations) {
            super(key, value, operations);
        }

        @Override
        protected void operation(final int j) throws Exception {
            final Transaction transaction = new Transaction();
            operation(transaction, j);
            transaction.closeConnection();
        }
    }

    /**
     * Performs a benchmark writing integer numbers on a single key and
     * increasing them using a new Transaction but re-using a single connection
     * for each test.
     *
     * @author Nico Kruber, kruber@zib.de
     */
    protected static final class TransIncrementBench2 extends TransIncrementBench {
        protected Connection connection;

        public TransIncrementBench2(final String key, final Object value, final int operations) {
            super(key, value, operations);
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

    /**
     * Performs a benchmark writing objects using a single Transaction object
     * for all tests.
     *
     * @author Nico Kruber, kruber@zib.de
     */
    protected static final class TransIncrementBench3 extends TransIncrementBench {
        Transaction transaction;

        public TransIncrementBench3(final String key, final Object value, final int operations) {
            super(key, value, operations);
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
     * Performs a benchmark reading X values and overwriting them afterwards
     * inside a transaction. Provides convenience methods for the full read-x,
     * write-x benchmark implementations.
     *
     * @author Nico Kruber, kruber@zib.de
     *
     * @param <T> type to bench on
     */
    protected static abstract class TransReadXWriteXBench<T> extends BenchRunnable<T> {
        private final String[] keys;
        private final T[] valueWrite;

        @SuppressWarnings("unchecked")
        public TransReadXWriteXBench(final String key, final T value, final int nrKeys, final int operations)
                throws IllegalArgumentException, SecurityException,
                InstantiationException, IllegalAccessException,
                InvocationTargetException, NoSuchMethodException {
            super(key, value, operations);
            keys = new String[nrKeys];
            valueWrite = (T[]) Array.newInstance(value.getClass(), nrKeys);
            for (int i = 0; i < keys.length; ++i) {
                keys[i] = key + "_" + i;
                valueWrite[i] = (T) getRandom(BENCH_DATA_SIZE, value.getClass());
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        protected void pre_init(final Connection conn) throws Exception {
            final T[] valueInit = (T[]) Array.newInstance(value.getClass(), keys.length);
            for (int i = 0; i < keys.length; ++i) {
                valueInit[i] = (T) getRandom(BENCH_DATA_SIZE, value.getClass());
            }
            final Transaction tx_init = new Transaction(conn);
            final Transaction.RequestList reqs = new Transaction.RequestList();
            for (int i = 0; i < keys.length; ++i) {
                reqs.addOp(new WriteOp(keys[i], valueInit[i]));
            }
            reqs.addCommit();
            final Transaction.ResultList results = tx_init.req_list(reqs);
            for (int i = 0; i < keys.length; ++i) {
                results.processWriteAt(i);
            }
        }

        protected void operation(final Transaction tx, final int j) throws Exception {
            Transaction.RequestList reqs;
            Transaction.ResultList results;
            reqs = new Transaction.RequestList();
            // read old values into the transaction
            for (final String key : keys) {
                if (value instanceof OtpErlangObject) {
                    reqs.addOp(new ReadOp(new OtpErlangString(key)));
                } else {
                    reqs.addOp(new ReadOp(key));
                }
            }
            reqs.addCommit();
            results = tx.req_list(reqs);
            // check results:
            for (int i = 0; i < keys.length; ++i) {
                if (value instanceof OtpErlangObject) {
                    results.processReadAt(i); // no need to extract the value itself
                } else {
                    results.processReadAt(i).stringValue();
                }
            }

            // write new values...
            reqs = new Transaction.RequestList();
            for (final String key : keys) {
                final T value = valueWrite[j % valueWrite.length];
                if (value instanceof OtpErlangObject) {
                    reqs.addOp(new WriteOp(new OtpErlangString(key), (OtpErlangObject) value));
                } else {
                    reqs.addOp(new WriteOp(key, value));
                }
            }
            reqs.addCommit();
            results = tx.req_list(reqs);
            for (int i = 0; i < keys.length; ++i) {
                results.processWriteAt(i);
            }
        }
    }

    /**
     * Performs a benchmark reading 5 values and overwriting them afterwards
     * inside a transaction using a new Transaction for each test.
     *
     * @author Nico Kruber, kruber@zib.de
     *
     * @param <T> type to bench on
     */
    protected static final class TransRead5Write5Bench1<T> extends TransReadXWriteXBench<T> {
        public TransRead5Write5Bench1(final String key, final T value, final int operations)
                throws IllegalArgumentException, SecurityException,
                InstantiationException, IllegalAccessException,
                InvocationTargetException, NoSuchMethodException {
            super(key, value, 5, operations);
        }

        @Override
        protected void operation(final int j) throws Exception {
            final Transaction tx = new Transaction();
            operation(tx, j);
            tx.closeConnection();
        }
    }

    /**
     * Performs a benchmark reading 5 values and overwriting them afterwards
     * inside a transaction using a new Transaction but re-using a single
     * connection for each test.
     *
     * @author Nico Kruber, kruber@zib.de
     *
     * @param <T> type to bench on
     */
    protected static final class TransRead5Write5Bench2<T> extends TransReadXWriteXBench<T> {
        protected Connection connection;

        public TransRead5Write5Bench2(final String key, final T value, final int operations)
                throws IllegalArgumentException, SecurityException,
                InstantiationException, IllegalAccessException,
                InvocationTargetException, NoSuchMethodException {
            super(key, value, 5, operations);
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

    /**
     * Performs a benchmark reading 5 values and overwriting them afterwards
     * inside a transaction using a single Transaction object for all tests.
     *
     * @author Nico Kruber, kruber@zib.de
     *
     * @param <T> type to bench on
     */
    protected static final class TransRead5Write5Bench3<T> extends TransReadXWriteXBench<T> {
        Transaction transaction;

        public TransRead5Write5Bench3(final String key, final T value, final int operations)
                throws IllegalArgumentException, SecurityException,
                InstantiationException, IllegalAccessException,
                InvocationTargetException, NoSuchMethodException {
            super(key, value, 5, operations);
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
     * Performs a benchmark adding values to a list inside a transaction.
     *
     * Provides convenience methods for the full append-to-list benchmark
     * implementations.
     *
     * @author Nico Kruber, kruber@zib.de
     */
    protected static abstract class TransAppendToListBench extends BenchRunnable<String> {
        private final List<String> valueInit;

        public TransAppendToListBench(final String key, final String value, final int nrKeys, final int operations)
                throws IllegalArgumentException, SecurityException,
                InstantiationException, IllegalAccessException,
                InvocationTargetException, NoSuchMethodException {
            super(key, value, operations);
            valueInit = new ArrayList<String>(nrKeys);
            for (int i = 0; i < nrKeys; ++i) {
                valueInit.add(getRandom(BENCH_DATA_SIZE, String.class));
            }
        }

        @Override
        protected void pre_init(final Connection conn, final int j) throws Exception {
            final Transaction tx_init = new Transaction(conn);
            final Transaction.RequestList reqs = new Transaction.RequestList();
            reqs.addOp(new WriteOp(key + '_' + j, valueInit)).addCommit();
            final Transaction.ResultList results = tx_init.req_list(reqs);
            results.processWriteAt(0);
        }

        protected void operation(final Transaction tx, final int j) throws Exception {
            final Transaction.RequestList reqs = new Transaction.RequestList();
            reqs.addOp(new AddDelOnListOp(key + '_' + j, Arrays.asList(value), new ArrayList<String>(0)));
            reqs.addCommit();
//            // read old list into the transaction
//            final List<String> list = tx.read(key + '_' + j).stringListValue();
//
//            // write new list ...
//            list.add(value);
//            reqs.addWrite(key + '_' + j, list).addCommit();
            final Transaction.ResultList results = tx.req_list(reqs);
//            results.processWriteAt(0);
            results.processAddDelOnListAt(0);
        }
    }

    /**
     * Performs a benchmark adding values to a list inside a transaction using a
     * new Transaction for each test.
     *
     * @author Nico Kruber, kruber@zib.de
     */
    protected static final class TransAppendToListBench1 extends TransAppendToListBench {
        public TransAppendToListBench1(final String key, final String value, final int operations)
                throws IllegalArgumentException, SecurityException,
                InstantiationException, IllegalAccessException,
                InvocationTargetException, NoSuchMethodException {
            super(key, value, 5, operations);
        }

        @Override
        protected void operation(final int j) throws Exception {
            final Transaction tx = new Transaction();
            operation(tx, j);
            tx.closeConnection();
        }
    }

    /**
     * Performs a benchmark adding values to a list inside a transaction using a
     * new Transaction but re-using a single connection for each test.
     *
     * @author Nico Kruber, kruber@zib.de
     */
    protected static final class TransAppendToListBench2 extends TransAppendToListBench {
        protected Connection connection;

        public TransAppendToListBench2(final String key, final String value, final int operations)
                throws IllegalArgumentException, SecurityException,
                InstantiationException, IllegalAccessException,
                InvocationTargetException, NoSuchMethodException {
            super(key, value, 5, operations);
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

    /**
     * Performs a benchmark adding values to a list inside a transaction using a
     * single Transaction object for all tests.
     *
     * @author Nico Kruber, kruber@zib.de
     */
    protected static final class TransAppendToListBench3 extends TransAppendToListBench {
        Transaction transaction;

        public TransAppendToListBench3(final String key, final String value, final int operations)
                throws IllegalArgumentException, SecurityException,
                InstantiationException, IllegalAccessException,
                InvocationTargetException, NoSuchMethodException {
            super(key, value, 5, operations);
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
    protected abstract static class BenchRunnable<T> extends Thread {
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
         * Number of operations to run.
         */
        protected int operations;

        /**
         * Creates a new runnable.
         *
         * @param key
         *            the key to operate on
         * @param value
         *            the value to use
         * @param operations
         *            number of operations to run
         */
        public BenchRunnable(final String key, final T value, final int operations) {
            this.key = key;
            this.value = value;
            this.operations = operations;
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
         * @param conn
         *            connection to use
         *
         * @throws Exception
         */
        protected void pre_init(final Connection conn) throws Exception {
        }

        /**
         * Will be called before the benchmark starts with all possible
         * variations of "j" in the {@link #operation(int)} call.
         *
         * @param conn
         *            connection to use
         * @param j
         *            the index {@link #operation(int)} will be called with
         *
         * @throws Exception
         */
        protected void pre_init(final Connection conn, final int j) throws Exception {
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
        final public void run() {
            Thread.currentThread().setName("BenchRunnable-" + key);
            Connection conn = null;
            try {
                conn = ConnectionFactory.getInstance().createConnection();
                for (int retry = 0; (retry < 3) && !stop; ++retry) {
                    try {
                        pre_init(conn);
                        for (int j = 0; j < operations; ++j) {
                            pre_init(conn, j);
                        }
                        testBegin();
                        init();
                        for (int j = 0; j < operations; ++j) {
                            operation(j);
                        }
                        cleanup();
                        this.speed = testEnd(operations);
                        break;
                    } catch (final Exception e) {
                        // e.printStackTrace();
                    }
                }
            } catch (final Exception e) {
                // e.printStackTrace();
            } finally {
                if (conn != null) {
                    conn.close();
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

        protected BenchRunnable2(final String key, final T value, final int operations) {
            super(key, value, operations);
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
     * constructor that expects a <tt>byte[]</tt> or a {@link String} parameter
     * (in which case the UTF-8 encoding is used for the bytes).
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
    public static <T> T getRandom(final int size, final Class<T> c)
            throws IllegalArgumentException, SecurityException,
            InstantiationException, IllegalAccessException,
            InvocationTargetException, NoSuchMethodException {
        return getRandom(size, c, new Random());
    }

    /**
     * Creates an object T from <tt>size</tt> random bytes. Uses either a
     * constructor that expects a <tt>byte[]</tt> or a {@link String} parameter
     * (in which case the UTF-8 encoding is used for the bytes).
     *
     * @param <T>
     *            the type of the object to create
     * @param size
     *            the number of (random) bytes to create
     * @param c
     *            the class of the object to create (needed due to type erasure)
     * @param r
     *            the random number generator to use
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
    public static <T> T getRandom(final int size, final Class<T> c, final Random r)
            throws IllegalArgumentException, SecurityException,
            InstantiationException, IllegalAccessException,
            InvocationTargetException, NoSuchMethodException {
        if (c == null) {
            return null;
        }
        if (Integer.class.isAssignableFrom(c)) {
            final int par = r.nextInt();
            return c.getConstructor(int.class).newInstance(par);
        }

        final byte[] data = new byte[size];
        r.nextBytes(data);

        try {
            if (!String.class.isAssignableFrom(c)) {
                return c.getConstructor(byte[].class).newInstance(data);
            }
        } catch (final NoSuchMethodException e) {
            // use String constructor below
        }

        final CharsetDecoder decoder = UTF_8.newDecoder();
        decoder.onMalformedInput(CodingErrorAction.REPLACE);
        decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
        String par;
        try {
            par = decoder.decode(ByteBuffer.wrap(data)).toString();
            return c.getConstructor(String.class).newInstance(par);
        } catch (final CharacterCodingException e) {
            throw new IllegalArgumentException(e);
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
    protected static void runBenchAndPrintResults(
            final Set<Integer> benchmarks, final long[][] results,
            final String[] columns, final String[] rows,
            @SuppressWarnings("rawtypes") final Class[] testTypes,
            final String[] testTypesStr,
            @SuppressWarnings("rawtypes") final Class[] testBench,
            final String testGroup, final int firstBenchId,
            final int operations, final int parallelRuns) {
        for (int test = 0; test < (results.length * results[0].length); ++test) {
            try {
                final int i = test % results.length;
                final int j = test / results.length;
                if (benchmarks.contains(test + firstBenchId)) {
                    results[i][j] = runBench(operations,
                            getRandom(BENCH_DATA_SIZE, testTypes[j]),
                            testGroup + "_" + testTypesStr[j] + "_" + (i + 1),
                            testBench[i], parallelRuns);
                    TimeUnit.SECONDS.sleep(1);
                } else {
                    results[i][j] = -2;
                }
            } catch (final Exception e) {
                 e.printStackTrace();
            }
        }
        printResults(columns, rows, results, operations, parallelRuns);
    }

    /**
     * Runs the given benchmark.
     *
     * @param <T>
     *            type of the value to use
     * @param operations
     *            the number of operations to execute
     * @param value
     *            the value to use
     * @param name
     *            the name of the benchmark (will be used as part of the key and
     *            must therefore be unique)
     * @param clazz
     *            bench runnable class
     * @param parallelRuns
     *            number of test runs (accumulates results over all test runs)
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
    protected static <T> long runBench(
            final int operations,
            final T value,
            final String name,
            @SuppressWarnings("rawtypes") final Class<? extends BenchRunnable> clazz,
            final int parallelRuns) throws IllegalArgumentException,
            SecurityException, InstantiationException, IllegalAccessException,
            InvocationTargetException, NoSuchMethodException {
        final String key = benchTime + name;
        final long[] results = new long[testRuns];
        Arrays.fill(results, -1);

        for (int i = 0; i < testRuns; ++i) {
            final BenchRunnable<T> worker[] = new BenchRunnable[parallelRuns];
            for (int thread = 0; thread < parallelRuns; ++thread) {
                final Class<? extends Object> valueClazz = (value == null) ? Object.class : value.getClass();
                try {
                    worker[thread] = clazz.getConstructor(String.class, valueClazz, int.class)
                            .newInstance(key + '_' + i + '_' + thread, value, operations);
                } catch (final NoSuchMethodException e) {
                    worker[thread] = clazz.getConstructor(String.class, Object.class, int.class)
                            .newInstance(key + '_' + i + '_' + thread, value, operations);
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
     * @param results
     *            result array
     * @param operations
     *            number of operations to execute
     * @param parallelRuns
     *            number of parallel threads
     *
     * @param results
     *            the results to print (results[i][j]: i = row, j = column)
     */
    protected static void printResults(final String[] columns, final String[] rows,
            final long[][] results, final int operations, final int parallelRuns) {
        System.out.println("Concurrent threads: " + parallelRuns + ", each using " + operations + " transactions");
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
                if (results[i][j] == -2) {
                    System.out.print("\tn/a");
                } else if (results[i][j] == -1) {
                    System.out.print("\tfailed");
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
