package de.zib.scalaris;

import java.util.Random;

import com.ericsson.otp.erlang.OtpConnection;
import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangString;

/**
 * Provides methods to run benchmarks and print the results.
 * 
 * Also provides some default benchmarks.
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 2.0
 * @since 2.0
 */
public class Benchmark {
	/**
	 * The size of a single data item that is send to scalaris.
	 */
	protected static final int BENCH_DATA_SIZE = 1000;
	/**
	 * The number of test runs.
	 */
	protected static final int BENCH_TEST_RUNS = 100;
	/**
	 * The time when the (whole) benchmark suite was started.
	 * 
	 * This is used to create different erlang keys for each run.
	 */
    protected static long benchTime = System.currentTimeMillis();
    /**
     * The time at the start of a single benchmark. 
     */
    protected static long timeAtStart = 0;
	
	/**
	 * Default minimal benchmark.
	 * 
	 * Tests some strategies for writing key/value pairs to scalaris:
	 * <ol>
	 *  <li>writing {@link OtpErlangBinary} objects (random data, size = {@link #BENCH_DATA_SIZE})</li>
	 *  <li>writing {@link OtpErlangString} objects (random data, size = {@link #BENCH_DATA_SIZE})</li>
	 *  <li>writing {@link String} objects (random data, size = {@link #BENCH_DATA_SIZE})</li>
	 * </ol>
	 * each {@link #BENCH_TEST_RUNS} times
	 * <ul>
	 *  <li>first using a new {@link Transaction} for each test,</li> 
	 *  <li>then using a new {@link Transaction} but re-using a single {@link OtpConnection},</li>
	 *  <li>and finally re-using a single {@link Transaction} object.</li>
	 * </ul>
	 */
    public static void minibench() {
    	long[][] results = new long[3][3];
    	
    	results[0][0] = bench1(BENCH_DATA_SIZE, BENCH_TEST_RUNS);
    	results[1][0] = bench2(BENCH_DATA_SIZE, BENCH_TEST_RUNS);
    	results[2][0] = bench3(BENCH_DATA_SIZE, BENCH_TEST_RUNS);
    	results[0][1] = bench4(BENCH_DATA_SIZE, BENCH_TEST_RUNS);
    	results[1][1] = bench5(BENCH_DATA_SIZE, BENCH_TEST_RUNS);
    	results[2][1] = bench6(BENCH_DATA_SIZE, BENCH_TEST_RUNS);
    	results[0][2] = bench7(BENCH_DATA_SIZE, BENCH_TEST_RUNS);
    	results[1][2] = bench8(BENCH_DATA_SIZE, BENCH_TEST_RUNS);
    	results[2][2] = bench9(BENCH_DATA_SIZE, BENCH_TEST_RUNS);
    	
    	String[] columns = {
				"Transaction.writeObject(OtpErlangString, OtpErlangBinary)",
				"Transaction.writeObject(OtpErlangString, OtpErlangString)",
				"Transaction.write(String, String)" };
    	String[] rows = {
    			"separate connection",
    			"re-use connection",
				"re-use transaction" };
    	printResults(columns, rows, results);
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
    protected static void printResults(String[] columns, String[] rows, long[][] results) {
    	final String firstColumn = new String("                         ");
    	System.out.print(firstColumn);
    	for (int i = 0; i < columns.length; ++i) {
    		System.out.print("\t(" + (i + 1) + ")");
		}
    	System.out.println();
    	
    	for (int i = 0; i < rows.length; ++i) {
    		System.out.print(rows[i] + firstColumn.substring(0, firstColumn.length() - rows[i].length() - 1));
    		for (int j = 0; j < columns.length; j++) {
    			System.out.print("\t" + results[i][j]);
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
//		System.out.println("  transactions : " + testRuns);
//		System.out.println("  time         : " + timeTaken + "ms");
//		System.out.println("  speed        : " + speed + " Transactions/s");
		return speed;
    }

	/**
	 * Performs a benchmark writing {@link OtpErlangBinary} objects (random
	 * data, size = {@link #BENCH_DATA_SIZE}) using a new {@link Transaction}
	 * for each test.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
    protected static long bench1(int size, int testRuns) {
		try {
//			System.out.println("Testing Transaction().writeObject(OtpErlangString, OtpErlangBinary) " +
//					"with separate connections...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_bench1";
			OtpErlangBinary value = new OtpErlangBinary(data);
			
			testBegin();

			for (int i = 0; i < testRuns; ++i) {
				Transaction transaction = new Transaction();
				transaction.start();
				transaction.writeObject(new OtpErlangString(key + i), value);
				transaction.commit();
				transaction.closeConnection();
			}

			long speed = testEnd(testRuns);
			return speed;
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
    }

	/**
	 * Performs a benchmark writing {@link OtpErlangBinary} objects (random
	 * data, size = {@link #BENCH_DATA_SIZE}) using a new {@link Transaction}
	 * but re-using a single {@link OtpConnection} for each test.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
    protected static long bench2(int size, int testRuns) {
		try {
//			System.out.println("Testing Transaction(OtpConnection).writeObject(OtpErlangString, OtpErlangBinary) " +
//					"re-using a single connection...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_bench2";
			OtpErlangBinary value = new OtpErlangBinary(data);

			testBegin();

			OtpConnection connection = ConnectionFactory.getInstance().createConnection();
			for (int i = 0; i < testRuns; ++i) {
				Transaction transaction = new Transaction(connection);
				transaction.start();
				transaction.writeObject(new OtpErlangString(key + i), value);
				transaction.commit();
			}
			connection.close();

			long speed = testEnd(testRuns);
			return speed;
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
    }

	/**
	 * Performs a benchmark writing {@link OtpErlangBinary} objects (random
	 * data, size = {@link #BENCH_DATA_SIZE}) using a single
	 * {@link Transaction} object for all tests.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
    protected static long bench3(int size, int testRuns) {
		try {
//			System.out.println("Testing Transaction().writeObject(OtpErlangString, OtpErlangBinary) " +
//					"re-using a single transaction...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_bench3";
			OtpErlangBinary value = new OtpErlangBinary(data);

			testBegin();

			Transaction transaction = new Transaction();
			for (int i = 0; i < testRuns; ++i) {
				transaction.start();
				transaction.writeObject(new OtpErlangString(key + i), value);
				transaction.commit();
				transaction.reset();
			}
			transaction.closeConnection();

			long speed = testEnd(testRuns);
			return speed;
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
    }

	/**
	 * Performs a benchmark writing {@link OtpErlangString} objects (random
	 * data, size = {@link #BENCH_DATA_SIZE}) first using a new
	 * {@link Transaction} for each test.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
    protected static long bench4(int size, int testRuns) {
		try {
//			System.out.println("Testing Transaction().writeObject(OtpErlangString, OtpErlangString) " +
//					"with separate connections...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_bench4";
			OtpErlangString value = new OtpErlangString(new String(data));

			testBegin();

			for (int i = 0; i < testRuns; ++i) {
				Transaction transaction = new Transaction();
				transaction.start();
				transaction.writeObject(new OtpErlangString(key + i), value);
				transaction.commit();
				transaction.closeConnection();
			}

			long speed = testEnd(testRuns);
			return speed;
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
    }

	/**
	 * Performs a benchmark writing {@link OtpErlangString} objects (random
	 * data, size = {@link #BENCH_DATA_SIZE}) using a new {@link Transaction}
	 * but re-using a single {@link OtpConnection} for each test.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
    protected static long bench5(int size, int testRuns) {
		try {
//			System.out.println("Testing Transaction(OtpConnection).writeObject(OtpErlangString, OtpErlangString) " +
//					"re-using a single connection...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_bench5";
			OtpErlangString value = new OtpErlangString(new String(data));

			testBegin();

			OtpConnection connection = ConnectionFactory.getInstance().createConnection();
			for (int i = 0; i < testRuns; ++i) {
				Transaction transaction = new Transaction(connection);
				transaction.start();
				transaction.writeObject(new OtpErlangString(key + i), value);
				transaction.commit();
			}
			connection.close();

			long speed = testEnd(testRuns);
			return speed;
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
    }

	/**
	 * Performs a benchmark writing {@link OtpErlangString} objects (random
	 * data, size = {@link #BENCH_DATA_SIZE}) re-using a single
	 * {@link Transaction} object for all tests..
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
    protected static long bench6(int size, int testRuns) {
		try {
//			System.out.println("Testing Transaction().writeObject(OtpErlangString, OtpErlangString) " +
//					"re-using a single transaction...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_bench6";
			OtpErlangString value = new OtpErlangString(new String(data));

			testBegin();

			Transaction transaction = new Transaction();
			for (int i = 0; i < testRuns; ++i) {
				transaction.start();
				transaction.writeObject(new OtpErlangString(key + i), value);
				transaction.commit();
				transaction.reset();
			}
			transaction.closeConnection();

			long speed = testEnd(testRuns);
			return speed;
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
    }

	/**
	 * Performs a benchmark writing {@link String} objects (random data, size =
	 * {@link #BENCH_DATA_SIZE}) using a new {@link Transaction} for each test.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
    protected static long bench7(int size, int testRuns) {
		try {
//			System.out.println("Testing Transaction().write(String, String) " +
//					"with separate connections...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_bench7";
			String value = new String(data);

			testBegin();

			for (int i = 0; i < testRuns; ++i) {
				Transaction transaction = new Transaction();
				transaction.start();
				transaction.write(key + i, value);
				transaction.commit();
				transaction.closeConnection();
			}

			long speed = testEnd(testRuns);
			return speed;
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
    }

	/**
	 * Performs a benchmark writing {@link String} objects (random data, size =
	 * {@link #BENCH_DATA_SIZE}) using a new {@link Transaction} but re-using a
	 * single {@link OtpConnection} for each test.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
    protected static long bench8(int size, int testRuns) {
		try {
//			System.out.println("Testing Transaction(OtpConnection).write(String, String) " +
//					"re-using a single connection...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_bench8";
			String value = new String(data);

			testBegin();

			OtpConnection connection = ConnectionFactory.getInstance().createConnection();
			for (int i = 0; i < testRuns; ++i) {
				Transaction transaction = new Transaction(connection);
				transaction.start();
				transaction.write(key + i, value);
				transaction.commit();
			}
			connection.close();

			long speed = testEnd(testRuns);
			return speed;
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
    }

	/**
	 * Performs a benchmark writing {@link String} objects (random data, size =
	 * {@link #BENCH_DATA_SIZE}) re-using a single {@link Transaction} object
	 * for all tests.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
    protected static long bench9(int size, int testRuns) {
		try {
//			System.out.println("Testing Transaction().write(String, String) " +
//					"re-using a single transaction...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_bench9";
			String value = new String(data);

			testBegin();

			Transaction transaction = new Transaction();
			for (int i = 0; i < testRuns; ++i) {
				transaction.start();
				transaction.write(key + i, value);
				transaction.commit();
				transaction.reset();
			}
			transaction.closeConnection();

			long speed = testEnd(testRuns);
			return speed;
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
    }
}
