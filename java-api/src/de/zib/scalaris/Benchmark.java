/**
 *  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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

import java.util.Random;

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
	 * <li>writing {@link OtpErlangBinary} objects (random data, size =
	 * {@link #BENCH_DATA_SIZE})</li>
	 * <li>writing {@link OtpErlangString} objects (random data, size =
	 * {@link #BENCH_DATA_SIZE})</li>
	 * <li>writing {@link String} objects (random data, size =
	 * {@link #BENCH_DATA_SIZE})</li>
	 * </ol>
	 * each testruns times
	 * <ul>
	 * <li>first using a new {@link Transaction} for each test,</li>
	 * <li>then using a new {@link Transaction} but re-using a single
	 * {@link Connection},</li>
	 * <li>and finally re-using a single {@link Transaction} object.</li>
	 * </ul>
	 * 
	 * @param testruns
	 *            the number of test runs to execute
	 * @param benchmarks
	 *            the benchmarks to run (1-9 or -1 for all benchmarks)
	 */
	public static void minibench(int testruns, int benchmarks) {
		long[][] results = new long[3][3];
		String[] columns;
		String[] rows;
		
		System.out.println("Benchmark of de.zib.scalaris.Scalaris:");

		results[0][0] = benchmarks == -1 || benchmarks == 1 ? scalarisBench1(BENCH_DATA_SIZE, testruns) : -1;
		results[1][0] = benchmarks == -1 || benchmarks == 2 ? scalarisBench2(BENCH_DATA_SIZE, testruns) : -1;
		results[2][0] = benchmarks == -1 || benchmarks == 3 ? scalarisBench3(BENCH_DATA_SIZE, testruns) : -1;
		results[0][1] = benchmarks == -1 || benchmarks == 4 ? scalarisBench4(BENCH_DATA_SIZE, testruns) : -1;
		results[1][1] = benchmarks == -1 || benchmarks == 5 ? scalarisBench5(BENCH_DATA_SIZE, testruns) : -1;
		results[2][1] = benchmarks == -1 || benchmarks == 6 ? scalarisBench6(BENCH_DATA_SIZE, testruns) : -1;
		results[0][2] = benchmarks == -1 || benchmarks == 7 ? scalarisBench7(BENCH_DATA_SIZE, testruns) : -1;
		results[1][2] = benchmarks == -1 || benchmarks == 8 ? scalarisBench8(BENCH_DATA_SIZE, testruns) : -1;
		results[2][2] = benchmarks == -1 || benchmarks == 9 ? scalarisBench9(BENCH_DATA_SIZE, testruns) : -1;

		columns = new String[] {
				"Scalaris.writeObject(OtpErlangString, OtpErlangBinary)",
				"Scalaris.writeObject(OtpErlangString, OtpErlangString)",
				"Scalaris.write(String, String)" };
		rows = new String[] {
				"separate connection",
				"re-use connection",
				"re-use Scalaris object" };
		printResults(columns, rows, results, testruns);
		
		
		results = new long[3][3];
		System.out.println("-----");
		System.out.println("Benchmark of de.zib.scalaris.Transaction:");

		results[0][0] = benchmarks == -1 || benchmarks == 1 ? transBench1(BENCH_DATA_SIZE, testruns) : -1;
		results[1][0] = benchmarks == -1 || benchmarks == 2 ? transBench2(BENCH_DATA_SIZE, testruns) : -1;
		results[2][0] = benchmarks == -1 || benchmarks == 3 ? transBench3(BENCH_DATA_SIZE, testruns) : -1;
		results[0][1] = benchmarks == -1 || benchmarks == 4 ? transBench4(BENCH_DATA_SIZE, testruns) : -1;
		results[1][1] = benchmarks == -1 || benchmarks == 5 ? transBench5(BENCH_DATA_SIZE, testruns) : -1;
		results[2][1] = benchmarks == -1 || benchmarks == 6 ? transBench6(BENCH_DATA_SIZE, testruns) : -1;
		results[0][2] = benchmarks == -1 || benchmarks == 7 ? transBench7(BENCH_DATA_SIZE, testruns) : -1;
		results[1][2] = benchmarks == -1 || benchmarks == 8 ? transBench8(BENCH_DATA_SIZE, testruns) : -1;
		results[2][2] = benchmarks == -1 || benchmarks == 9 ? transBench9(BENCH_DATA_SIZE, testruns) : -1;

		columns = new String[] {
				"Transaction.writeObject(OtpErlangString, OtpErlangBinary)",
				"Transaction.writeObject(OtpErlangString, OtpErlangString)",
				"Transaction.write(String, String)" };
		rows = new String[] {
				"separate connection",
				"re-use connection",
				"re-use transaction" };
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
		System.out.println("Test runs: " + testruns);
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
					+ firstColumn.substring(0, firstColumn.length()
							- rows[i].length() - 1));
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
		// System.out.println("  transactions : " + testRuns);
		// System.out.println("  time         : " + timeTaken + "ms");
		// System.out.println("  speed        : " + speed + " Transactions/s");
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
	protected static long transBench1(int size, int testRuns) {
		try {
			// System.out.println("Testing Transaction().writeObject(OtpErlangString, OtpErlangBinary) "
			// +
			// "with separate connections...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_transBench1";
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
	 * but re-using a single {@link Connection} for each test.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
	protected static long transBench2(int size, int testRuns) {
		try {
			// System.out.println("Testing Transaction(Connection).writeObject(OtpErlangString, OtpErlangBinary) "
			// +
			// "re-using a single connection...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_transBench2";
			OtpErlangBinary value = new OtpErlangBinary(data);

			testBegin();

			Connection connection = ConnectionFactory.getInstance()
					.createConnection();
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
	 * data, size = {@link #BENCH_DATA_SIZE}) using a single {@link Transaction}
	 * object for all tests.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
	protected static long transBench3(int size, int testRuns) {
		try {
			// System.out.println("Testing Transaction().writeObject(OtpErlangString, OtpErlangBinary) "
			// +
			// "re-using a single transaction...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_transBench3";
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
	protected static long transBench4(int size, int testRuns) {
		try {
			// System.out.println("Testing Transaction().writeObject(OtpErlangString, OtpErlangString) "
			// +
			// "with separate connections...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_transBench4";
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
	 * but re-using a single {@link Connection} for each test.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
	protected static long transBench5(int size, int testRuns) {
		try {
			// System.out.println("Testing Transaction(Connection).writeObject(OtpErlangString, OtpErlangString) "
			// +
			// "re-using a single connection...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_transBench5";
			OtpErlangString value = new OtpErlangString(new String(data));

			testBegin();

			Connection connection = ConnectionFactory.getInstance()
					.createConnection();
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
	protected static long transBench6(int size, int testRuns) {
		try {
			// System.out.println("Testing Transaction().writeObject(OtpErlangString, OtpErlangString) "
			// +
			// "re-using a single transaction...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_transBench6";
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
	protected static long transBench7(int size, int testRuns) {
		try {
			// System.out.println("Testing Transaction().write(String, String) "
			// +
			// "with separate connections...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_transBench7";
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
	 * single {@link Connection} for each test.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
	protected static long transBench8(int size, int testRuns) {
		try {
			// System.out.println("Testing Transaction(Connection).write(String, String) "
			// +
			// "re-using a single connection...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_transBench8";
			String value = new String(data);

			testBegin();

			Connection connection = ConnectionFactory.getInstance()
					.createConnection();
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
	protected static long transBench9(int size, int testRuns) {
		try {
			// System.out.println("Testing Transaction().write(String, String) "
			// +
			// "re-using a single transaction...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_transBench9";
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

	/**
	 * Performs a benchmark writing {@link OtpErlangBinary} objects (random
	 * data, size = {@link #BENCH_DATA_SIZE}) using a new {@link Scalaris}
	 * object for each test.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
	protected static long scalarisBench1(int size, int testRuns) {
		try {
			// System.out.println("Testing Scalaris().writeObject(OtpErlangString, OtpErlangBinary) "
			// +
			// "with separate connections...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_scalarisBench1";
			OtpErlangBinary value = new OtpErlangBinary(data);

			testBegin();

			for (int i = 0; i < testRuns; ++i) {
				Scalaris sc = new Scalaris();
				sc.writeObject(new OtpErlangString(key + i), value);
				sc.closeConnection();
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
	 * data, size = {@link #BENCH_DATA_SIZE}) using a new {@link Scalaris}
	 * object but re-using a single {@link Connection} for each test.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
	protected static long scalarisBench2(int size, int testRuns) {
		try {
			// System.out.println("Testing Scalaris(Connection).writeObject(OtpErlangString, OtpErlangBinary) "
			// +
			// "re-using a single connection...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_scalarisBench2";
			OtpErlangBinary value = new OtpErlangBinary(data);

			testBegin();

			Connection connection = ConnectionFactory.getInstance()
					.createConnection();
			for (int i = 0; i < testRuns; ++i) {
				Scalaris sc = new Scalaris(connection);
				sc.writeObject(new OtpErlangString(key + i), value);
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
	 * data, size = {@link #BENCH_DATA_SIZE}) using a single {@link Scalaris}
	 * object for all tests.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
	protected static long scalarisBench3(int size, int testRuns) {
		try {
			// System.out.println("Testing Scalaris().writeObject(OtpErlangString, OtpErlangBinary) "
			// +
			// "re-using a single transaction...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_scalarisBench3";
			OtpErlangBinary value = new OtpErlangBinary(data);

			testBegin();

			Scalaris sc = new Scalaris();
			for (int i = 0; i < testRuns; ++i) {
				sc.writeObject(new OtpErlangString(key + i), value);
			}
			sc.closeConnection();

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
	 * {@link Scalaris} object for each test.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
	protected static long scalarisBench4(int size, int testRuns) {
		try {
			// System.out.println("Testing Scalaris().writeObject(OtpErlangString, OtpErlangString) "
			// +
			// "with separate connections...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_scalarisBench4";
			OtpErlangString value = new OtpErlangString(new String(data));

			testBegin();

			for (int i = 0; i < testRuns; ++i) {
				Scalaris sc = new Scalaris();
				sc.writeObject(new OtpErlangString(key + i), value);
				sc.closeConnection();
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
	 * data, size = {@link #BENCH_DATA_SIZE}) using a new {@link Scalaris}
	 * object but re-using a single {@link Connection} for each test.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
	protected static long scalarisBench5(int size, int testRuns) {
		try {
			// System.out.println("Testing Scalaris(Connection).writeObject(OtpErlangString, OtpErlangString) "
			// +
			// "re-using a single connection...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_scalarisBench5";
			OtpErlangString value = new OtpErlangString(new String(data));

			testBegin();

			Connection connection = ConnectionFactory.getInstance()
					.createConnection();
			for (int i = 0; i < testRuns; ++i) {
				Scalaris sc = new Scalaris(connection);
				sc.writeObject(new OtpErlangString(key + i), value);
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
	 * {@link Scalaris} object for all tests..
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
	protected static long scalarisBench6(int size, int testRuns) {
		try {
			// System.out.println("Testing Scalaris().writeObject(OtpErlangString, OtpErlangString) "
			// +
			// "re-using a single transaction...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_scalarisBench6";
			OtpErlangString value = new OtpErlangString(new String(data));

			testBegin();

			Scalaris sc = new Scalaris();
			for (int i = 0; i < testRuns; ++i) {
				sc.writeObject(new OtpErlangString(key + i), value);
			}
			sc.closeConnection();

			long speed = testEnd(testRuns);
			return speed;
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
	}

	/**
	 * Performs a benchmark writing {@link String} objects (random data, size =
	 * {@link #BENCH_DATA_SIZE}) using a new {@link Scalaris} object for each
	 * test.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
	protected static long scalarisBench7(int size, int testRuns) {
		try {
			// System.out.println("Testing Scalaris().write(String, String) "
			// +
			// "with separate connections...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_scalarisBench7";
			String value = new String(data);

			testBegin();

			for (int i = 0; i < testRuns; ++i) {
				Scalaris sc = new Scalaris();
				sc.write(key + i, value);
				sc.closeConnection();
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
	 * {@link #BENCH_DATA_SIZE}) using a new {@link Scalaris} object but
	 * re-using a single {@link Connection} for each test.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
	protected static long scalarisBench8(int size, int testRuns) {
		try {
			// System.out.println("Testing Scalaris(Connection).write(String, String) "
			// +
			// "re-using a single connection...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_scalarisBench8";
			String value = new String(data);

			testBegin();

			Connection connection = ConnectionFactory.getInstance()
					.createConnection();
			for (int i = 0; i < testRuns; ++i) {
				Scalaris sc = new Scalaris(connection);
				sc.write(key + i, value);
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
	 * {@link #BENCH_DATA_SIZE}) re-using a single {@link Scalaris} object for
	 * all tests.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
	protected static long scalarisBench9(int size, int testRuns) {
		try {
			// System.out.println("Testing Scalaris().write(String, String) "
			// +
			// "re-using a single transaction...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_scalarisBench9";
			String value = new String(data);

			testBegin();

			Scalaris sc = new Scalaris();
			for (int i = 0; i < testRuns; ++i) {
				sc.write(key + i, value);
			}
			sc.closeConnection();

			long speed = testEnd(testRuns);
			return speed;
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
	}
}
