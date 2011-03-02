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
package de.zib.scalaris.examples;

import java.util.Random;

import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangString;

import de.zib.scalaris.Benchmark;
import de.zib.scalaris.Connection;
import de.zib.scalaris.ConnectionFactory;
import de.zib.scalaris.Scalaris;
import de.zib.scalaris.Transaction;

/**
 * Mini benchmark of the {@link Transaction} class using custom objects provided
 * by {@link CustomOtpFastStringObject}.
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 2.1
 * @since 2.0
 */
public class FastStringBenchmark extends Benchmark {

	/**
	 * Runs a mini benchmark of the {@link Transaction} class using custom
	 * objects provided by {@link CustomOtpFastStringObject}.
	 * Accepts the same parameters as the
	 * {@link de.zib.scalaris.Main#main(String[])} method's benchmark
	 * parameter. 
	 * 
	 * @param args
	 *            command line arguments
	 */
	public static void main(String[] args) {
		int testruns = 100;
		int benchmarks = -1;
		if (args != null && args.length == 2) {
			testruns = Integer.parseInt(args[0]);
			String benchmarks_str = args[1];
			benchmarks = benchmarks_str.equals("all") ? -1 : Integer.parseInt(benchmarks_str);
		}
		minibench(testruns, benchmarks);
	}
	
	/**
	 * Runs the benchmark.
	 * 
	 * Tests some strategies for writing key/value pairs to scalaris:
	 * <ol>
	 *  <li>writing {@link OtpErlangBinary} objects (random data, size = {@link #BENCH_DATA_SIZE})</li>
	 *  <li>writing {@link OtpErlangString} objects (random data, size = {@link #BENCH_DATA_SIZE})</li>
	 *  <li>writing {@link String} objects (random data, size = {@link #BENCH_DATA_SIZE})</li>
	 *  <li>writing {@link String} objects by converting them to {@link OtpErlangBinary}s
	 *      (random data, size = {@link #BENCH_DATA_SIZE})</li>
	 * </ol>
	 * each testruns times
	 * <ul>
	 *  <li>first using a new {@link Transaction} for each test,</li> 
	 *  <li>then using a new {@link Transaction} but re-using a single {@link Connection},</li>
	 *  <li>and finally re-using a single {@link Transaction} object.</li>
	 * </ul>
	 * 
	 * @param testruns
	 *            the number of test runs to execute
	 * @param benchmarks
	 *            the benchmarks to run (1-12 or -1 for all benchmarks)
	 */
    public static void minibench(int testruns, int benchmarks) {
		long[][] results = new long[3][4];
		String[] columns;
		String[] rows;
		
		System.out.println("Benchmark of de.zib.scalaris.Scalaris:");

		results[0][0] = benchmarks == -1 || benchmarks == 1 ? Benchmark.scalarisBench1(BENCH_DATA_SIZE, testruns) : -1;
		results[1][0] = benchmarks == -1 || benchmarks == 1 ? Benchmark.scalarisBench2(BENCH_DATA_SIZE, testruns) : -1;
		results[2][0] = benchmarks == -1 || benchmarks == 1 ? Benchmark.scalarisBench3(BENCH_DATA_SIZE, testruns) : -1;
		results[0][1] = benchmarks == -1 || benchmarks == 1 ? Benchmark.scalarisBench4(BENCH_DATA_SIZE, testruns) : -1;
		results[1][1] = benchmarks == -1 || benchmarks == 1 ? Benchmark.scalarisBench5(BENCH_DATA_SIZE, testruns) : -1;
		results[2][1] = benchmarks == -1 || benchmarks == 1 ? Benchmark.scalarisBench6(BENCH_DATA_SIZE, testruns) : -1;
		results[0][2] = benchmarks == -1 || benchmarks == 1 ? Benchmark.scalarisBench7(BENCH_DATA_SIZE, testruns) : -1;
		results[1][2] = benchmarks == -1 || benchmarks == 1 ? Benchmark.scalarisBench8(BENCH_DATA_SIZE, testruns) : -1;
		results[2][2] = benchmarks == -1 || benchmarks == 1 ? Benchmark.scalarisBench9(BENCH_DATA_SIZE, testruns) : -1;
    	results[0][3] = benchmarks == -1 || benchmarks == 1 ? fastScalarisBench1(BENCH_DATA_SIZE, testruns) : -1;
    	results[1][3] = benchmarks == -1 || benchmarks == 1 ? fastScalarisBench2(BENCH_DATA_SIZE, testruns) : -1;
    	results[2][3] = benchmarks == -1 || benchmarks == 1 ? fastScalarisBench3(BENCH_DATA_SIZE, testruns) : -1;

		columns = new String[] {
				"Scalaris.writeObject(OtpErlangString, OtpErlangBinary)",
				"Scalaris.writeObject(OtpErlangString, OtpErlangString)",
				"Scalaris.write(String, String)",
				"Scalaris.writeCustom(String, CustomOtpFastStringObject)" };
		rows = new String[] {
				"separate connection",
				"re-use connection",
				"re-use Scalaris object" };
		printResults(columns, rows, results, testruns);
		
		
		results = new long[3][4];
		System.out.println("-----");
		System.out.println("Benchmark of de.zib.scalaris.Transaction:");

		results[0][0] = benchmarks == -1 || benchmarks == 1 ? Benchmark.transBench1(BENCH_DATA_SIZE, testruns) : -1;
		results[1][0] = benchmarks == -1 || benchmarks == 1 ? Benchmark.transBench2(BENCH_DATA_SIZE, testruns) : -1;
		results[2][0] = benchmarks == -1 || benchmarks == 1 ? Benchmark.transBench3(BENCH_DATA_SIZE, testruns) : -1;
		results[0][1] = benchmarks == -1 || benchmarks == 1 ? Benchmark.transBench4(BENCH_DATA_SIZE, testruns) : -1;
		results[1][1] = benchmarks == -1 || benchmarks == 1 ? Benchmark.transBench5(BENCH_DATA_SIZE, testruns) : -1;
		results[2][1] = benchmarks == -1 || benchmarks == 1 ? Benchmark.transBench6(BENCH_DATA_SIZE, testruns) : -1;
		results[0][2] = benchmarks == -1 || benchmarks == 1 ? Benchmark.transBench7(BENCH_DATA_SIZE, testruns) : -1;
		results[1][2] = benchmarks == -1 || benchmarks == 1 ? Benchmark.transBench8(BENCH_DATA_SIZE, testruns) : -1;
		results[2][2] = benchmarks == -1 || benchmarks == 1 ? Benchmark.transBench9(BENCH_DATA_SIZE, testruns) : -1;
    	results[0][3] = benchmarks == -1 || benchmarks == 1 ? fastTransBench1(BENCH_DATA_SIZE, testruns) : -1;
    	results[1][3] = benchmarks == -1 || benchmarks == 1 ? fastTransBench2(BENCH_DATA_SIZE, testruns) : -1;
    	results[2][3] = benchmarks == -1 || benchmarks == 1 ? fastTransBench3(BENCH_DATA_SIZE, testruns) : -1;

		columns = new String[] {
				"Transaction.writeObject(OtpErlangString, OtpErlangBinary)",
				"Transaction.writeObject(OtpErlangString, OtpErlangString)",
				"Transaction.write(String, String)",
				"Transaction.writeCustom(String, CustomOtpFastStringObject)" };
		rows = new String[] {
				"separate connection",
				"re-use connection",
				"re-use transaction" };
		printResults(columns, rows, results, testruns);
	}

	/**
	 * Performs a benchmark writing {@link String} objects by converting them to
	 * {@link OtpErlangBinary}s (random data, size = {@link #BENCH_DATA_SIZE})
	 * using a new {@link Transaction} for each test.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
    protected static long fastTransBench1(int size, int testRuns) {
		try {
//			System.out.println("Testing FastStringTransaction().write(String, String) " +
//					"with separate connections...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_fastTransBench1";
			String value = new String(data);

			testBegin();

			for (int i = 0; i < testRuns; ++i) {
				Transaction transaction = new Transaction();
				transaction.writeCustom(key + i, new CustomOtpFastStringObject(value));
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
	 * Performs a benchmark writing {@link String} objects by converting them to
	 * {@link OtpErlangBinary}s (random data, size = {@link #BENCH_DATA_SIZE})
	 * using a new {@link Transaction} but re-using a single
	 * {@link Connection} for each test.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
    protected static long fastTransBench2(int size, int testRuns) {
		try {
//			System.out.println("Testing FastStringTransaction(Connection).write(String, String) " +
//					"re-using a single connection...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_fastTransBench2";
			String value = new String(data);

			testBegin();

			Connection connection = ConnectionFactory.getInstance().createConnection();
			for (int i = 0; i < testRuns; ++i) {
				Transaction transaction = new Transaction(connection);
				transaction.writeCustom(key + i, new CustomOtpFastStringObject(value));
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
	 * Performs a benchmark writing {@link String} objects by converting them to
	 * {@link OtpErlangBinary}s (random data, size = {@link #BENCH_DATA_SIZE})
	 * using a single {@link Transaction} object for all tests.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
    protected static long fastTransBench3(int size, int testRuns) {
		try {
//			System.out.println("Testing FastStringTransaction().write(String, String) " +
//					"re-using a single transaction...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_fastTransBench3";
			String value = new String(data);

			testBegin();

			Transaction transaction = new Transaction();
			for (int i = 0; i < testRuns; ++i) {
				transaction.writeCustom(key + i, new CustomOtpFastStringObject(value));
				transaction.commit();
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
	 * Performs a benchmark writing {@link String} objects by converting them to
	 * {@link OtpErlangBinary}s (random data, size = {@link #BENCH_DATA_SIZE})
	 * using a new {@link Scalaris} object for each test.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
    protected static long fastScalarisBench1(int size, int testRuns) {
		try {
//			System.out.println("Testing FastStringTransaction().write(String, String) " +
//					"with separate connections...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_fastTransBench1";
			String value = new String(data);

			testBegin();

			for (int i = 0; i < testRuns; ++i) {
				Scalaris sc = new Scalaris();
				sc.writeCustom(key + i, new CustomOtpFastStringObject(value));
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
	 * Performs a benchmark writing {@link String} objects by converting them to
	 * {@link OtpErlangBinary}s (random data, size = {@link #BENCH_DATA_SIZE})
	 * using a new {@link Scalaris} object but re-using a single
	 * {@link Connection} for each test.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
    protected static long fastScalarisBench2(int size, int testRuns) {
		try {
//			System.out.println("Testing FastStringTransaction(Connection).write(String, String) " +
//					"re-using a single connection...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_fastTransBench2";
			String value = new String(data);

			testBegin();

			Connection connection = ConnectionFactory.getInstance().createConnection();
			for (int i = 0; i < testRuns; ++i) {
				Scalaris sc = new Scalaris(connection);
				sc.writeCustom(key + i, new CustomOtpFastStringObject(value));
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
	 * Performs a benchmark writing {@link String} objects by converting them to
	 * {@link OtpErlangBinary}s (random data, size = {@link #BENCH_DATA_SIZE})
	 * using a single {@link Scalaris} object for all tests.
	 * 
	 * @param size
	 *            the size of a single data item
	 * @param testRuns
	 *            the number of times to write the value
	 * 
	 * @return the number of achieved transactions per second
	 */
    protected static long fastScalarisBench3(int size, int testRuns) {
		try {
//			System.out.println("Testing FastStringTransaction().write(String, String) " +
//					"re-using a single transaction...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_fastTransBench3";
			String value = new String(data);

			testBegin();

			Scalaris sc = new Scalaris();
			for (int i = 0; i < testRuns; ++i) {
				sc.writeCustom(key + i, new CustomOtpFastStringObject(value));
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
