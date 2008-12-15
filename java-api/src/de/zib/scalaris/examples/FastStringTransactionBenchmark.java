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
package de.zib.scalaris.examples;

import java.util.Random;

import com.ericsson.otp.erlang.OtpConnection;
import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangString;

import de.zib.scalaris.Benchmark;
import de.zib.scalaris.ConnectionFactory;
import de.zib.scalaris.Transaction;

/**
 * Mini benchmark of the {@link Transaction} and {@link FastStringTransaction}
 * classes.
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 2.0
 * @since 2.0
 */
public class FastStringTransactionBenchmark extends Benchmark {

	/**
	 * Runs a mini benchmark of the {@link Transaction} and
	 * {@link FastStringTransaction} classes.
	 * 
	 * @param args
	 *            command line arguments
	 */
	public static void main(String[] args) {
		minibench();
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
	 * each {@link #BENCH_TEST_RUNS} times
	 * <ul>
	 *  <li>first using a new {@link Transaction} for each test,</li> 
	 *  <li>then using a new {@link Transaction} but re-using a single {@link OtpConnection},</li>
	 *  <li>and finally re-using a single {@link Transaction} object.</li>
	 * </ul>
	 */
    public static void minibench() {
    	long[][] results = new long[3][4];
    	
    	results[0][0] = Benchmark.bench1(BENCH_DATA_SIZE, BENCH_TEST_RUNS);
    	results[1][0] = Benchmark.bench2(BENCH_DATA_SIZE, BENCH_TEST_RUNS);
    	results[2][0] = Benchmark.bench3(BENCH_DATA_SIZE, BENCH_TEST_RUNS);
    	results[0][1] = Benchmark.bench4(BENCH_DATA_SIZE, BENCH_TEST_RUNS);
    	results[1][1] = Benchmark.bench5(BENCH_DATA_SIZE, BENCH_TEST_RUNS);
    	results[2][1] = Benchmark.bench6(BENCH_DATA_SIZE, BENCH_TEST_RUNS);
    	results[0][2] = Benchmark.bench7(BENCH_DATA_SIZE, BENCH_TEST_RUNS);
    	results[1][2] = Benchmark.bench8(BENCH_DATA_SIZE, BENCH_TEST_RUNS);
    	results[2][2] = Benchmark.bench9(BENCH_DATA_SIZE, BENCH_TEST_RUNS);
    	results[0][3] = bench1(BENCH_DATA_SIZE, BENCH_TEST_RUNS);
    	results[1][3] = bench2(BENCH_DATA_SIZE, BENCH_TEST_RUNS);
    	results[2][3] = bench3(BENCH_DATA_SIZE, BENCH_TEST_RUNS);
    	
    	String[] columns = {
				"Transaction.writeObject(OtpErlangString, OtpErlangBinary)",
				"Transaction.writeObject(OtpErlangString, OtpErlangString)",
				"Transaction.write(String, String)",
				"FastTransaction.write(String, String)" };
    	String[] rows = {
    			"separate connection",
    			"re-use connection",
				"re-use transaction" };
    	printResults(columns, rows, results);
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
    protected static long bench1(int size, int testRuns) {
		try {
//			System.out.println("Testing FastStringTransaction().write(String, String) " +
//					"with separate connections...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_bench10";
			String value = new String(data);

			testBegin();

			for (int i = 0; i < testRuns; ++i) {
				FastStringTransaction transaction = new FastStringTransaction();
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
	 * Performs a benchmark writing {@link String} objects by converting them to
	 * {@link OtpErlangBinary}s (random data, size = {@link #BENCH_DATA_SIZE})
	 * using a new {@link Transaction} but re-using a single
	 * {@link OtpConnection} for each test.
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
//			System.out.println("Testing FastStringTransaction(OtpConnection).write(String, String) " +
//					"re-using a single connection...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_bench11";
			String value = new String(data);

			testBegin();

			OtpConnection connection = ConnectionFactory.getInstance().createConnection();
			for (int i = 0; i < testRuns; ++i) {
				FastStringTransaction transaction = new FastStringTransaction(connection);
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
    protected static long bench3(int size, int testRuns) {
		try {
//			System.out.println("Testing FastStringTransaction().write(String, String) " +
//					"re-using a single transaction...");
			byte[] data = new byte[size];
			Random r = new Random();
			r.nextBytes(data);

			String key = benchTime + "_bench12";
			String value = new String(data);

			testBegin();

			FastStringTransaction transaction = new FastStringTransaction();
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
