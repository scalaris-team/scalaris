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

import java.util.concurrent.TimeUnit;

import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangString;

import de.zib.scalaris.Benchmark;

/**
 * Mini benchmark of the {@link de.zib.scalaris.Transaction} and
 * {@link de.zib.scalaris.TransactionSingleOp} class using custom objects
 * provided by {@link ErlangValueFastString} and {@link ErlangValueBitString}.
 * 
 * <p>
 * Run the benchmark with
 * <code>java -cp scalaris-examples.jar de.zib.scalaris.examples.FastStringBenchmark</code>
 * </p>
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 3.0
 * @since 2.0
 */
public class FastStringBenchmark extends Benchmark {
    /**
     * Runs a mini benchmark of the {@link de.zib.scalaris.Transaction} and
     * {@link de.zib.scalaris.TransactionSingleOp} class using custom objects
     * provided by {@link ErlangValueFastString} and
     * {@link ErlangValueBitString}. Accepts the same parameters as the
     * {@link de.zib.scalaris.Main#main(String[])} method's benchmark parameter.
     * 
     * @param args
     *            command line arguments
     * 
     * @see de.zib.scalaris.Main#main(String[])
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
     *  <li>first using a new {@link de.zib.scalaris.Transaction} for each test,</li> 
     *  <li>then using a new {@link de.zib.scalaris.Transaction} but re-using a single {@link de.zib.scalaris.Connection},</li>
     *  <li>and finally re-using a single {@link de.zib.scalaris.Transaction} object.</li>
     * </ul>
     * 
     * @param testruns
     *            the number of test runs to execute
     * @param benchmarks
     *            the benchmarks to run (1-9 or -1 for all benchmarks)
     */
    public static void minibench(int testruns, int benchmarks) {
        long[][] results = getResultArray(3, 3);
        String[] columns;
        String[] rows;
        
        System.out.println("Benchmark of de.zib.scalaris.TransactionSingleOp:");

        try {
            if (benchmarks == -1 || benchmarks == 1) {
                results[0][0] = 
                    transSingleOpBench1(testruns, getRandom(BENCH_DATA_SIZE, String.class), "transsinglebench_S_1");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (benchmarks == -1 || benchmarks == 2) {
                results[1][0] = 
                    transSingleOpBench2(testruns, getRandom(BENCH_DATA_SIZE, String.class), "transsinglebench_S_2");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (benchmarks == -1 || benchmarks == 3) {
                results[2][0] = 
                    transSingleOpBench3(testruns, getRandom(BENCH_DATA_SIZE, String.class), "transsinglebench_S_3");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (benchmarks == -1 || benchmarks == 4) {
                results[0][1] = 
                    transSingleOpBench1(testruns, getRandom(BENCH_DATA_SIZE, ErlangValueBitString.class), "transsinglebench_EVBS_1");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (benchmarks == -1 || benchmarks == 5) {
                results[1][1] = 
                    transSingleOpBench2(testruns, getRandom(BENCH_DATA_SIZE, ErlangValueBitString.class), "transsinglebench_EVBS_2");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (benchmarks == -1 || benchmarks == 6) {
                results[2][1] = 
                    transSingleOpBench3(testruns, getRandom(BENCH_DATA_SIZE, ErlangValueBitString.class), "transsinglebench_EVBS_3");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (benchmarks == -1 || benchmarks == 7) {
                results[0][2] = 
                    transSingleOpBench1(testruns, getRandom(BENCH_DATA_SIZE, ErlangValueFastString.class), "transsinglebench_EVFS_1");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (benchmarks == -1 || benchmarks == 8) {
                results[1][2] = 
                    transSingleOpBench2(testruns, getRandom(BENCH_DATA_SIZE, ErlangValueFastString.class), "transsinglebench_EVFS_2");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (benchmarks == -1 || benchmarks == 9) {
                results[2][2] = 
                    transSingleOpBench3(testruns, getRandom(BENCH_DATA_SIZE, ErlangValueFastString.class), "transsinglebench_EVFS_3");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        columns = new String[] {
                "TransactionSingleOp.write(String, String)",
                "TransactionSingleOp.write(String, ErlangValueBitString)",
                "TransactionSingleOp.write(String, ErlangValueFastString)" };
        rows = new String[] {
                "separate connection",
                "re-use connection",
                "re-use object" };
        printResults(columns, rows, results, testruns);
        
        
        results = getResultArray(3, 3);
        System.out.println("-----");
        System.out.println("Benchmark of de.zib.scalaris.Transaction:");

        try {
            if (benchmarks == -1 || benchmarks == 1) {
                results[0][0] = 
                    transBench1(testruns, getRandom(BENCH_DATA_SIZE, String.class), "transbench_S_1");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (benchmarks == -1 || benchmarks == 2) {
                results[1][0] = 
                    transBench2(testruns, getRandom(BENCH_DATA_SIZE, String.class), "transbench_S_2");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (benchmarks == -1 || benchmarks == 3) {
                results[2][0] = 
                    transBench3(testruns, getRandom(BENCH_DATA_SIZE, String.class), "transbench_S_3");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (benchmarks == -1 || benchmarks == 4) {
                results[0][1] = 
                    transBench1(testruns, getRandom(BENCH_DATA_SIZE, ErlangValueBitString.class), "transbench_EVBS_1");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (benchmarks == -1 || benchmarks == 5) {
                results[1][1] = 
                    transBench2(testruns, getRandom(BENCH_DATA_SIZE, ErlangValueBitString.class), "transbench_EVBS_2");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (benchmarks == -1 || benchmarks == 6) {
                results[2][1] = 
                    transBench3(testruns, getRandom(BENCH_DATA_SIZE, ErlangValueBitString.class), "transbench_EVBS_3");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (benchmarks == -1 || benchmarks == 7) {
                results[0][2] = 
                    transBench1(testruns, getRandom(BENCH_DATA_SIZE, ErlangValueFastString.class), "transbench_EVFS_1");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (benchmarks == -1 || benchmarks == 8) {
                results[1][2] = 
                    transBench2(testruns, getRandom(BENCH_DATA_SIZE, ErlangValueFastString.class), "transbench_EVFS_2");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (benchmarks == -1 || benchmarks == 9) {
                results[2][2] = 
                    transBench3(testruns, getRandom(BENCH_DATA_SIZE, ErlangValueFastString.class), "transbench_EVFS_3");
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        columns = new String[] {
                "Transaction.write(String, String)",
                "Transaction.write(String, ErlangValueBitString)",
                "Transaction.write(String, ErlangValueFastString)" };
        rows = new String[] {
                "separate connection",
                "re-use connection",
                "re-use object" };
        printResults(columns, rows, results, testruns);
    }
}
