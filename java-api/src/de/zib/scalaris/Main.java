/*
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

import java.lang.management.ManagementFactory;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.HashSet;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import de.zib.scalaris.Monitor.GetNodeInfoResult;
import de.zib.scalaris.Monitor.GetNodePerformanceResult;
import de.zib.scalaris.Monitor.GetServiceInfoResult;
import de.zib.scalaris.Monitor.GetServicePerformanceResult;

/**
 * Class to test basic functionality of the package and to use scalaris
 * from command line.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 2.0
 * @since 2.0
 */
public class Main {
    /**
     * Queries the command line options for an action to perform.
     *
     * <pre>
     * <code>
     * > java -jar scalaris.jar --help
     * usage: scalaris [Options]
     *  -h,--help                                   print this message
     *  -v,--verbose                                print verbose information,
     *                                              e.g. the properties read
     *  -lh,--localhost                             gets the local host's name as
     *                                              known to Java (for debugging
     *                                              purposes)
     *  -b,--minibench <[ops]> <[tpn]> <[benchs]>   run selected mini
     *                                              benchmark(s) [1|...|18|all]
     *                                              (default: all benchmarks, 500
     *                                              operations, 10 threads per
     *                                              Scalaris node)
     *  -m,--monitor <node>                         print monitoring information
     *  -r,--read <key>                             read an item
     *  -w,--write <key> <value>                    write an item
     *     --test-and-set <key> <old> <new>         atomic test and set, i.e.
     *                                              write <key> to <new> if the
     *                                              current value is <old>
     *  -d,--delete <key> <[timeout]>               delete an item (default
     *                                              timeout: 2000ms)
     *                                              WARNING: This function can
     *                                              lead to inconsistent data
     *                                              (e.g. deleted items can
     *                                              re-appear). Also when
     *                                              re-creating an item the
     *                                              version before the delete can
     *                                              re-appear.
     *  -jmx,--jmxservice <node>                    starts a service exposing
     *                                              Scalaris monitoring values
     *                                              via JMX
     * </code>
     * </pre>
     *
     * In order to override node and cookie to use for a connection, specify
     * the <tt>scalaris.node</tt> or <tt>scalaris.cookie</tt> system properties.
     * Their values will be used instead of the values defined in the config
     * file!
     *
     * @param args
     *            command line arguments
     */
    public static void main(final String[] args) {
        boolean verbose = false;
        final CommandLineParser parser = new GnuParser();
        CommandLine line = null;
        final Options options = getOptions();
        try {
            line = parser.parse(options, args);
        } catch (final ParseException e) {
            printException("Parsing failed", e, false);
            return; // will not be reached since printException exits
        }

        if (line.hasOption("verbose")) {
            verbose = true;
            ConnectionFactory.getInstance().printProperties();
        }

        if (line.hasOption("minibench")) {
            final String[] optionValues = line.getOptionValues("minibench");
            int nrOperations = 500;
            int threadsPerNode = 10;
            final HashSet<Integer> benchmarks = new HashSet<Integer>(18);
            boolean all = true;
            if (optionValues != null) {
                checkArguments(optionValues, 0, options, "b");
                if (optionValues.length >= 1) {
                    nrOperations = Integer.parseInt(optionValues[0]);
                }
                if (optionValues.length >= 2) {
                    threadsPerNode = Integer.parseInt(optionValues[1]);
                }
                if (optionValues.length >= 3) {
                    all = false;
                    for (int i = 2; i < Math.min(20, optionValues.length); ++i) {
                        final String benchmarks_str = optionValues[i];
                        if (benchmarks_str.equals("all")) {
                            all = true;
                        } else {
                            benchmarks.add(Integer.parseInt(benchmarks_str));
                        }
                    }
                }
            }
            if (all) {
                for (int i = 1; i <= 18; ++i) {
                    benchmarks.add(i);
                }
            }
            Benchmark.minibench(nrOperations, threadsPerNode, benchmarks);
        } else if (line.hasOption("r")) { // read
            final String key = line.getOptionValue("read");
            checkArguments(key, options, "r");
            try {
                final TransactionSingleOp sc = new TransactionSingleOp();
                final String value = sc.read(key).value().toString();
                System.out.println("read(" + key + ") == " + value);
            } catch (final ConnectionException e) {
                printException("read failed with connection error", e, verbose);
            } catch (final NotFoundException e) {
                printException("read failed with not found", e, verbose);
            } catch (final UnknownException e) {
                printException("read failed with unknown", e, verbose);
            }
        } else if (line.hasOption("w")) { // write
            final String[] optionValues = line.getOptionValues("write");
            checkArguments(optionValues, 2, options, "w");
            final String key = optionValues[0];
            final String value = optionValues[1];
            try {
                final TransactionSingleOp sc = new TransactionSingleOp();
                sc.write(key, value);
                System.out.println("write(" + key + ", " + value + "): ok");
            } catch (final ConnectionException e) {
                printException("write failed with connection error", e, verbose);
            } catch (final AbortException e) {
                printException("write failed with abort", e, verbose);
            } catch (final UnknownException e) {
                printException("write failed with unknown", e, verbose);
            }
        } else if (line.hasOption("test-and-set")) { // test_and_set
            final String[] optionValues = line.getOptionValues("test-and-set");
            checkArguments(optionValues, 3, options, "test-and-set");
            final String key = optionValues[0];
            final String oldValue = optionValues[1];
            final String newValue = optionValues[2];
            try {
                final TransactionSingleOp sc = new TransactionSingleOp();
                sc.testAndSet(key, oldValue, newValue);
                System.out.println("testAndSet(" + key + ", " + oldValue + ", " + newValue + "): ok");
            } catch (final ConnectionException e) {
                printException("testAndSet failed with connection error", e, verbose);
            } catch (final AbortException e) {
                printException("testAndSet failed with abort", e, verbose);
            } catch (final UnknownException e) {
                printException("testAndSet failed with unknown", e, verbose);
            } catch (final NotFoundException e) {
                printException("testAndSet failed with not found", e, verbose);
            } catch (final KeyChangedException e) {
                printException("testAndSet failed with key changed (current value: " + e.getOldValue().toString() + ")", e, verbose);
            }
        } else if (line.hasOption("d")) { // delete
            final String[] optionValues = line.getOptionValues("delete");
            checkArguments(optionValues, 1, options, "d");
            final String key = optionValues[0];
            int timeout = 2000;
            if (optionValues.length >= 2) {
                try {
                    timeout = Integer.parseInt(optionValues[1]);
                } catch (final Exception e) {
                    printException("Parsing failed", new ParseException(
                            "wrong type for timeout parameter of option d"
                                    + " (parameters: <"
                                    + options.getOption("d").getArgName()
                                    + ">)"), verbose);
                }
            }
            try {
                final ReplicatedDHT sc = new ReplicatedDHT();
                final DeleteResult deleteResult = sc.delete(key, timeout);
                System.out.println("delete(" + key + ", " + timeout + "): "
                        + deleteResult.ok + " ok, "
                        + deleteResult.locks_set + " locks_set, "
                        + deleteResult.undef + " undef");
            } catch (final ConnectionException e) {
                printException("delete failed with connection error", e, verbose);
            } catch (final TimeoutException e) {
                printException("delete failed with timeout", e, verbose);
            } catch (final UnknownException e) {
                printException("delete failed with unknown error", e, verbose);
            }
        } else if (line.hasOption("lh")) { // get local host name
            System.out.println(ConnectionFactory.getLocalhostName());
        } else if (line.hasOption("monitor")) { // print monitoring data
            final String node = line.getOptionValue("monitor");
            checkArguments(node, options, "monitor");
            try {
                final Monitor monitor = new Monitor(node);
                final GetNodeInfoResult nodeInfo = monitor.getNodeInfo();
                final GetNodePerformanceResult nodePerf = monitor.getNodePerformance();
                final GetServiceInfoResult srvInfo = monitor.getServiceInfo();
                final GetServicePerformanceResult srvPerf = monitor.getServicePerformance();

                final Double nodePerfCurLatAvg = Monitor.getCurrentPerfValue(nodePerf.latencyAvg);
                final Double nodePerfCurLatStddev = Monitor.getCurrentPerfValue(nodePerf.latencyStddev);
                final Double srvPerfCurLatAvg = Monitor.getCurrentPerfValue(srvPerf.latencyAvg);
                final Double srcPerfCurLatStddev = Monitor.getCurrentPerfValue(srvPerf.latencyStddev);

                final DecimalFormat df = new DecimalFormat("0.##");
                System.out.println("== Node Info ==");
                System.out.println("Scalaris version: " + nodeInfo.scalarisVersion);
                System.out.println("Erlang   version: " + nodeInfo.erlangVersion);
                System.out.println("# of DHT nodes  : " + nodeInfo.dhtNodes);
                System.out.println("== Service Info (from mgmt_server) ==");
                System.out.println("Total # of nodes: " + srvInfo.nodes);
                System.out.println("Total load      : " + srvInfo.totalLoad);
                System.out.println("== Node Performance ==");
                System.out.println("Current latency : " + (nodePerfCurLatAvg == null ? "n/a" : df.format(nodePerfCurLatAvg)));
                System.out.println("Current stddev  : " + (nodePerfCurLatStddev == null ? "n/a" : df.format(nodePerfCurLatStddev)));
                System.out.println("== Service Performance ==");
                System.out.println("Current latency : " + (srvPerfCurLatAvg == null ? "n/a" : df.format(srvPerfCurLatAvg)));
                System.out.println("Current stddev  : " + (srcPerfCurLatStddev == null ? "n/a" : df.format(srcPerfCurLatStddev)));
            } catch (final ConnectionException e) {
                printException("monitor failed with connection error", e, verbose);
            } catch (final UnknownException e) {
                printException("monitor failed with unknown error", e, verbose);
            }
        } else if (line.hasOption("jmx")) { // start JMX monitoring service
            final String node = line.getOptionValue("jmx");
            checkArguments(node, options, "jmx");
            startJmxService(node, verbose);
        } else {
            // print help if no other option was given
//        if (line.hasOption("help")) {
            final HelpFormatter formatter = new HelpFormatter();
            formatter.setOptionComparator(new Comparator<Option>() {
                private int optionToInt(final Option option) {
                    if (option.getLongOpt().equals("help")) {
                        return 1;
                    } else if (option.getLongOpt().equals("verbose")) {
                        return 2;
                    } else if (option.getLongOpt().equals("localhost")) {
                        return 3;
                    } else if (option.getLongOpt().equals("minibench")) {
                        return 4;
                    } else if (option.getLongOpt().equals("monitor")) {
                        return 5;
                    } else if (option.getLongOpt().equals("read")) {
                        return 6;
                    } else if (option.getLongOpt().equals("write")) {
                        return 7;
                    } else if (option.getLongOpt().equals("test-and-set")) {
                        return 8;
                    } else if (option.getLongOpt().equals("add-del-on-list")) {
                        return 9;
                    } else if (option.getLongOpt().equals("add-on-nr")) {
                        return 10;
                    } else if (option.getLongOpt().equals("delete")) {
                        return 11;
                    } else if (option.getLongOpt().equals("jmxservice")) {
                        return 12;
                    } else {
                        return 13;
                    }
                }

                public int compare(final Option arg0, final Option arg1) {
                    final int arg0_i = optionToInt(arg0);
                    final int arg1_i = optionToInt(arg1);
                    if (arg0_i < arg1_i) {
                        return -1;
                    } else if (arg0_i == arg1_i) {
                        return 0;
                    } else {
                        return 1;
                    }
                }
            });
            formatter.printHelp("scalaris [Options]", getOptions());
            if (!line.hasOption("help")) {
                System.exit(1);
            }
        }
    }

    /**
     * Registers some MBeans to monitor Scalaris via JMX and then waits forever
     * until interrupted.
     *
     * @param node
     *            the node name of the Erlang VM to connect to
     * @param verbose
     *            whether verbose information should be printed in case of
     *            connection failures
     */
    private static void startJmxService(final String node, final boolean verbose) {
        try {
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            final ObjectName nodeMonitorName = new ObjectName("de.zib.scalaris:type=MonitorNode");
            final de.zib.scalaris.jmx.MonitorNode nodeMonitorMbean = new de.zib.scalaris.jmx.MonitorNode(node);
            final ObjectName serviceMonitorName = new ObjectName("de.zib.scalaris:type=MonitorService");
            final de.zib.scalaris.jmx.MonitorService serviceMonitorMbean = new de.zib.scalaris.jmx.MonitorService(node);
            mbs.registerMBean(nodeMonitorMbean, nodeMonitorName);
            mbs.registerMBean(serviceMonitorMbean, serviceMonitorName);
            System.out.println("Waiting forever...");
            Thread.sleep(Long.MAX_VALUE);
        } catch (final InterruptedException e) {
            System.out.println("stopped service");
        } catch (final MalformedObjectNameException e) {
            throw new RuntimeException(e);
        } catch (final NullPointerException e) {
            throw new RuntimeException(e);
        } catch (final ConnectionException e) {
            printException("JMX service failed with connection error", e, verbose);
        } catch (final InstanceAlreadyExistsException e) {
            throw new RuntimeException(e);
        } catch (final MBeanRegistrationException e) {
            throw new RuntimeException(e);
        } catch (final NotCompliantMBeanException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates the options the command line should understand.
     *
     * @return the options the program understands
     */
    private static Options getOptions() {
        final Options options = new Options();
        final OptionGroup group = new OptionGroup();

        /* Note: arguments are set to be optional since we implement argument
         * checks on our own (commons.cli is not flexible enough and only
         * checks for the existence of a first argument)
         */

        options.addOption(new Option("h", "help", false, "print this message"));

        options.addOption(new Option("v", "verbose", false, "print verbose information, e.g. the properties read"));

        final Option read = new Option("r", "read", true, "read an item");
        read.setArgName("key");
        read.setArgs(1);
        read.setOptionalArg(true);
        group.addOption(read);

        final Option write = new Option("w", "write", true, "write an item");
        write.setArgName("key> <value");
        write.setArgs(2);
        write.setOptionalArg(true);
        group.addOption(write);

        final Option test_and_set = new Option(null, "test-and-set", true,
                "atomic test and set, i.e. write <key> to " +
                "<new> if the current value is <old>");
        test_and_set.setArgName("key> <old> <new");
        test_and_set.setArgs(3);
        test_and_set.setOptionalArg(true);
        group.addOption(test_and_set);

        final Option delete = new Option("d", "delete", true,
                "delete an item (default timeout: 2000ms)\n" +
                "WARNING: This function can lead to inconsistent data (e.g. " +
                "deleted items can re-appear). Also when re-creating an item " +
                "the version before the delete can re-appear.");
        delete.setArgName("key> <[timeout]");
        delete.setArgs(2);
        delete.setOptionalArg(true);
        group.addOption(delete);

        final Option bench = new Option("b", "minibench", true, "run selected mini benchmark(s) [1|...|18|all] (default: all benchmarks, 500 operations, 10 threads per Scalaris node)");
        bench.setArgName("[ops]> <[tpn]> <[benchs]");
        bench.setArgs(20);
        bench.setOptionalArg(true);
        group.addOption(bench);

        final Option monitor = new Option("m", "monitor", true, "print monitoring information");
        monitor.setArgName("node");
        monitor.setArgs(1);
        monitor.setOptionalArg(true);
        group.addOption(monitor);

        final Option jmx = new Option("jmx", "jmxservice", true, "starts a service exposing Scalaris monitoring values via JMX");
        jmx.setArgName("node");
        jmx.setArgs(1);
        jmx.setOptionalArg(true);
        group.addOption(jmx);

        options.addOptionGroup(group);

        options.addOption(new Option("lh", "localhost", false, "gets the local host's name as known to Java (for debugging purposes)"));


        return options;
    }

    /**
     * Prints the given exception with the given description and terminates the
     * JVM.
     *
     * @param description  will be prepended to the error message
     * @param e            the exception to print
     * @param verbose      specifies whether to include the stack trace or not
     */
    final static void printException(final String description, final ParseException e, final boolean verbose) {
        printException(description, e, verbose, 1);
    }

    /**
     * Prints the given exception with the given description and terminates the
     * JVM.
     *
     * @param description  will be prepended to the error message
     * @param e            the exception to print
     * @param verbose      specifies whether to include the stack trace or not
     */
    final static void printException(final String description, final ConnectionException e, final boolean verbose) {
        printException(description, e, verbose, 2);
    }

    /**
     * Prints the given exception with the given description and terminates the
     * JVM.
     *
     * @param description  will be prepended to the error message
     * @param e            the exception to print
     * @param verbose      specifies whether to include the stack trace or not
     */
    final static void printException(final String description, final TimeoutException e, final boolean verbose) {
        printException(description, e, verbose, 3);
    }

    /**
     * Prints the given exception with the given description and terminates the
     * JVM.
     *
     * @param description  will be prepended to the error message
     * @param e            the exception to print
     * @param verbose      specifies whether to include the stack trace or not
     */
    final static void printException(final String description, final NotFoundException e, final boolean verbose) {
        printException(description, e, verbose, 4);
    }

    /**
     * Prints the given exception with the given description and terminates the
     * JVM.
     *
     * @param description  will be prepended to the error message
     * @param e            the exception to print
     * @param verbose      specifies whether to include the stack trace or not
     */
    final static void printException(final String description, final UnknownException e, final boolean verbose) {
        printException(description, e, verbose, 5);
    }

    /**
     * Prints the given exception with the given description and terminates the
     * JVM.
     *
     * @param description  will be prepended to the error message
     * @param e            the exception to print
     * @param verbose      specifies whether to include the stack trace or not
     */
    final static void printException(final String description, final AbortException e, final boolean verbose) {
        printException(description, e, verbose, 7);
    }

    /**
     * Prints the given exception with the given description and terminates the
     * JVM.
     *
     * @param description  will be prepended to the error message
     * @param e            the exception to print
     * @param verbose      specifies whether to include the stack trace or not
     */
    final static void printException(final String description, final KeyChangedException e, final boolean verbose) {
        printException(description, e, verbose, 8);
    }

    /**
     * Prints the given exception with the given description and terminates the
     * JVM.
     *
     * Note: exit status 6 was used by the NodeNotFoundException which is not
     * needed anymore - do not re-use this exit status!
     *
     * @param description  will be prepended to the error message
     * @param e            the exception to print
     * @param verbose      specifies whether to include the stack trace or not
     * @param exitStatus   the status code the JVM exits with
     */
    final static void printException(final String description, final Exception e, final boolean verbose, final int exitStatus) {
        System.err.print(description + ": ");
        if (verbose) {
            System.err.println();
            e.printStackTrace();
        } else {
            System.err.println(e.getMessage());
        }
        System.exit(exitStatus);
    }

    /**
     * Checks that the given option value as returned from e.g.
     * {@link CommandLine#getOptionValue(String)} does exist and prints an error
     * message if not.
     *
     * @param optionValue    the value to check
     * @param options        the available command line options
     * @param currentOption  the short name of the current option being parsed
     */
    final static void checkArguments(final String optionValue,
            final Options options, final String currentOption) {
        if (optionValue == null) {
            printException("Parsing failed", new ParseException(
                    "missing parameter for option " + currentOption
                            + " (required: <"
                            + options.getOption(currentOption).getArgName()
                            + ">)"), false);
        }
    }

    /**
     * Checks that the given option values as returned from e.g.
     * {@link CommandLine#getOptionValues(String)} do exist and contain
     * enough parameters. Prints an error message if not.
     *
     * @param optionValues   the values to check
     * @param required       the number of required parameters
     * @param options        the available command line options
     * @param currentOption  the short name of the current option being parsed
     */
    final static void checkArguments(final String[] optionValues,
            final int required, final Options options, final String currentOption) {
        if ((optionValues == null) || (optionValues.length < required)) {
            printException("Parsing failed", new ParseException(
                    "missing parameter for option " + currentOption
                            + " (required: <"
                            + options.getOption(currentOption).getArgName()
                            + ">)"), false);
        }
    }
}
