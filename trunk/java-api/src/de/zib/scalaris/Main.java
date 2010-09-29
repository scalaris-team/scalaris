/*
 *  Copyright 2007-2010 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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

import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

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
	 * > java -jar scalaris.jar -help
	 * usage: scalaris [Options]
	 *  -b,--minibench                   run mini benchmark
	 *  -d,--delete <key> <[timeout]>    delete an item (default timeout: 2000ms)
	 *                                   WARNING: This function can lead to
	 *                                   inconsistent data (e.g. deleted items
	 *                                   can re-appear). Also when re-creating an
	 *                                   item the version before the delete can
	 *                                   re-appear.
	 *  -g,--getsubscribers <topic>      get subscribers of a topic
	 *  -h,--help                        print this message
	 *  -lh,--localhost                  gets the local host's name as known to
	 *                                   Java (for debugging purposes)
	 *  -p,--publish <topic> <message>   publish a new message for the given
	 *                                   topic
	 *  -r,--read <key>                  read an item
	 *  -s,--subscribe <topic> <url>     subscribe to a topic
	 *  -u,--unsubscribe <topic> <url>   unsubscribe from a topic
	 *  -v,--verbose                     print verbose information, e.g. the
	 *                                   properties read
	 *  -w,--write <key> <value>         write an item
	 * </code>
	 * </pre>
	 * 
	 * @param args
	 *            command line arguments
	 */
	public static void main(String[] args) {
		boolean verbose = false;
		CommandLineParser parser = new GnuParser();
		CommandLine line = null;
		Options options = getOptions();
		try {
			line = parser.parse(options, args);
		} catch (ParseException e) {
			printException("Parsing failed", e, false);
		}

		if (line.hasOption("verbose")) {
	        verbose = true;
	        ConnectionFactory.getInstance().printProperties();
		}

		if (line.hasOption("minibench")) {
			String[] optionValues = line.getOptionValues("minibench");
			int testruns = 100;
			int benchmarks = -1;
			if (optionValues != null) {
				checkArguments(optionValues, 2, options, "b");
				testruns = Integer.parseInt(optionValues[0]);
				String benchmarks_str = optionValues[1];
				benchmarks = benchmarks_str.equals("all") ? -1 : Integer.parseInt(benchmarks_str);
			}
	        Benchmark.minibench(testruns, benchmarks);
		} else if (line.hasOption("r")) { // read
			String key = line.getOptionValue("read");
			checkArguments(key, options, "r");
			try {
				Scalaris sc = new Scalaris();
				String value = sc.read(key);
				System.out.println("read(" + key + ") == " + value);
			} catch (ConnectionException e) {
				printException("read failed with connection error", e, verbose);
			} catch (TimeoutException e) {
				printException("read failed with timeout", e, verbose);
			} catch (NotFoundException e) {
				printException("read failed with not found", e, verbose);
			} catch (UnknownException e) {
				printException("read failed with unknown", e, verbose);
			}
		} else if (line.hasOption("w")) { // write
			String[] optionValues = line.getOptionValues("write");
			checkArguments(optionValues, 2, options, "w");
			String key = optionValues[0];
			String value = optionValues[1];
			try {
				Scalaris sc = new Scalaris();
				sc.write(key, value);
				System.out.println("write(" + key + ", " + value + "): ok");
			} catch (ConnectionException e) {
				printException("write failed with connection error", e, verbose);
			} catch (TimeoutException e) {
				printException("write failed with timeout", e, verbose);
			} catch (UnknownException e) {
				printException("write failed with unknown", e, verbose);
			}
		} else if (line.hasOption("p")) { // publish
			String[] optionValues = line.getOptionValues("publish");
			checkArguments(optionValues, 2, options, "p");
			String topic = optionValues[0];
			String content = optionValues[1];
			if (content == null) {
				// parsing methods of commons.cli only checks the first argument :(
				printException("Parsing failed", new ParseException("missing content for option p"), verbose);
			}
			try {
				Scalaris sc = new Scalaris();
				sc.publish(topic, content);
				System.out.println("publish(" + topic + ", " + content + "): ok");
			} catch (ConnectionException e) {
				printException("publish failed with connection error", e, verbose);
			}
		} else if (line.hasOption("s")) { // subscribe
			String[] optionValues = line.getOptionValues("subscribe");
			checkArguments(optionValues, 2, options, "s");
			String topic = optionValues[0];
			String url = optionValues[1];
			try {
				Scalaris sc = new Scalaris();
				sc.subscribe(topic, url);
				System.out.println("subscribe(" + topic + ", " + url + "): ok");
			} catch (ConnectionException e) {
				printException("subscribe failed with connection error", e, verbose);
			} catch (TimeoutException e) {
				printException("subscribe failed with timeout", e, verbose);
			} catch (UnknownException e) {
				printException("subscribe failed with unknown", e, verbose);
			}
		} else if (line.hasOption("u")) { // unsubscribe
			String[] optionValues = line.getOptionValues("unsubscribe");
			checkArguments(optionValues, 2, options, "u");
			String topic = optionValues[0];
			String url = optionValues[1];
			try {
				Scalaris sc = new Scalaris();
				sc.unsubscribe(topic, url);
				System.out.println("unsubscribe(" + topic + ", " + url + "): ok");
			} catch (ConnectionException e) {
				printException("unsubscribe failed with connection error", e, verbose);
			} catch (TimeoutException e) {
				printException("unsubscribe failed with timeout", e, verbose);
			} catch (NotFoundException e) {
				printException("unsubscribe failed with not found", e, verbose);
			} catch (UnknownException e) {
				printException("unsubscribe failed with unknown", e, verbose);
			}
		} else if (line.hasOption("g")) { // getsubscribers
			String topic = line.getOptionValue("getsubscribers");
			checkArguments(topic, options, "g");
			try {
				Scalaris sc = new Scalaris();
				ArrayList<String> subscribers = sc.getSubscribers(topic);
				System.out.println("getSubscribers(" + topic + ") == "
						+ subscribers);
			} catch (ConnectionException e) {
				printException("getSubscribers failed with connection error", e, verbose);
			} catch (UnknownException e) {
				printException("getSubscribers failed with unknown error", e, verbose);
			}
		} else if (line.hasOption("d")) { // delete
			String[] optionValues = line.getOptionValues("delete");
			checkArguments(optionValues, 1, options, "d");
			String key = optionValues[0];
			int timeout = 2000;
			if (optionValues.length >= 2) {
				try {
					timeout = Integer.parseInt(optionValues[1]);
				} catch (Exception e) {
					printException("Parsing failed", new ParseException(
							"wrong type for timeout parameter of option d"
									+ " (parameters: <"
									+ options.getOption("d").getArgName()
									+ ">)"), verbose);
				}
			}
			try {
				Scalaris sc = new Scalaris();
				sc.delete(key, timeout);
				DeleteResult deleteResult = sc.getLastDeleteResult();
				System.out.println("delete(" + key + ", " + timeout + "): "
						+ deleteResult.ok + " ok, "
						+ deleteResult.locks_set + " locks_set, "
						+ deleteResult.undef + " undef");
			} catch (ConnectionException e) {
				printException("delete failed with connection error", e, verbose);
			} catch (TimeoutException e) {
				printException("delete failed with timeout", e, verbose);
			} catch (NodeNotFoundException e) {
				printException("delete failed with node not found", e, verbose);
			} catch (UnknownException e) {
				printException("delete failed with unknown error", e, verbose);
			}
		} else if (line.hasOption("lh")) { // get local host name
			System.out.println(ConnectionFactory.getLocalhostName());
		} else {
			// print help if no other option was given
//		if (line.hasOption("help")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("scalaris [Options]", getOptions());
		}
	}

	/**
	 * Creates the options the command line should understand.
	 * 
	 * @return the options the program understands
	 */
	private static Options getOptions() {
		Options options = new Options();
		OptionGroup group = new OptionGroup();

		/* Note: arguments are set to be optional since we implement argument
		 * checks on our own (commons.cli is not flexible enough and only
		 * checks for the existence of a first argument)
		 */
		
		options.addOption(new Option("h", "help", false, "print this message"));
		
		options.addOption(new Option("v", "verbose", false, "print verbose information, e.g. the properties read"));

		Option read = new Option("r", "read", true, "read an item");
		read.setArgName("key");
		read.setArgs(1);
		read.setOptionalArg(true);
		group.addOption(read);

		Option write = new Option("w", "write", true, "write an item");
		write.setArgName("key> <value");
		write.setArgs(2);
		write.setOptionalArg(true);
		group.addOption(write);

		Option publish = new Option("p", "publish", true, "publish a new message for the given topic");
		publish.setArgName("topic> <message");
		publish.setArgs(2);
		publish.setOptionalArg(true);
		group.addOption(publish);

		Option subscribe = new Option("s", "subscribe", true, "subscribe to a topic");
		subscribe.setArgName("topic> <url");
		subscribe.setArgs(2);
		subscribe.setOptionalArg(true);
		group.addOption(subscribe);
		
		Option unsubscribe = new Option("u", "unsubscribe", true, "unsubscribe from a topic");
		unsubscribe.setArgName("topic> <url");
		unsubscribe.setArgs(2);
		unsubscribe.setOptionalArg(true);
		group.addOption(unsubscribe);

		Option getSubscribers = new Option("g", "getsubscribers", true, "get subscribers of a topic");
		getSubscribers.setArgName("topic");
		getSubscribers.setArgs(1);
		getSubscribers.setOptionalArg(true);
		group.addOption(getSubscribers);

		Option delete = new Option("d", "delete", true,
				"delete an item (default timeout: 2000ms)\n" +
				"WARNING: This function can lead to inconsistent data (e.g. " +
				"deleted items can re-appear). Also when re-creating an item " +
				"the version before the delete can re-appear.");
		delete.setArgName("key> <[timeout]");
		delete.setArgs(2);
		delete.setOptionalArg(true);
		group.addOption(delete);

		Option bench = new Option("b", "minibench", true, "run selected mini benchmark [1|...|9|all] (default: all benchmarks, 100 test runs)");
		bench.setArgName("runs> <benchmark");
		bench.setArgs(2);
		bench.setOptionalArg(true);
		group.addOption(bench);

		options.addOption(new Option("lh", "localhost", false, "gets the local host's name as known to Java (for debugging purposes)"));

		options.addOptionGroup(group);

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
	private final static void printException(String description, ParseException e, boolean verbose) {
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
	private final static void printException(String description, ConnectionException e, boolean verbose) {
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
	private final static void printException(String description, TimeoutException e, boolean verbose) {
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
	private final static void printException(String description, NotFoundException e, boolean verbose) {
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
	private final static void printException(String description, UnknownException e, boolean verbose) {
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
	private final static void printException(String description, NodeNotFoundException e, boolean verbose) {
		printException(description, e, verbose, 6);
	}
	
	/**
	 * Prints the given exception with the given description and terminates the
	 * JVM.
	 * 
	 * @param description  will be prepended to the error message
	 * @param e            the exception to print
	 * @param verbose      specifies whether to include the stack trace or not
	 * @param exitStatus   the status code the JVM exits with 
	 */
	private final static void printException(String description, Exception e, boolean verbose, int exitStatus) {
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
	private final static void checkArguments(String optionValue,
			Options options, String currentOption) {
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
	private final static void checkArguments(String[] optionValues,
			int required, Options options, String currentOption) {
		if (optionValues == null || optionValues.length < required) {
			printException("Parsing failed", new ParseException(
					"missing parameter for option " + currentOption
							+ " (required: <"
							+ options.getOption(currentOption).getArgName()
							+ ">)"), false);
		}
	}
}
