/*
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
	 * usage: scalaris
	 *  -getsubscribers <topic>   get subscribers of a topic
	 *  -help                     print this message
	 *  -publish <params>         publish a new message for a topic: <topic> <message>
	 *  -read <key>               read an item
	 *  -subscribe <params>       subscribe to a topic: <topic> <url>
	 *  -unsubscribe <params>     unsubscribe from a topic: <topic> <url>
	 *  -write <params>           write an item: <key> <value>
	 *  -minibench                run mini benchmark
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
		try {
			line = parser.parse(getOptions(), args);
		} catch (ParseException e) {
			printException("Parsing failed. Reason", e, true);
			System.exit(0);
		}

		if (line.hasOption("verbose")) {
	        verbose = true;
	        ConnectionFactory.getInstance().printProperties();
		}

		if (line.hasOption("minibench")) {
	        Benchmark.minibench();
		} else if (line.hasOption("r")) { // read
			try {
				Scalaris sc = new Scalaris();
				System.out.println("read(" + line.getOptionValue("read")
						+ ") == "
						+ sc.read(line.getOptionValue("read")));
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
			try {
				Scalaris sc = new Scalaris();
				System.out.println("write(" + line.getOptionValues("write")[0]
						+ ", " + line.getOptionValues("write")[1] + ")");
				sc.write(line.getOptionValues("write")[0], line
						.getOptionValues("write")[1]);
			} catch (ConnectionException e) {
				printException("write failed with connection error", e, verbose);
			} catch (TimeoutException e) {
				printException("write failed with timeout", e, verbose);
			} catch (UnknownException e) {
				printException("write failed with unknown", e, verbose);
			}
		} else if (line.hasOption("p")) { // publish
			try {
				Scalaris sc = new Scalaris();
				System.out.println("publish("
						+ line.getOptionValues("publish")[0] + ", "
						+ line.getOptionValues("publish")[1] + ")");
				sc.publish(line.getOptionValues("publish")[0], line
						.getOptionValues("publish")[1]);
			} catch (ConnectionException e) {
				printException("publish failed with connection error", e, verbose);
//			} catch (TimeoutException e) {
//				printException("publish failed with timeout", e, verbose);
//			} catch (UnknownException e) {
//				printException("write failed with unknown", e, verbose);
			}
		} else if (line.hasOption("s")) { // subscribe
			try {
				Scalaris sc = new Scalaris();
				System.out.println("subscribe("
						+ line.getOptionValues("subscribe")[0] + ", "
						+ line.getOptionValues("subscribe")[1] + ")");
				sc.subscribe(line.getOptionValues("subscribe")[0], line
						.getOptionValues("subscribe")[1]);
			} catch (ConnectionException e) {
				printException("subscribe failed with connection error", e, verbose);
			} catch (TimeoutException e) {
				printException("subscribe failed with timeout", e, verbose);
			} catch (UnknownException e) {
				printException("subscribe failed with unknown", e, verbose);
			}
		} else if (line.hasOption("u")) { // unsubscribe
			try {
				Scalaris sc = new Scalaris();
				System.out.println("unsubscribe("
						+ line.getOptionValues("unsubscribe")[0] + ", "
						+ line.getOptionValues("unsubscribe")[1] + ")");
				sc.unsubscribe(line.getOptionValues("unsubscribe")[0], line
						.getOptionValues("unsubscribe")[1]);
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
			try {
				Scalaris sc = new Scalaris();
				System.out.println("getSubscribers("
						+ line.getOptionValues("getsubscribers")[0]
						+ ") == "
						+ sc.getSubscribers(line
								.getOptionValues("getsubscribers")[0]));
			} catch (ConnectionException e) {
				printException("getSubscribers failed with connection error", e, verbose);
//			} catch (TimeoutException e) {
//				printException("getSubscribers failed with timeout", e, verbose);
			} catch (UnknownException e) {
				printException("getSubscribers failed with unknown error", e, verbose);
			}
		} else if (line.hasOption("d")) { // delete
			try {
				Scalaris sc = new Scalaris();
				String key = line.getOptionValues("delete")[0];
				int timeout;
				try {
					timeout = Integer.parseInt(line.getOptionValues("delete")[1]);
				} catch (Exception e) {
					timeout = 2000;
				}
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

		options.addOption(new Option("h", "help", false, "print this message"));
		
		options.addOption(new Option("v", "verbose", false, "print verbose information, e.g. the properties read"));

		Option read = new Option("r", "read", true, "read an item");
		read.setArgName("key");
		read.setArgs(1);
		group.addOption(read);

		Option write = new Option("w", "write", true, "write an item");
		write.setArgName("key> <value");
		write.setArgs(2);
		group.addOption(write);

		Option publish = new Option("p", "publish", true, "publish a new message for the given topic");
		publish.setArgName("topic> <message");
		publish.setArgs(2);
		group.addOption(publish);

		Option subscribe = new Option("s", "subscribe", true, "subscribe to a topic");
		subscribe.setArgName("topic> <url");
		subscribe.setArgs(2);
		group.addOption(subscribe);
		
		Option unsubscribe = new Option("u", "unsubscribe", true, "unsubscribe from a topic");
		unsubscribe.setArgName("topic> <url");
		unsubscribe.setArgs(2);
		group.addOption(unsubscribe);

		Option getSubscribers = new Option("g", "getsubscribers", true, "get subscribers of a topic");
		getSubscribers.setArgName("topic");
		getSubscribers.setArgs(1);
		group.addOption(getSubscribers);

		Option delete = new Option("d", "delete", true,
				"delete an item (default timeout: 2000ms)\n" +
				"WARNING: This function can lead to inconsistent data (e.g. " +
				"deleted items can re-appear). Also when re-creating an item " +
				"the version before the delete can re-appear.");
		delete.setArgName("key> <[timeout]");
		delete.setArgs(2);	
		group.addOption(delete);

		options.addOption(new Option("b", "minibench", false, "run mini benchmark"));

		options.addOption(new Option("lh", "localhost", false, "gets the local host's name as known to Java (for debugging purposes)"));

		options.addOptionGroup(group);

		return options;
	}
	
	/**
	 * Prints the given exception with the given description.
	 * 
	 * @param description  will be prepended to the error message
	 * @param e            the exception to print
	 * @param verbose      specifies whether to include the stack trace or not
	 */
	private static void printException(String description, Exception e, boolean verbose) {
		System.err.print(description + ": ");
		if (verbose) {
			System.err.println();
			e.printStackTrace();
		} else {
			System.err.println(e.getMessage());
		}
	}
}
