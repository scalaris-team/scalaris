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
package de.zib.chordsharp;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Class to test basic functionality of the package.
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 1.4
 */
public class Main {

	/**
	 * Queries the command line options for an action to perform.
	 * 
	 * <pre>
	 * {@code
	 * > java -jar chordsharp.jar -help
	 * usage: chordsharp
	 *  -getsubscribers <topic>   get subscribers of a topic
	 *  -help                     print this message
	 *  -publish <params>         publish a new message for a topic: <topic> <message>
	 *  -read <key>               read an item
	 *  -subscribe <params>       subscribe to a topic: <topic> <url>
	 *  -unsubscribe <params>     unsubscribe from a topic: <topic> <url>
	 *  -write <params>           write an item: <key> <value>
	 * }
	 * </pre>
	 * 
	 * @param args command line arguments
	 */
	public static void main(String[] args) {
		CommandLineParser parser = new GnuParser();
		CommandLine line = null;
		try {
			line = parser.parse(getOptions(), args);
		} catch (ParseException e) {
			System.err.println("Parsing failed. Reason: " + e.getMessage());
			System.exit(0);
		}

		if (line.hasOption("help")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("chordsharp", getOptions());
			System.exit(0);
		}

		if (line.hasOption("read")) {
			try {
				System.out.println("read(" + line.getOptionValue("read")
						+ ") == "
						+ ChordSharp.read(line.getOptionValue("read")));
			} catch (ConnectionException e) {
				System.err.println("read failed: " + e.getMessage());
			} catch (TimeoutException e) {
				System.err.println("read failed with timeout: "
						+ e.getMessage());
			} catch (NotFoundException e) {
				System.err.println("read failed with not found: "
						+ e.getMessage());
			} catch (UnknownException e) {
				System.err.println("read failed with unknown: "
						+ e.getMessage());
			}
		}
		if (line.hasOption("write")) {
			try {
				System.out.println("write(" + line.getOptionValues("write")[0]
						+ ", " + line.getOptionValues("write")[1] + ")");
				ChordSharp.write(line.getOptionValues("write")[0], line
						.getOptionValues("write")[1]);
			} catch (ConnectionException e) {
				System.err.println("write failed with connection error: "
						+ e.getMessage());
			} catch (TimeoutException e) {
				System.err.println("write failed with timeout: "
						+ e.getMessage());
			} catch (UnknownException e) {
				System.err.println("write failed with unknown: "
						+ e.getMessage());
			}
		}
		if (line.hasOption("publish")) {
			try {
				System.out.println("publish("
						+ line.getOptionValues("publish")[0] + ", "
						+ line.getOptionValues("publish")[1] + ")");
				ChordSharp.publish(line.getOptionValues("publish")[0], line
						.getOptionValues("publish")[1]);
			} catch (ConnectionException e) {
				System.err.println("publish failed with connection error: "
						+ e.getMessage());
//			} catch (TimeoutException e) {
//				System.err.println("publish failed with timeout: "
//						+ e.getMessage());
//			} catch (UnknownException e) {
//				System.err.println("publish failed with unknown: "
//						+ e.getMessage());
			}
		}
		if (line.hasOption("subscribe")) {
			try {
				System.out.println("subscribe("
						+ line.getOptionValues("subscribe")[0] + ", "
						+ line.getOptionValues("subscribe")[1] + ")");
				ChordSharp.subscribe(line.getOptionValues("subscribe")[0], line
						.getOptionValues("subscribe")[1]);
			} catch (ConnectionException e) {
				System.err.println("subscribe failed with connection error: "
						+ e.getMessage());
			} catch (TimeoutException e) {
				System.err.println("subscribe failed with timeout: "
						+ e.getMessage());
			} catch (UnknownException e) {
				System.err.println("subscribe failed with unknown: "
						+ e.getMessage());
			}
		}
		if (line.hasOption("unsubscribe")) {
			try {
				System.out.println("unsubscribe("
						+ line.getOptionValues("unsubscribe")[0] + ", "
						+ line.getOptionValues("unsubscribe")[1] + ")");
				ChordSharp.unsubscribe(line.getOptionValues("unsubscribe")[0], line
						.getOptionValues("unsubscribe")[1]);
			} catch (ConnectionException e) {
				System.err.println("unsubscribe failed with connection error: "
						+ e.getMessage());
			} catch (TimeoutException e) {
				System.err.println("unsubscribe failed with timeout: "
						+ e.getMessage());
			} catch (NotFoundException e) {
				System.err.println("unsubscribe failed with not found: "
						+ e.getMessage());
			} catch (UnknownException e) {
				System.err.println("unsubscribe failed with unknown: "
						+ e.getMessage());
			}
		}
		if (line.hasOption("getsubscribers")) {
			try {
				System.out.println("getSubscribers("
						+ line.getOptionValues("getsubscribers")[0]
						+ ") == "
						+ ChordSharp.getSubscribers(line
								.getOptionValues("getsubscribers")[0]));
			} catch (ConnectionException e) {
				System.err
						.println("getSubscribers failed with connection error: "
								+ e.getMessage());
//			} catch (TimeoutException e) {
//				System.err.println("getSubscribers failed with timeout: "
//						+ e.getMessage());
			} catch (UnknownException e) {
				System.err.println("getSubscribers failed with unknown error: "
						+ e.getMessage());
			}
		}
	}

	/**
	 * creates the options the command line should understand
	 * 
	 * @return the options the program understands
	 */
	static Options getOptions() {
		Options options = new Options();
		OptionGroup group = new OptionGroup();

		options.addOption(new Option("help", "print this message"));

		Option read = OptionBuilder.create("read");
		read.setArgName("key");
		read.setArgs(1);
		read.setDescription("read an item");
//		Option read = OptionBuilder.withArgName("key").hasArg()
//				.withDescription("read an item").create("read");
		group.addOption(read);

		Option write = OptionBuilder.create("write");
		write.setArgName("params");
		write.setArgs(2);
		write.setDescription("write an item: <key> <value>");
//		Option write = OptionBuilder.withArgName("params").hasArgs(2)
//				.withDescription("write an item: <key> <value>")
//				.create("write");
		group.addOption(write);

		Option publish = OptionBuilder.create("publish");
		publish.setArgName("params");
		publish.setArgs(2);
		publish.setDescription("publish a new message for a topic: <topic> <message>");
//		Option publish = OptionBuilder.withArgName("params").hasArgs(2)
//				.withDescription(
//						"publish a new message for a topic: <topic> <message>")
//				.create("publish");
		group.addOption(publish);

		Option subscribe = OptionBuilder.create("subscribe");
		subscribe.setArgName("params");
		subscribe.setArgs(2);
		subscribe.setDescription("subscribe to a topic: <topic> <url>");
//		Option subscribe = OptionBuilder.withArgName("params").hasArgs(2)
//				.withDescription("subscribe to a topic: <topic> <url>").create(
//						"subscribe");
		group.addOption(subscribe);
		
		Option unsubscribe = OptionBuilder.create("unsubscribe");
		unsubscribe.setArgName("params");
		unsubscribe.setArgs(2);
		unsubscribe.setDescription("unsubscribe from a topic: <topic> <url>");
//		Option subscribe = OptionBuilder.withArgName("params").hasArgs(2)
//				.withDescription("unsubscribe from a topic: <topic> <url>").create(
//						"unsubscribe");
		group.addOption(unsubscribe);

		Option getSubscribers = OptionBuilder.create("getsubscribers");
		getSubscribers.setArgName("topic");
		getSubscribers.setArgs(1);
		getSubscribers.setDescription("get subscribers of a topic");
//		Option getSubscribers = OptionBuilder.withArgName("topic").hasArgs(1)
//				.withDescription("get subscribers of a topic").create(
//						"getsubscribers");
		group.addOption(getSubscribers);

		options.addOptionGroup(group);

		return options;
	}

}
