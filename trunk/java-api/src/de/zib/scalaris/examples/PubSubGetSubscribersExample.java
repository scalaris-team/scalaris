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

import java.util.Iterator;
import java.util.List;

import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangString;

import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.PubSub;
import de.zib.scalaris.UnknownException;

/**
 * Provides an example for using the <tt>getSubscribers</tt> methods of the
 * {@link PubSub} class.
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 2.5
 * @since 2.5
 */
public class PubSubGetSubscribersExample {
    /**
     * Prints a list of all subscribers to a given topic, provided on the
     * command line with the <tt>getSubscribers</tt> methods of
     * {@link PubSub}.<br />
     * If no topic is given, the default topic <tt>"topic"</tt> is used.
     * 
     * @param args
     *            command line arguments (first argument can be an optional
     *            topic to get the subscribers for)
     */
    public static void main(String[] args) {
        String topic;

        if (args.length != 1) {
            topic = "topic";
        } else {
            topic = args[0];
        }

        OtpErlangString otpTopic = new OtpErlangString(topic);

        OtpErlangList otpSubscribers;
        List<String> subscribers;

        System.out.println("Reading values with the class `PubSub`:");

        try {
            System.out.println("  creating object...");
            PubSub sc = new PubSub();
            System.out
                    .println("    `OtpErlangList getSubscribers(OtpErlangString)`...");
            otpSubscribers = (OtpErlangList) sc.getSubscribers(otpTopic).value();
            System.out.println("      getSubscribers(" + otpTopic.stringValue()
                    + ") == " + getSubscribers(otpSubscribers));
        } catch (ConnectionException e) {
            System.out.println("      getSubscribers(" + otpTopic.stringValue()
                    + ") failed: " + e.getMessage());
        } catch (UnknownException e) {
            System.out.println("    getSubscribers(" + otpTopic.stringValue()
                    + ") failed: " + e.getMessage());
        }

        try {
            System.out.println("  creating object...");
            PubSub sc = new PubSub();
            System.out
                    .println("    `List<String> getSubscribers(String)`...");
            subscribers = sc.getSubscribers(topic).stringListValue();
            System.out.println("      getSubscribers(" + topic + ") == "
                    + getSubscribers(subscribers));
        } catch (ConnectionException e) {
            System.out.println("      getSubscribers(" + topic + ") failed: "
                    + e.getMessage());
        } catch (UnknownException e) {
            System.out.println("    getSubscribers(" + topic + ") failed: "
                    + e.getMessage());
        }
    }

    /**
     * converts the list of strings to a comma-separated list of strings
     * 
     * @param subscribers
     *            the list of subscribers to convert
     * @return a comma-separated list of subscriber URLs
     */
    private static String getSubscribers(List<String> subscribers) {
        StringBuffer result = new StringBuffer();
        for (Iterator<String> iterator = subscribers.iterator(); iterator
                .hasNext();) {
            result.append(iterator.next());
            result.append(", ");
        }
        if (result.length() > 2) {
            return "[" + result.substring(0, result.length() - 3) + "]";
        } else {
            return "[" + result.toString() + "]";
        }
    }

    /**
     * converts the list of erlang strings to a comma-separated list of strings
     * 
     * @param subscribers
     *            the list of subscribers to convert
     * @return a comma-separated list of subscriber URLs
     */
    private static String getSubscribers(OtpErlangList subscribers) {
        StringBuffer result = new StringBuffer();
        for (int i = 0; i < subscribers.arity(); ++i) {
            result.append(((OtpErlangString) subscribers.elementAt(i))
                    .stringValue());
            result.append(", ");
        }
        if (result.length() > 2) {
            return "[" + result.substring(0, result.length() - 3) + "]";
        } else {
            return "[" + result.toString() + "]";
        }
    }
}
