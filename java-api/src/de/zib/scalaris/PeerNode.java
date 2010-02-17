/**
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
import java.util.Date;
import java.util.List;

import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.BufferUtils;
import org.apache.commons.collections.buffer.CircularFifoBuffer;

import com.ericsson.otp.erlang.OtpPeer;

/**
 * Wrapper class to the {@link OtpPeer} class, adding some additional
 * information.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class PeerNode {
	/**
	 * The node this object wraps.
	 */
	private OtpPeer node;
	/**
	 * A (circular) buffer of dates of failed connections (attempts as well as
	 * disrupted connections).
	 */
	private Buffer failedConnections = BufferUtils
			.synchronizedBuffer(new CircularFifoBuffer(10));
	/**
	 * Date of the last successful connection attempt.
	 */
	private Date connectionSuccess = null;

	/**
	 * Creates a new object using the given node.
	 * 
	 * @param node
	 *            the node to wrap
	 */
	public PeerNode(OtpPeer node) {
		super();
		this.node = node;
	}

	/**
	 * Creates a new object using the given node.
	 * 
	 * Provided for convenience.
	 * 
	 * @param node
	 *            the name of the node to wrap
	 */
	public PeerNode(String node) {
		super();
		this.node = new OtpPeer(node);
	}

	/**
	 * Gets the OTP node that is being wrapped.
	 * 
	 * @return the node
	 */
	public OtpPeer getNode() {
		return node;
	}

	/**
	 * Adds a failed connection (attempt or broken connection) with the current
	 * date and time.
	 * 
	 * Provided for convenience.
	 * 
	 * @see PeerNode#addFailedConnection(Date)
	 */
	public void addFailedConnection() {
		addFailedConnection(new Date());
	}

	/**
	 * Adds a failed connection (attempt or broken connection) with the given
	 * date and time.
	 * 
	 * Beware: failed connections will be stored in the order they were added,
	 * not in the order of their dates!
	 * 
	 * @param date
	 *            the date of the failure
	 * 
	 * @see PeerNode#addFailedConnection()
	 */
	@SuppressWarnings("unchecked")
	public void addFailedConnection(Date date) {
		failedConnections.add(date);
	}

	/**
	 * Gets the failed connections as a list.
	 * 
	 * Internally the failed connections are stored in a circular buffer. This
	 * method converts this buffer to a list and therefore takes some resources.
	 * You should cache the result.
	 * 
	 * @return a list of dates and times of connection failures
	 */
	@SuppressWarnings("unchecked")
	public List<Date> getFailedConnections() {
		return new ArrayList<Date>(failedConnections);
	}
	
	/**
	 * Iterates through all stored connection failures and returns the most
	 * recently failed one (according to the date, not the insert order).
	 * 
	 * @return the date of the most recently failed connection
	 */
	public synchronized Date getLastFailedConnection() {
		if (failedConnections.isEmpty()) {
			return null;
		} else {
			Date lastFailed = new Date(0);
			for (Object obj : failedConnections) {
				Date d = (Date) obj;
				if (d.getTime() > lastFailed.getTime()) {
					lastFailed = d;
				}
			}
			return lastFailed;
		}
	}

	/**
	 * Gets the number of failed connections.
	 * 
	 * This is faster than getting a list of failed connections and calling
	 * {@link List#size()} since this does not require a conversion.
	 * 
	 * @return the number of failed connections (dates and times)
	 */
	public int getFailedConnectionsCount() {
		return failedConnections.size();
	}
	
	/**
	 * Resets the failed connections statistics.
	 */
	public void resetFailedConnections() {
		failedConnections.clear();
	}
	
	/**
	 * Gets the date of the last successful connection.
	 * 
	 * @return the last connection success
	 */
	public Date getConnectionSuccess() {
		return connectionSuccess;
	}

	/**
	 * Adds a connection success with the current date.
	 */
	public void setConnectionSuccess() {
		this.connectionSuccess = new Date();
	}
}
