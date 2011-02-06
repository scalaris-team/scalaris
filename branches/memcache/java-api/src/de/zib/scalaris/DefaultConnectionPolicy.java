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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import com.ericsson.otp.erlang.OtpAuthException;

/**
 * Implements a {@link ConnectionPolicy} by choosing nodes randomly.
 * 
 * Sorts nodes into two lists:
 * <ul>
 * <li>good nodes</li>
 * <li>bad nodes</li>
 * </ul>
 * Nodes that failed during a connection or in an attempt to connect will be
 * transferred to the bad nodes list {@link #badNodes}. When a connection
 * attempt to a node is successful, it will be removed from this list and put
 * into the good nodes list {@link #goodNodes} and the node's connection failure
 * statistics will be reset using {@link PeerNode#resetFailureCount()}.
 * 
 * Whenever a node is being selected for a new connection (or reconnect), it
 * will select one randomly from the {@link #goodNodes} list. If this list is
 * empty, it will select the least recently failed node from {@link #badNodes}.
 * At most {@link #maxRetries} retries are attempted per operation (see
 * {@link Connection#connect()},
 * {@link Connection#doRPC(String, String, com.ericsson.otp.erlang.OtpErlangList)}
 * and
 * {@link Connection#doRPC(String, String, com.ericsson.otp.erlang.OtpErlangObject[])}
 * ) - the number of the current attempt will not be cached in this class. Set
 * the maximal number of retries using {@link #setMaxRetries(int)}.
 * 
 * Attention: All member's functions are synchronised as there can be a single
 * connection policy object used by many threads and the access to the
 * {@link #goodNodes} and {@link #badNodes} members are logically linked
 * together and operations on both need to be performed atomically. Additionally
 * access to {@link PeerNode} objects are synchronised on themselves. It is
 * therefore important not to use any of this classes methods in blocks that
 * synchronise on any node object. Otherwise deadlocks might occur!!
 * 
 * @author Nico Kruber, kruber@zib.de
 * 
 * @version 2.3
 * @since 2.3
 */
public class DefaultConnectionPolicy extends ConnectionPolicy {

	/**
	 * Helper class that defines the order of the {@link PeerNode} objects in
	 * the {@link DefaultConnectionPolicy#badNodes} member: least recently
	 * failed = first.
	 * 
	 * @author Nico Kruber, kruber@zib.de
	 * 
	 * @version 2.3
	 * @since 2.3
	 */
	private static class BadNodesComparator implements Comparator<PeerNode>, java.io.Serializable {
		/**
		 * ID for serialisation purposes.
		 */
		private static final long serialVersionUID = 1L;

		/**
		 * Defines a order of nodes with failed connections (least recently
		 * failed nodes first).
		 * 
		 * Returns a negative integer, zero, or a positive integer if the first
		 * argument is less than, equal to, or greater than the second.
		 * 
		 * Warning: This method is unsynchronised and should therefore only be
		 * used by calls from functions inside {@link DefaultConnectionPolicy}
		 * which are synchronised!
		 * 
		 * @param o1
		 *            the first node
		 * @param o2
		 *            the second node
		 * 
		 * @return a negative integer, zero, or a positive integer if the first
		 *         argument is less than, equal to, or greater than the second
		 */
		public int compare(PeerNode o1, PeerNode o2) {
			// Returns a negative integer, zero, or a positive integer as the
			// first argument is less than, equal to, or greater than the
			// second.
			if (o1 == o2) {
				return 0;
			}
			
			Date d1 = o1.getLastFailedConnect();
			Date d2 = o2.getLastFailedConnect();
			Long o1Time = ((d1 == null) ? 0 : d1.getTime());
			Long o2Time = ((d2 == null) ? 0 : d2.getTime());
			int compByTime = o1Time.compareTo(o2Time);
			
			if (compByTime == 0) {
				// two different nodes have the same fail dates
				// -> make order dependent on their hash code:
				int h1 = o1.hashCode();
				int h2 = o2.hashCode();
				if (h1 < h2) {
					return -1;
				} else if (h1 > h2){
					return 1;
				} else {
					// two different nodes have equal fail dates and hash codes
					// -> compare their names (last resort)
					int compByName = o1.getNode().node().compareTo(o2.getNode().node());
					if (compByName != 0) {
						return compByName;
					} else {
						throw new ClassCastException(
								"Cannot compare "
										+ o1
										+ " with "
										+ o2
										+ ": they share the same fail time, hash code, node name and cookie.");
					}
				}
			} else {
				return compByTime;
			}
		}
	}
	
	// we could use synchronised lists and sets as provided by
	// Collections.synchronizedList and Collections.synchronizedSortedSet
	// but those two depend on each other and we thus need synchronised methods
	// which make synchronisations here obsolete
	/**
	 * A list of good nodes (nodes which recently successfully connected).
	 */
	protected List<PeerNode> goodNodes = new ArrayList<PeerNode>();
	/**
	 * Bad nodes (nodes which recently failed) in the order of their failed
	 * date.
	 */
	protected SortedSet<PeerNode> badNodes = new TreeSet<PeerNode>(new BadNodesComparator());

	/**
	 * Random number generator for selecting random nodes in the
	 * {@link #goodNodes} list.
	 */
	private Random random = new Random();
	
	/**
	 * The maximal number of connection retries.
	 */
	private int maxRetries = 3;

	/**
	 * Creates a new connection policy working with the given remote node.
	 * 
	 * Provided for convenience.
	 * 
	 * Attention: This method also synchronises on the node.
	 * 
	 * @param remoteNode the (only) available remote node
	 */
	public DefaultConnectionPolicy(PeerNode remoteNode) {
		super(remoteNode);
		availableNodeAdded(remoteNode);
	}

	/**
	 * Creates a new connection policy with the given remote nodes.
	 * 
	 * Attention: This method synchronises on {@code availableRemoteNodes}.
	 * 
	 * Any time this list is changed, the according methods in this class should
	 * be called, i.e. {@link #availableNodeAdded(PeerNode)},
	 * {@link #availableNodeRemoved(PeerNode)}, {@link #availableNodesReset()}
	 * to update the good and bad nodes lists.
	 * 
	 * @param availableRemoteNodes
	 *            the remote nodes available for connections
	 */
	public DefaultConnectionPolicy(List<PeerNode> availableRemoteNodes) {
		super(availableRemoteNodes);
		synchronized (availableRemoteNodes) {
			for (PeerNode remoteNode : availableRemoteNodes) {
				availableNodeAdded(remoteNode);
			}
		}
	}
	
	/**
	 * Adds the given node to the {@link #goodNodes} list if it has no failures,
	 * otherwise it will be added to {@link #badNodes}.
	 * 
	 * Attention: This method also synchronises on the node.
	 * 
	 * @param newNode the new node
	 */
	public synchronized void availableNodeAdded(PeerNode newNode) {
		synchronized (newNode) {
			if (newNode.getFailureCount() == 0) {
				goodNodes.add(newNode);
			} else {
				badNodes.add(newNode);
			}
		}
	}
	
	/**
	 * Removes the node from the {@link #goodNodes} and {@link #badNodes} lists.
	 * 
	 * @param removedNode the removed node
	 */
	public synchronized void availableNodeRemoved(PeerNode removedNode) {
		goodNodes.remove(removedNode);
		badNodes.remove(removedNode);
	}
	
	/**
	 * Resets the {@link #goodNodes} and {@link #badNodes} members as the list
	 * of available nodes has been reset.
	 */
	public synchronized void availableNodesReset() {
		goodNodes.clear();
		badNodes.clear();
	}
	
	/**
	 * Sets the given node's last failed connect time stamp and moves it to the
	 * {@link #badNodes} list.
	 * 
	 * Attention: This method also synchronises on the node.
	 * 
	 * @param node the failed node
	 */
	public synchronized void nodeFailed(PeerNode node) {
		synchronized (node) {
			// remove the node from the badNodes if it is in there (will be
			// reinserted at a new point)
			badNodes.remove(node);
			// update fail time before adding the node to the SortedSet!
			node.setLastFailedConnect();
			if (node.getFailureCount() == 1) {
				// a node that has not failed before must be in goodNodes
				// -> move it to badNodes
				goodNodes.remove(node);
			}
			badNodes.add(node);
		}
	}
	
	/**
	 * Acts upon a failure reset of the given node.
	 * 
	 * Resets the node's last failure state.
	 * 
	 * @param node the node
	 */
	public void nodeFailReset(PeerNode node) {
		synchronized (node) {
			if (node.getFailureCount() > 0) {
				// a previously failed node must be in badNodes
				// -> move it back to goodNodes
				badNodes.remove(node);
				node.resetFailureCount();
				goodNodes.add(node);
			}
		}
	}

	/**
	 * Sets the node's last successful connect time stamp, resets its failure
	 * statistics and moves it to the {@link #goodNodes} list.
	 * 
	 * Attention: This method also synchronises on the node.
	 * 
	 * @param node the node
	 */
	public synchronized void nodeConnectSuccess(PeerNode node) {
		synchronized (node) {
			node.setLastConnectSuccess();
			if (node.getFailureCount() > 0) {
				// a previously failed node must be in badNodes
				// -> move it back to goodNodes
				badNodes.remove(node);
				node.resetFailureCount();
				goodNodes.add(node);
			}
		}
	}
	
	/**
	 * Returns a random node from the list of good nodes
	 * 
	 * @return a random good node
	 */
	private final synchronized PeerNode getRandomGoodNode() {
		if (goodNodes.size() == 1) {
			return goodNodes.get(0);
		} else {
			return goodNodes.get(random.nextInt(goodNodes.size()));
		}
	}

	/**
	 * Selects the node to (re-)connect with until the maximal number of
	 * {@link #maxRetries} has been reached.
	 * 
	 * Throws an exception if {@code retry > maxRetries} and thus stops further
	 * node connection attempts. Otherwise chooses a random good node or (if
	 * there are no good nodes) the least recently failed bad node. 
	 * 
	 * @param <E>
	 *            the type of the exception that came from the failed connection
	 *            and may be re-thrown
	 * 
	 * @param retry
	 *            the n'th retry (initial connect = 0, 1st reconnect = 1,...)
	 * @param failedNode
	 *            the node from the previous connection attempt or {@code null}
	 * @param e
	 *            the exception that came back from the previous connection
	 *            attempt or {@code null}
	 * 
	 * @return the new node to connect with
	 * 
	 * @throws E
	 *             if thrown, automatic re-connection attempts will stop
	 * @throws UnsupportedOperationException
	 *             is thrown if the operation can not be performed, e.g. the
	 *             list is empty
	 * 
	 * @see Connection#connect()
	 * @see Connection#doRPC(String, String,
	 *      com.ericsson.otp.erlang.OtpErlangList)
	 * @see Connection#doRPC(String, String,
	 *      com.ericsson.otp.erlang.OtpErlangObject[])
	 */
	@SuppressWarnings("unchecked")
	public synchronized <E extends Exception> PeerNode selectNode(int retry,
			PeerNode failedNode, E e) throws E {
		if (retry <= maxRetries) {
			if ((goodNodes.size() + badNodes.size()) < 1) {
				throw new UnsupportedOperationException(
						"Can not choose a node from an empty list.");
			} else if (goodNodes.size() > 0) {
				return getRandomGoodNode();
			} else {
				return badNodes.first();
			}
		} else {
			String newMessage = e.getMessage() + ", bad nodes: " + badNodes.toString() + ", good nodes: " + goodNodes.toString() + ", retries: " + (retry - 1);
			if (e instanceof OtpAuthException) {
				OtpAuthException e1 = new OtpAuthException(newMessage);
				e1.setStackTrace(e.getStackTrace());
				throw (E) e1;
			} else if (e instanceof IOException) {
				IOException e1 = new IOException(newMessage);
				e1.setStackTrace(e.getStackTrace());
				throw (E) e1;
			} else {
				throw e;
			}
		}
	}

	
	/**
	 * Sets the maximal number of automatic connection retries.
	 * 
	 * @return the maxRetries
	 */
	public int getMaxRetries() {
		return maxRetries;
	}

	/**
	 * Gets the maximal number of automatic connection retries.
	 * 
	 * @param maxRetries the maxRetries to set
	 */
	public void setMaxRetries(int maxRetries) {
		this.maxRetries = maxRetries;
	}

	/**
	 * Gets a copy of the list of good nodes (contains references to the
	 * {@link PeerNode} objects).
	 * 
	 * @return the list of good nodes
	 */
	public synchronized List<PeerNode> getGoodNodes() {
		return new ArrayList<PeerNode>(goodNodes);
	}

	/**
	 * Gets a copy of the list of good nodes (contains references to the
	 * {@link PeerNode} objects).
	 * 
	 * @return the list of good nodes
	 */
	public synchronized List<PeerNode> getBadNodes() {
		ArrayList<PeerNode> result = new ArrayList<PeerNode>(badNodes.size());
		for (PeerNode p : badNodes) {
			result.add(p);
		}
		return result;
	}
}
