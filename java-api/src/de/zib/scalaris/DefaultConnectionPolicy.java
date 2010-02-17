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
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Implements a {@link ConnectionPolicy} by choosing nodes randomly.
 * 
 * @author Nico Kruber, kruber@zib.de
 * 
 * @version 2.3
 * @since 2.3
 */
public class DefaultConnectionPolicy extends ConnectionPolicy {
	
	private class BadNodesComparator implements Comparator<PeerNode> {
		public int compare(PeerNode o1, PeerNode o2) {
			// Returns a negative integer, zero, or a positive integer as the
			// first argument is less than, equal to, or greater than the
			// second.
			Long o1Time = new Long(o1.getLastFailedConnect().getTime());
			Long o2Time = new Long(o2.getLastFailedConnect().getTime());
			return o1Time.compareTo(o2Time);
		}
		
	}
	
	// we could use synchronised lists and sets as provided by
	// Collections.synchronizedList and Collections.synchronizedSortedSet
	// but those two depend on each other and we thus need synchronised methods
	// which make synchronisations here obsolete
	protected List<PeerNode> goodNodes = new ArrayList<PeerNode>();
	protected SortedSet<PeerNode> badNodes = new TreeSet<PeerNode>(new BadNodesComparator());

	private Random random = new Random();
	private int maxRetries = 3;

	/**
	 * Creates a new connection policy working with the given remote node.
	 * 
	 * Provided for convenience.
	 * 
	 * @param remoteNode the (only) available remote node
	 */
	public DefaultConnectionPolicy(PeerNode remoteNode) {
		super(remoteNode);
		synchronized (remoteNode) {
			if (remoteNode.getFailureCount() == 0) {
				goodNodes.add(remoteNode);
			} else {
				badNodes.add(remoteNode);
			}
		}
	}

	/**
	 * Creates a new connection policy with the given remote nodes.
	 * 
	 * @param availableRemoteNodes the remote nodes available for connections
	 */
	public DefaultConnectionPolicy(List<PeerNode> availableRemoteNodes) {
		super(availableRemoteNodes);
		synchronized (availableRemoteNodes) {
			for (PeerNode remoteNode : availableRemoteNodes) {
				synchronized (remoteNode) {
					if (remoteNode.getFailureCount() == 0) {
						goodNodes.add(remoteNode);
					} else {
						badNodes.add(remoteNode);
					}
				}
			}
		}
	}
	
	public synchronized void availableNodeAdded(PeerNode newNode) {
		synchronized (newNode) {
			if (newNode.getFailureCount() == 0) {
				goodNodes.add(newNode);
			} else {
				badNodes.add(newNode);
			}
		}
	}
	
	public synchronized void availableNodeRemoved(PeerNode removedNode) {
		goodNodes.remove(removedNode);
		badNodes.remove(removedNode);
	}

	public synchronized void availableNodesReset() {
		goodNodes.clear();
		badNodes.clear();
	}
	
	public synchronized void nodeFailed(PeerNode node) {
		synchronized (node) {
			// update fail time before adding the node to the SortedSet!
			node.setLastFailedConnect();
			if (node.getFailureCount() == 0) {
				// a node that has not failed before must be in goodNodes
				// -> move it to badNodes
				goodNodes.remove(node);
				badNodes.add(node);
			}
		}
	}
	
	public synchronized void nodeConnectSuccess(PeerNode node) {
		synchronized (node) {
			node.setLastConnectSuccess();
			if (node.getFailureCount() > 0) {
				// a previously failed node must be in badNodes
				// -> move it back to goodNodes
				node.resetFailureCount();
				badNodes.remove(node);
				goodNodes.add(node);
			}
		}
	}
	
	private final synchronized PeerNode getRandomGoodNode() {
		if (goodNodes.size() == 1) {
			return goodNodes.get(0);
		} else {
			return goodNodes.get(random.nextInt(goodNodes.size()));
		}
	}

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
			throw e;
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
}
