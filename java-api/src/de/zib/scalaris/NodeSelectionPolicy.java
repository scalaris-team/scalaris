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
package de.zib.scalaris;

import java.util.List;

/**
 * Defines a policy on how to select a node to connect with from a set of
 * possible nodes.
 * 
 * @author Nico Kruber, kruber@zib.de
 * 
 * @see ConnectionFactory
 * 
 * @version 2.3
 * @since 2.3
 */
public interface NodeSelectionPolicy {
	/**
	 * Selects the node to connect with from the given set of nodes.
	 * 
	 * @param nodes
	 *            a list of available nodes
	 * 
	 * @return the node to use for the connection
	 * 
	 * @throws UnsupportedOperationException
	 *             is thrown if the operation can not be performed, e.g. the
	 *             list is empty
	 * 
	 * @see ConnectionFactory
	 */
	public PeerNode selectNode(List<PeerNode> nodes)
			throws UnsupportedOperationException;
}
