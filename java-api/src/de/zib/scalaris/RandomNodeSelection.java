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

import java.util.List;
import java.util.Random;

import com.ericsson.otp.erlang.OtpPeer;

/**
 * Implements a {@link NodeSelectionPolicy} by choosing nodes randomly.
 * 
 * @author Nico Kruber, kruber@zib.de
 * 
 * @version 2.3
 * @since 2.3
 */
public class RandomNodeSelection implements NodeSelectionPolicy {

	private Random random = new Random();

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.zib.scalaris.NodeSelectionPolicy#selectNode(java.util.List)
	 */
	// override tag does not seem to work with Java 1.5
//	@Override
	public OtpPeer selectNode(List<OtpPeer> nodes)
			throws UnsupportedOperationException {
		if (nodes.size() < 1) {
			throw new UnsupportedOperationException(
					"Can not choose a node from an empty list.");
		} else if (nodes.size() == 1) {
			return nodes.get(0);
		} else {
			return nodes.get(random.nextInt(nodes.size()));
		}
	}

}
