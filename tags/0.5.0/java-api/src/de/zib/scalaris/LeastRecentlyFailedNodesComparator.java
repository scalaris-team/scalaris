/**
 *  Copyright 2007-2012 Zuse Institute Berlin
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

import java.util.Comparator;
import java.util.Date;

/**
 * Defines the order of the {@link PeerNode} objects with least recently
 * failed nodes being first.
 *
 * @author Nico Kruber, kruber@zib.de
 *
 * @version 2.3
 * @since 2.3
 */
class LeastRecentlyFailedNodesComparator implements Comparator<PeerNode>, java.io.Serializable {
    /**
     * ID for serialisation purposes.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Defines a order of nodes with failed connections (least recently failed
     * nodes first).
     *
     * Returns a negative integer, zero, or a positive integer if the first
     * argument is less than, equal to, or greater than the second.
     *
     * Warning: This method is unsynchronised! Special care needs to be taken
     * using it on collections of nodes, i.e. the node list in
     * {@link DefaultConnectionPolicy}!
     *
     * @param o1
     *            the first node
     * @param o2
     *            the second node
     *
     * @return a negative integer, zero, or a positive integer if the first
     *         argument is less than, equal to, or greater than the second
     */
    public int compare(final PeerNode o1, final PeerNode o2) {
        if (o1 == o2) {
            return 0;
        }

        final Date d1 = o1.getLastFailedConnect();
        final Date d2 = o2.getLastFailedConnect();
        final Long o1Time = ((d1 == null) ? 0 : d1.getTime());
        final Long o2Time = ((d2 == null) ? 0 : d2.getTime());
        final int compByTime = o1Time.compareTo(o2Time);

        if (compByTime != 0) {
            return compByTime;
        }

        return LeastFailedNodesComparator.compareS(o1, o2);
    }
}