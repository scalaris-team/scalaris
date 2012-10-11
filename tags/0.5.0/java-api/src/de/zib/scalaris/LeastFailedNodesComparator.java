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
 * @version 3.16
 * @since 3.16
 */
class LeastFailedNodesComparator implements Comparator<PeerNode>, java.io.Serializable {
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
        return LeastFailedNodesComparator.compareS(o1, o2);
    }

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
    public static int compareS(final PeerNode o1, final PeerNode o2) {
        if (o1 == o2) {
            return 0;
        }

        final Integer o1FailureCount = o1.getFailureCount();
        final Integer o2FailureCount = o2.getFailureCount();
        final int compByFailureCount = o1FailureCount.compareTo(o2FailureCount);

        if (compByFailureCount != 0) {
            return compByFailureCount;
        }

        // two different nodes have the same fail counts
        // -> make order dependent on their last successful connection date
        final Date d1 = o1.getLastConnectSuccess();
        final Date d2 = o2.getLastConnectSuccess();
        final Long o1Time = ((d1 == null) ? 0 : d1.getTime());
        final Long o2Time = ((d2 == null) ? 0 : d2.getTime());
        // most recently connection success first:
        final int compByTime = o2Time.compareTo(o1Time);

        if (compByTime != 0) {
            return compByTime;
        }

        // -> make order dependent on their hash code:
        final int h1 = o1.hashCode();
        final int h2 = o2.hashCode();
        if (h1 < h2) {
            return -1;
        } else if (h1 > h2){
            return 1;
        } else {
            // two different nodes have equal fail dates and hash codes
            // -> compare their names (last resort)
            final int compByName = o1.getNode().node().compareTo(o2.getNode().node());
            if (compByName != 0) {
                return compByName;
            } else {
                throw new RuntimeException("Cannot compare " + o1 + " with "
                        + o2 + ".");
            }
        }
    }
}