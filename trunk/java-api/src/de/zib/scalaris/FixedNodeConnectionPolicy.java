package de.zib.scalaris;

/**
 * Implements a {@link ConnectionPolicy} which only supports a single node
 * and does not issue automatic re-tries.
 *
 * @author Nico Kruber, kruber@zib.de
 *
 * @version 3.10
 * @since 3.10
 *
 * @see DefaultConnectionPolicy
 */
public class FixedNodeConnectionPolicy extends ConnectionPolicy {
    /**
     * Creates a new connection policy working with the given remote node.
     *
     * @param remoteNode the (only) available remote node
     */
    public FixedNodeConnectionPolicy(final PeerNode remoteNode) {
        super(remoteNode);
    }
    /**
     * Creates a new connection policy working with the given remote node.
     *
     * @param remoteNode the (only) available remote node
     */
    public FixedNodeConnectionPolicy(final String remoteNode) {
        this(new PeerNode(remoteNode));
    }

    @Override
    public <E extends Exception> PeerNode selectNode(final int retry,
            final PeerNode failedNode, final E e) throws E, UnsupportedOperationException {
        // no re-try, i.e. re-throw any previous exception:
        if (e != null) {
            throw e;
        }
        return availableRemoteNodes.get(0);
    }
}