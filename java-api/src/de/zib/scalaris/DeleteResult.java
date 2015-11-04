/**
 *
 */
package de.zib.scalaris;

import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangList;

/**
 * Stores the result of a delete operation.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.19
 * @since 2.2
 *
 * @see ReplicatedDHT#delete(String)
 */
public class DeleteResult {
    /**
     * Number of successfully deleted replicas.
     */
    public int ok = 0;
    /**
     * Skipped replicas because locks were set.
     */
    public int locks_set = 0;
    /**
     * Skipped replicas because they did not exist.
     */
    public int undef = 0;

    /**
     * Creates a delete state object by converting the result list returned from
     * erlang.
     *
     * @param list
     *            the list to convert
     * @throws UnknownException
     *             is thrown if an unknown reason was encountered
     */
    public DeleteResult(final OtpErlangList list) throws UnknownException {
        if (list != null) {
            for (int i = 0; i < list.arity(); ++i) {
                final OtpErlangAtom element = (OtpErlangAtom) list.elementAt(i);
                if (element.equals(new OtpErlangAtom("ok"))) {
                    ++ok;
                } else if (element.equals(new OtpErlangAtom("locks_set"))) {
                    ++locks_set;
                } else if (element.equals(new OtpErlangAtom("undef"))) {
                    ++undef;
                } else {
                    throw new UnknownException("Unknow reason: "
                            + element.atomValue() + " in " + list.toString());
                }
            }
        }
    }

    /**
     * Checks whether the delete operation has successfully deleted all replicas
     * (replicas which did not exist are counted as successfully deleted as
     * well). If not, the delete needs to be executed again (see
     * {@link ReplicatedDHT#delete(com.ericsson.otp.erlang.OtpErlangString, int)}.
     *
     * @param conn
     *            a connection to Scalaris to find out the current replication
     *            degree
     * @return whether all replicas were deleted or not
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     *
     * @since 3.19
     */
    public boolean hasDeletedAll(final Connection conn) throws ConnectionException {
        final RoutingTable rt = new RoutingTable(conn);
        final int r = rt.getReplicationFactor();

        return (ok + undef) == r;
    }
}
