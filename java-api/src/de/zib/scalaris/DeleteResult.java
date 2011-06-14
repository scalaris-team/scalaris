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
 * @version 2.6
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
}
