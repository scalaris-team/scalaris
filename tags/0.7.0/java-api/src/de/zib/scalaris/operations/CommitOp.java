package de.zib.scalaris.operations;

import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

import de.zib.scalaris.AbortException;
import de.zib.scalaris.CommonErlangObjects;
import de.zib.scalaris.UnknownException;

/**
 * An operation committing a transaction.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.18
 * @since 3.18
 */
public class CommitOp implements TransactionOperation {
    protected OtpErlangObject resultRaw = null;
    protected boolean resultCompressed = false;
    /**
     * Constructor
     */
    public CommitOp() {
    }

    public OtpErlangObject getErlang(final boolean compressed) {
        return CommonErlangObjects.commitTupleAtom;
    }

    public OtpErlangString getKey() {
        return null;
    }

    public void setResult(final OtpErlangObject resultRaw, final boolean compressed) {
        this.resultRaw = resultRaw;
        this.resultCompressed = compressed;
    }

    public OtpErlangObject getResult() {
        return this.resultRaw;
    }

    public boolean getResultCompressed() {
        return this.resultCompressed;
    }

    public Object processResult() throws AbortException, UnknownException {
        processResult_commit(resultRaw, resultCompressed);
        return null;
    }

    @Override
    public String toString() {
        return "commit()";
    }

    /**
     * Processes the <tt>received_raw</tt> term from erlang interpreting it as a
     * result from a commit operation.
     *
     * NOTE: this method should not be called manually by an application and may
     * change without prior notice!
     *
     * @param received_raw
     *            the object to process
     * @param compressed
     *            whether the transfer of values is compressed or not
     *
     * @throws AbortException
     *             if the commit of the commit failed
     * @throws UnknownException
     *             if any other error occurs
     */
    public static final void processResult_commit(final OtpErlangObject received_raw,
            final boolean compressed) throws AbortException,
            UnknownException {
        /*
         * possible return values:
         *  {ok} | {fail, abort, KeyList}
         */
        try {
            final OtpErlangTuple received = (OtpErlangTuple) received_raw;
            if (received.equals(CommonErlangObjects.okTupleAtom)) {
                return;
            } else {
                CommonErlangObjects.checkResult_failAbort(received, compressed);
            }
            throw new UnknownException(received_raw);
        } catch (final ClassCastException e) {
            // e.printStackTrace();
            throw new UnknownException(e, received_raw);
        }
    }
}