/**
 *  Copyright 2012 Zuse Institute Berlin
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
package de.zib.scalaris.executor;

import de.zib.scalaris.AbortException;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.RequestList;
import de.zib.scalaris.TransactionSingleOp;
import de.zib.scalaris.TransactionSingleOp.ResultList;
import de.zib.scalaris.UnknownException;


/**
 * Executes multiple {@link ScalarisOp} operations in multiple phases only
 * sending requests to Scalaris once per work phase. Uses
 * {@link TransactionSingleOp}.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.13
 * @since 3.13
 */
public class ScalarisSingleOpExecutor extends ScalarisOpExecutor {
    protected final TransactionSingleOp scalaris_single;

    /**
     * Creates a new executor.
     *
     * @param scalaris_single
     *            the Scalaris connection to use
     */
    public ScalarisSingleOpExecutor(final TransactionSingleOp scalaris_single) {
        this.scalaris_single = scalaris_single;
        reset();
    }

    @Override
    protected ResultList executeRequests(final RequestList requests)
            throws ConnectionException, AbortException, UnknownException {
        return scalaris_single.req_list((TransactionSingleOp.RequestList) requests);
    }

    @Override
    protected RequestList newRequestList() {
        return new TransactionSingleOp.RequestList();
    }
}
