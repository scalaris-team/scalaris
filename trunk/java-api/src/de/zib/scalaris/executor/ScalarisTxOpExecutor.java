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
import de.zib.scalaris.TimeoutException;
import de.zib.scalaris.Transaction;
import de.zib.scalaris.Transaction.ResultList;
import de.zib.scalaris.UnknownException;

/**
 * Executes multiple {@link ScalarisOp} operations in multiple phases only
 * sending requests to Scalaris once per work phase. Uses {@link Transaction}.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.13
 * @since 3.13
 */
public class ScalarisTxOpExecutor extends ScalarisOpExecutor {
    protected final Transaction scalaris_tx;

    /**
     * Creates a new executor.
     *
     * @param scalaris_tx
     *            the Scalaris connection to use
     */
    public ScalarisTxOpExecutor(final Transaction scalaris_tx) {
        this.scalaris_tx = scalaris_tx;
    }

    @Override
    protected ResultList executeRequests(final RequestList requests)
            throws ConnectionException, TimeoutException, AbortException,
            UnknownException {
        return scalaris_tx.req_list((Transaction.RequestList) requests);
    }

    @Override
    protected RequestList newRequestList() {
        return new Transaction.RequestList();
    }
}
