/**
 *  Copyright 2012-2013 Zuse Institute Berlin
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
package de.zib.scalaris.examples.wikipedia;

import java.util.List;

import com.ericsson.otp.erlang.OtpErlangException;

import de.zib.scalaris.AbortException;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.RequestList;
import de.zib.scalaris.TransactionSingleOp;
import de.zib.scalaris.TransactionSingleOp.ResultList;
import de.zib.scalaris.UnknownException;
import de.zib.scalaris.executor.ScalarisOp;
import de.zib.scalaris.executor.ScalarisSingleOpExecutor;

/**
 * Executes multiple {@link ScalarisOp} operations in multiple phases only
 * sending requests to Scalaris once per work phase.
 * 
 * In addition to {@link ScalarisSingleOpExecutor}, also collects info about all
 * involved keys.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class MyScalarisSingleOpExecutor extends ScalarisSingleOpExecutor {
    protected final List<InvolvedKey> involvedKeys;

    /**
     * Creates a new executor.
     * 
     * @param scalaris_single
     *            the Scalaris connection to use
     * @param involvedKeys
     *            list of all involved keys
     */
    public MyScalarisSingleOpExecutor(TransactionSingleOp scalaris_single,
            List<InvolvedKey> involvedKeys) {
        super(scalaris_single);
        this.involvedKeys = involvedKeys;
    }

    /**
     * Executes the given requests and records all involved keys.
     * 
     * @param requests
     *            a request list to execute
     * 
     * @return the results from executing the requests
     * 
     * @throws OtpErlangException
     *             if an error occurred verifying a result from previous
     *             operations
     * @throws UnknownException
     *             if an error occurred verifying a result from previous
     *             operations
     */
    @Override
    protected ResultList executeRequests(RequestList requests)
            throws ConnectionException, AbortException,
            UnknownException {
        ScalarisDataHandler.addInvolvedKeys(involvedKeys, requests.getRequests());
        return super.executeRequests(requests);
    }

    /**
     * @return the involvedKeys
     */
    public List<InvolvedKey> getInvolvedKeys() {
        return involvedKeys;
    }
}
