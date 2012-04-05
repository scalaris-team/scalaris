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
package de.zib.scalaris.examples.wikipedia;

import java.util.ArrayList;
import java.util.List;

import com.ericsson.otp.erlang.OtpErlangException;

import de.zib.scalaris.Transaction;
import de.zib.scalaris.Transaction.RequestList;
import de.zib.scalaris.Transaction.ResultList;
import de.zib.scalaris.UnknownException;

/**
 * Executes multiple {@link ScalarisOp} operations in multiple phases only
 * sending requests to Scalaris once per work phase.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class ScalarisTxOpExecutor {
    final List<String> involvedKeys;
    final Transaction scalaris_tx;
    
    final protected ArrayList<ScalarisOp<RequestList, ResultList>> ops =
            new ArrayList<ScalarisOp<RequestList,ResultList>>();
    
    protected int workPhases = 0;

    /**
     * Creates a new executor.
     * 
     * @param scalaris_tx
     *            the Scalaris connection to use
     * @param involvedKeys
     *            list of all involved keys
     */
    public ScalarisTxOpExecutor(Transaction scalaris_tx, List<String> involvedKeys) {
        this.scalaris_tx = scalaris_tx;
        this.involvedKeys = involvedKeys;
    }

    /**
     * Adds the given operation to be executed.
     * 
     * @param op
     *            the operation to add
     */
    public void addOp(ScalarisOp<RequestList, ResultList> op) {
        if (op.workPhases() > workPhases) {
            workPhases = op.workPhases();
        }
        ops.add(op);
    }
    
    /**
     * Executes all operations previously added with {@link #addOp(ScalarisOp)}.
     * 
     * @param commitLast
     *            whether to commit the requests in the last work phase
     * 
     * @throws OtpErlangException
     *             if an error occurred verifying a result from previous
     *             operations
     * @throws UnknownException
     *             if an error occurred verifying a result from previous
     *             operations
     */
    public void run(boolean commitLast) throws OtpErlangException,
            UnknownException {
        ResultList results = null;
        for (int phase = 0; phase <= workPhases; ++phase) {
            RequestList requests = new RequestList();
            int curOp = 0;
            for (ScalarisOp<RequestList, ResultList> op : ops) {
                // translate the global phase into an operation-specific phase,
                // execute operations as late as possible
                final int opPhase = phase - (workPhases - op.workPhases());
                if (opPhase >= 0 && opPhase <= op.workPhases()) {
                    curOp += op.doPhase(opPhase, curOp, results, requests);
                }
            }
            
            if (phase == (workPhases - 1) && commitLast) {
                requests.addCommit();
            }
            if (phase != workPhases) {
                involvedKeys.addAll(requests.keyList());
                results = scalaris_tx.req_list(requests);
            }
        }
    }

    /**
     * @return the workPhases
     */
    public int getWorkPhases() {
        return workPhases;
    }

    /**
     * @return the involvedKeys
     */
    public List<String> getInvolvedKeys() {
        return involvedKeys;
    }
}
