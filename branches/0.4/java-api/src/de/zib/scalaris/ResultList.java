/**
 *  Copyright 2011 Zuse Institute Berlin
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

import com.ericsson.otp.erlang.OtpErlangList;

/**
 * Generic result list.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.8
 * @since 3.5
 */
public abstract class ResultList {
    protected OtpErlangList results = new OtpErlangList();

    /**
     * Default constructor.
     *
     * @param results  the raw results list as returned by scalaris.
     */
    protected ResultList(final OtpErlangList results) {
        this.results = results;
    }

    /**
     * Gets the number of results in the list.
     *
     * @return total number of results
     */
    public int size() {
        return results.arity();
    }

    /**
     * Gets the raw results.
     * (for internal use only)
     *
     * @return results as returned by erlang
     */
    OtpErlangList getResults() {
        return results;
    }
}
