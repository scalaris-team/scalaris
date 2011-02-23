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

import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * Contains some often used objects as static objects as static members in
 * order to avoid re-creating them each time they are needed.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
final class CommonErlangObjects { 
    static final OtpErlangAtom readAtom = new OtpErlangAtom("read");
    static final OtpErlangAtom writeAtom = new OtpErlangAtom("write");
    static final OtpErlangAtom okAtom = new OtpErlangAtom("ok");
    static final OtpErlangAtom failAtom = new OtpErlangAtom("fail");
    static final OtpErlangAtom abortAtom = new OtpErlangAtom("abort");
    static final OtpErlangAtom timeoutAtom = new OtpErlangAtom("timeout");
    static final OtpErlangAtom notFoundAtom = new OtpErlangAtom("not_found");
    static final OtpErlangTuple okTupleAtom = new OtpErlangTuple(okAtom);
    static final OtpErlangTuple commitTupleAtom = new OtpErlangTuple(new OtpErlangAtom("commit"));
}
