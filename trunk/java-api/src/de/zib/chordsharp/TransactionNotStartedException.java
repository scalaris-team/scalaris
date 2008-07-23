/**
 *  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
package de.zib.chordsharp;

/**
 * Exception that is thrown when a new transaction is not started but is used
 * nevertheless. Since this is a programmer's mistake, a RuntimeException is
 * chosen as the base class which eliminates the need to specify the exception
 * in a throws clause.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class TransactionNotStartedException extends RuntimeException {
	/**
	 * class version for serialisation
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Creates the exception with no message.
	 */
	public TransactionNotStartedException() {
	}

	/**
	 * Creates the exception with the given message.
	 * 
	 * @param msg
	 *            message of the exception
	 */
	public TransactionNotStartedException(String msg) {
		super(msg);
	}
}
