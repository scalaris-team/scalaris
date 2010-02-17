/**
 *  Copyright 2007-2010 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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

import java.io.IOException;
import java.net.UnknownHostException;

import com.ericsson.otp.erlang.OtpAuthException;
import com.ericsson.otp.erlang.OtpConnection;
import com.ericsson.otp.erlang.OtpErlangExit;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpSelf;

/**
 * Wraps an {@link OtpConnection} and allows automatic re-connects.
 * 
 * @author Nico Kruber, kruber@zib.de
 * 
 * @version 2.3
 * @since 2.3
 */
public class Connection {
	OtpConnection connection;
	OtpSelf self;
	PeerNode remote;

	/**
	 * @param self
	 *            the local node
	 * @param remote
	 *            the remote node to connect to
	 * 
	 * @throws UnknownHostException
	 *             if the remote host could not be found
	 * @throws IOException
	 *             if it was not possible to connect to the remote node
	 * @throws OtpAuthException
	 *             if the connection was refused by the remote node
	 */
	public Connection(OtpSelf self, PeerNode remote) throws UnknownHostException,
			IOException, OtpAuthException {
		super();
		this.self = self;
		this.remote = remote;
		try {
			connection = self.connect(remote.getNode());
		} catch (UnknownHostException e) {
			remote.addFailedConnection();
			throw e;
		} catch (OtpAuthException e) {
			remote.addFailedConnection();
			throw e;
		} catch (IOException e) {
			remote.addFailedConnection();
			throw e;
		}
		this.remote.setConnectionSuccess();
	}

	/**
	 * Sends the given RPC and waits for a result.
	 * 
	 * @param mod
	 *            the module of the function to call
	 * @param fun
	 *            the function to call
	 * @param args
	 *            the function's arguments
	 * 
	 * @return the result of the call
	 * 
	 * @throws IOException
	 *             if the connection is not active or a communication error
	 *             occurs
	 * @throws OtpErlangExit
	 *             if an exit signal is received from a process on the peer node
	 * @throws OtpAuthException
	 *             if the remote node sends a message containing an invalid
	 *             cookie
	 */
	public OtpErlangObject doRPC(String mod, String fun, OtpErlangList args)
			throws IOException, OtpErlangExit, OtpAuthException {
		try {
			connection.sendRPC(mod, fun, args);
			return connection.receiveRPC();
		} catch (OtpErlangExit e) {
			remote.addFailedConnection();
			throw e;
		} catch (OtpAuthException e) {
			remote.addFailedConnection();
			throw e;
		} catch (IOException e) {
			remote.addFailedConnection();
			throw e;
		}
	}

	/**
	 * Sends the given RPC and waits for a result.
	 * 
	 * Provided for convenience.
	 * 
	 * @param mod
	 *            the module of the function to call
	 * @param fun
	 *            the function to call
	 * @param args
	 *            the function's arguments
	 * 
	 * @return the result of the call
	 * 
	 * @throws IOException
	 *             if the connection is not active or a communication error
	 *             occurs
	 * @throws OtpErlangExit
	 *             if an exit signal is received from a process on the peer node
	 * @throws OtpAuthException
	 *             if the remote node sends a message containing an invalid
	 *             cookie
	 */
	public OtpErlangObject doRPC(String mod, String fun, OtpErlangObject[] args)
			throws IOException, OtpErlangExit, OtpAuthException {
		return doRPC(mod, fun, new OtpErlangList(args));
	}

	/**
	 * Closes the connection to the remote node.
	 */
	public void close() {
		connection.close();
	}
	
	/**
	 * Gets the local node used for the connection.
	 * 
	 * @return the local node (self)
	 */
	public OtpSelf getSelf() {
		return self;
	}

	/**
	 * Gets the remote node connected to.
	 * 
	 * @return the remote node
	 */
	public PeerNode getRemote() {
		return remote;
	}

	/**
	 * Closes the connection when the object is destroyed.
	 */
	@Override
	protected void finalize() throws Throwable {
		close();
		super.finalize();
	}
}
