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
	ConnectionPolicy connectionPolicy;

	/**
	 * Creates a new connection using the given nodes and a default connection
	 * policy.
	 * 
	 * Provided for convenience.
	 * 
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
		this.connectionPolicy = new DefaultConnectionPolicy(remote);
		this.remote = connectionPolicy.selectNode();
		
		connect();
	}

	/**
	 * Creates a new connection between the a <tt>self</tt> node and one of the
	 * <tt>remoteNodes</tt>, selected by the <tt>connectionPolicy</tt>.
	 * 
	 * @param self
	 *            the local node
	 * @param connectionPolicy
	 *            the connection policy to use
	 * 
	 * @throws UnknownHostException
	 *             if the remote host could not be found
	 * @throws IOException
	 *             if it was not possible to connect to the remote node
	 * @throws OtpAuthException
	 *             if the connection was refused by the remote node
	 */
	public Connection(OtpSelf self, ConnectionPolicy connectionPolicy) throws UnknownHostException,
			IOException, OtpAuthException {
		super();
		this.self = self;
		this.remote = connectionPolicy.selectNode();
		this.connectionPolicy = connectionPolicy;
		
		connect();
	}
	
	/**
	 * Tries connecting to the current {@link #remote} node. If this fails, it
	 * will try re-connecting to a node the {@link #connectionPolicy} chooses as
	 * long as this does not throw an exception. The {@link #remote} node will
	 * be set to the node the connection has been established with (or the last
	 * tried node).
	 * 
	 * @throws UnknownHostException
	 *             if the remote host could not be found
	 * @throws IOException
	 *             if it was not possible to connect to the remote node
	 * @throws OtpAuthException
	 *             if the connection was refused by the remote node
	 */
	private void connect() throws UnknownHostException,
	IOException, OtpAuthException {
		boolean success = false;
		int retry = 0;
		while(!success) {
			try {
				connection = self.connect(remote.getNode());
				connectionPolicy.nodeConnectSuccess(remote);
				success = true;
			} catch (UnknownHostException e) {
				connectionPolicy.nodeFailed(remote);
				remote = connectionPolicy.selectNode(++retry, remote, e);
			} catch (OtpAuthException e) {
				connectionPolicy.nodeFailed(remote);
				remote = connectionPolicy.selectNode(++retry, remote, e);
			} catch (IOException e) {
				connectionPolicy.nodeFailed(remote);
				remote = connectionPolicy.selectNode(++retry, remote, e);
			}
		}
	}
	
	private void reconnect() throws UnknownHostException, IOException,
			OtpAuthException {
		close();
		connect();
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
		boolean success = false;
		while(!success) {
			try {
				connection.sendRPC(mod, fun, args);
				OtpErlangObject result = connection.receiveRPC();
				success = true;
				return result;
			} catch (OtpErlangExit e) {
				connectionPolicy.nodeFailed(remote);
				// first re-try (connection was the first contact)
				remote = connectionPolicy.selectNode(1, remote, e);
				// reconnect (and then re-try the operation) if no exception was thrown:
				reconnect();
			} catch (OtpAuthException e) {
				connectionPolicy.nodeFailed(remote);
				// first re-try (connection was the first contact)
				remote = connectionPolicy.selectNode(1, remote, e);
				// reconnect (and then re-try the operation) if no exception was thrown:
				reconnect();
			} catch (IOException e) {
				connectionPolicy.nodeFailed(remote);
				// first re-try (connection was the first contact)
				remote = connectionPolicy.selectNode(1, remote, e);
				// reconnect (and then re-try the operation) if no exception was thrown:
				reconnect();
			}
		}
		// this should not happen as there is only one way out of the while
		// without throwing an exception
		throw new InternalError();
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
