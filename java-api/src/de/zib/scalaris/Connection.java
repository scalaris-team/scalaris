/**
 *  Copyright 2007-2011 Zuse Institute Berlin
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
 * Wraps an {@link OtpConnection} and allows automatic re-connects using a
 * {@link ConnectionPolicy} object.
 *
 * @author Nico Kruber, kruber@zib.de
 *
 * @version 3.14
 * @since 2.3
 */
public class Connection {
    /**
     * The connection this object wraps.
     */
    OtpConnection connection;
    /**
     * The local node used for the connection to a remote node.
     */
    OtpSelf self;
    /**
     * The remote node connected to.
     */
    PeerNode remote;
    /**
     * The connection policy object that sets how and whether to automatically
     * reconnect on failures.
     */
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
    public Connection(final OtpSelf self, final PeerNode remote) throws UnknownHostException,
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
    public Connection(final OtpSelf self, final ConnectionPolicy connectionPolicy) throws UnknownHostException,
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
            } catch (final UnknownHostException e) {
                connectionPolicy.nodeFailed(remote);
                remote = connectionPolicy.selectNode(++retry, remote, e);
            } catch (final OtpAuthException e) {
                connectionPolicy.nodeFailed(remote);
                remote = connectionPolicy.selectNode(++retry, remote, e);
            } catch (final IOException e) {
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
     * @throws ConnectionException
     *             if the connection is not active, a communication error
     *             occurs, an exit signal is received from a process on the
     *             peer node or the remote node sends a message containing an
     *             invalid cookie
     */
    public OtpErlangObject doRPC(final String mod, final String fun, final OtpErlangList args)
            throws ConnectionException {
        try {
            boolean success = false;
            final boolean isConnected = connection.isConnected();
            while(!success) {
                try {
                    connection.sendRPC(mod, fun, args);
                    final OtpErlangObject result = connection.receiveRPC();
                    // result may be null but this should not happen and is an error anyway!
                    if (result != null) {
                        success = true;
                        return result;
                    }
                } catch (final OtpErlangExit e) {
                    connectionPolicy.nodeFailed(remote);
                    // first re-try (connection was the first contact)
                    remote = connectionPolicy.selectNode(1, remote, e);
                    // reconnect (and then re-try the operation) if no exception was thrown:
                    reconnect();
                } catch (final OtpAuthException e) {
                    connectionPolicy.nodeFailed(remote);
                    // first re-try (connection was the first contact)
                    remote = connectionPolicy.selectNode(1, remote, e);
                    // reconnect (and then re-try the operation) if no exception was thrown:
                    reconnect();
                } catch (final IOException e) {
                    // don't count RPC requests on closed connections as a failing node:
                    if (isConnected) {
                        connectionPolicy.nodeFailed(remote);
                    }
                    // first re-try (connection was the first contact)
                    remote = connectionPolicy.selectNode(1, remote, e);
                    // reconnect (and then re-try the operation) if no exception was thrown:
                    reconnect();
                }
            }
            // this should not happen as there is only one way out of the while
            // without throwing an exception
            throw new InternalError();
        } catch (final OtpErlangExit e) {
            // e.printStackTrace();
            throw new ConnectionException(e);
        } catch (final OtpAuthException e) {
            // e.printStackTrace();
            throw new ConnectionException(e);
        } catch (final IOException e) {
            // e.printStackTrace();
            throw new ConnectionException(e);
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
     * @throws ConnectionException
     *             if the connection is not active, a communication error
     *             occurs, an exit signal is received from a process on the
     *             peer node or the remote node sends a message containing an
     *             invalid cookie
     */
    public OtpErlangObject doRPC(final String mod, final String fun, final OtpErlangObject[] args)
            throws ConnectionException {
        return doRPC(mod, fun, new OtpErlangList(args));
    }

    /**
     * Sends the given RPC and returns immediately.
     *
     * @param mod
     *            the module of the function to call
     * @param fun
     *            the function to call
     * @param args
     *            the function's arguments
     *
     * @throws ConnectionException
     *             if the connection is not active, a communication error
     *             occurs, an exit signal is received from a process on the
     *             peer node or the remote node sends a message containing an
     *             invalid cookie
     */
    public void sendRPC(final String mod, final String fun, final OtpErlangList args)
            throws ConnectionException {
        try {
            boolean success = false;
            while(!success) {
                try {
                    connection.sendRPC(mod, fun, args);
                    success = true;
                    return;
                } catch (final IOException e) {
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
        } catch (final OtpAuthException e) {
            // e.printStackTrace();
            throw new ConnectionException(e);
        } catch (final IOException e) {
            // e.printStackTrace();
            throw new ConnectionException(e);
        }
    }

    /**
     * Sends the given RPC and returns immediately.
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
     * @throws ConnectionException
     *             if the connection is not active, a communication error
     *             occurs, an exit signal is received from a process on the
     *             peer node or the remote node sends a message containing an
     *             invalid cookie
     */
    public void sendRPC(final String mod, final String fun, final OtpErlangObject[] args)
            throws ConnectionException {
        sendRPC(mod, fun, new OtpErlangList(args));
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
     * Gets the encapsulated OTP connection object.
     *
     * @return the connection object
     */
    public OtpConnection getConnection() {
        return connection;
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
