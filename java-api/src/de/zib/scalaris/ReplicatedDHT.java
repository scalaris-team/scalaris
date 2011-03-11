/*
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

import com.ericsson.otp.erlang.OtpAuthException;
import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangExit;
import com.ericsson.otp.erlang.OtpErlangInt;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * Provides methods to delete all replicas of the given key
 * (from <code>api_rdht.erl</code>).
 * 
 * <p>
 * Instances of this class can be generated using a given connection to a
 * scalaris node using {@link #ReplicatedDHT(Connection)} or without a
 * connection ({@link #ReplicatedDHT()}) in which case a new connection is
 * created using {@link ConnectionFactory#createConnection()}.
 * </p>
 * 
 * <h3>Deleting values</h3>
 * <pre>
 * <code style="white-space:pre;">
 *   String key;
 *   int timeout;
 *   DeleteResult result;
 *   
 *   TransactionSingleOp sc = new TransactionSingleOp();
 *   sc.delete(key);                    // {@link #delete(String)}
 *   sc.delete(key, timeout);           // {@link #delete(String, int)}
 *   result = sc.getLastDeleteResult(); // {@link #getLastDeleteResult()}
 * </code>
 * </pre>
 * 
 * <h3>Connection errors</h3>
 * 
 * Errors when setting up connections or trying to send/receive RPCs will be
 * handed to the {@link ConnectionPolicy} that has been set when the connection
 * was created. By default, {@link ConnectionFactory} uses
 * {@link DefaultConnectionPolicy} which implements automatic connection-retries
 * by classifying nodes as good or bad depending on their previous state. The
 * number of automatic retries is adjustable (default: 3).
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 2.8
 * @since 2.6
 */
public class ReplicatedDHT {
    /**
     * Stores the result list returned by erlang during a delete operation.
     * 
     * @see #delete(String)
     */
    private OtpErlangList lastDeleteResult = null;
    
    /**
     * Connection to a TransactionSingleOp node.
     */
    private Connection connection;
    
    /**
     * Constructor, uses the default connection returned by
     * {@link ConnectionFactory#createConnection()}.
     * 
     * @throws ConnectionException
     *             if the connection fails
     */
    public ReplicatedDHT() throws ConnectionException {
        connection = ConnectionFactory.getInstance().createConnection();
    }

    /**
     * Constructor, uses the given connection to an erlang node.
     * 
     * @param conn
     *            connection to use for the transaction
     * 
     * @throws ConnectionException
     *             if the connection fails
     */
    public ReplicatedDHT(Connection conn) throws ConnectionException {
        connection = conn;
    }
    
    // /////////////////////////////
    // delete methods
    // /////////////////////////////
    
    /**
     * Tries to delete all replicas of the given <tt>key</tt> in 2000ms.
     * 
     * @param key
     *            the key to delete
     * 
     * @return the number of successfully deleted replicas
     * 
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws TimeoutException
     *             if a timeout occurred while trying to delete the value
     * @throws NodeNotFoundException
     *             if no scalaris node was found
     * @throws UnknownException
     *             if any other error occurs
     * 
     * @since 2.8
     * 
     * @see #delete(OtpErlangString, int)
     */
    public long delete(OtpErlangString key) throws ConnectionException,
    TimeoutException, NodeNotFoundException, UnknownException {
        return delete(key, 2000);
    }

    /**
     * Tries to delete all replicas of the given <tt>key</tt>.
     * 
     * WARNING: This function can lead to inconsistent data (e.g. deleted items
     * can re-appear). Also when re-creating an item the version before the
     * delete can re-appear.
     * 
     * @param key
     *            the key to delete
     * @param timeout
     *            the time (in milliseconds) to wait for results
     * 
     * @return the number of successfully deleted replicas
     * 
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws TimeoutException
     *             if a timeout occurred while trying to delete the value
     * @throws NodeNotFoundException
     *             if no scalaris node was found
     * @throws UnknownException
     *             if any other error occurs
     * 
     * @since 2.8
     */
    public long delete(OtpErlangString key, int timeout) throws ConnectionException,
    TimeoutException, NodeNotFoundException, UnknownException {
        OtpErlangObject received_raw = null;
        lastDeleteResult = null;
        try {
            received_raw = connection.doRPC("api_rdht", "delete",
                    new OtpErlangList( new OtpErlangObject[] {
                            key,
                            new OtpErlangInt(timeout) }));
            OtpErlangTuple received = (OtpErlangTuple) received_raw;
            OtpErlangAtom state = (OtpErlangAtom) received.elementAt(0);

            /*
             * possible return values:
             *  - {ok, ResultsOk::pos_integer(), ResultList::[ok | undef]}
             *  - {fail, timeout}
             *  - {fail, timeout, ResultsOk::pos_integer(), ResultList::[ok | undef]}
             *  - {fail, node_not_found}
             */
            if (state.equals(CommonErlangObjects.okAtom) && received.arity() == 3) {
                lastDeleteResult = (OtpErlangList) received.elementAt(2);
                long succeeded = ((OtpErlangLong) received.elementAt(1)).longValue();
                return succeeded;
            } else if (state.equals(CommonErlangObjects.failAtom) && received.arity() >= 2) {
                OtpErlangObject reason = received.elementAt(1);
                if (reason.equals(CommonErlangObjects.timeoutAtom)) {
                    if (received.arity() == 4) {
                        lastDeleteResult = (OtpErlangList) received.elementAt(3);
                        throw new TimeoutException(received_raw);
                    } else if(received.arity() == 2) {
                        throw new TimeoutException(received_raw);
                    } else {
                        throw new UnknownException(received_raw);
                    }
                } else if (reason.equals(CommonErlangObjects.nodeNotFoundAtom)) {
                    throw new NodeNotFoundException(received_raw);
                } else {
                    throw new UnknownException(received_raw);
                }
            } else {
                throw new UnknownException(received_raw);
            }
        } catch (OtpErlangExit e) {
            // e.printStackTrace();
            throw new ConnectionException(e);
        } catch (OtpAuthException e) {
            // e.printStackTrace();
            throw new ConnectionException(e);
        } catch (IOException e) {
            // e.printStackTrace();
            throw new ConnectionException(e);
        } catch (ClassCastException e) {
            // e.printStackTrace();
            // received_raw is not null since the first class cast is after the RPC!
            throw new UnknownException(e, received_raw);
        }
    }
    
    /**
     * Tries to delete all replicas of the given <tt>key</tt> in 2000ms.
     * 
     * @param key
     *            the key to delete
     * 
     * @return the number of successfully deleted replicas
     * 
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws TimeoutException
     *             if a timeout occurred while trying to delete the value
     * @throws NodeNotFoundException
     *             if no scalaris node was found
     * @throws UnknownException
     *             if any other error occurs
     * 
     * @since 2.2
     * 
     * @see #delete(String, int)
     */
    public long delete(String key) throws ConnectionException,
    TimeoutException, NodeNotFoundException, UnknownException {
        return delete(key, 2000);
    }

    /**
     * Tries to delete all replicas of the given <tt>key</tt>.
     * 
     * WARNING: This function can lead to inconsistent data (e.g. deleted items
     * can re-appear). Also when re-creating an item the version before the
     * delete can re-appear.
     * 
     * @param key
     *            the key to delete
     * @param timeout
     *            the time (in milliseconds) to wait for results
     * 
     * @return the number of successfully deleted replicas
     * 
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws TimeoutException
     *             if a timeout occurred while trying to delete the value
     * @throws NodeNotFoundException
     *             if no scalaris node was found
     * @throws UnknownException
     *             if any other error occurs
     * 
     * @since 2.2
     * 
     * @see #delete(OtpErlangString, int)
     */
    public long delete(String key, int timeout) throws ConnectionException,
    TimeoutException, NodeNotFoundException, UnknownException {
        return delete(new OtpErlangString(key), timeout);
    }

    /**
     * Returns the result of the last call to {@link #delete(String)}.
     * 
     * NOTE: This function traverses the result list returned by erlang and
     * therefore takes some time to process. It is advised to store the returned
     * result object once generated.
     * 
     * @return the delete result
     * 
     * @throws UnknownException
     *             is thrown if an unknown reason was encountered
     * 
     * @see #delete(String)
     */
    public DeleteResult getLastDeleteResult() throws UnknownException {
        try {
            return new DeleteResult(lastDeleteResult);
        } catch (UnknownException e) {
            throw new UnknownException(e, lastDeleteResult);
        }
    }
    
    /**
     * Closes the transaction's connection to a scalaris node.
     * 
     * Note: Subsequent calls to the other methods will throw
     * {@link ConnectionException}s!
     */
    public void closeConnection() {
        connection.close();
    }
}
