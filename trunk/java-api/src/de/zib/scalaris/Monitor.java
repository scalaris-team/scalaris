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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangTuple;

import de.zib.scalaris.ErlangValue.ListElementConverter;

/**
 * Provides methods to monitor a specific Scalaris (Erlang) VM.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.11
 * @since 3.11
 */
public class Monitor {
    /**
     * Connection to a Scalaris node.
     */
    private final Connection connection;

    /**
     * Creates a connection to the erlang VM of the given Scalaris node. Uses
     * the connection policy of the global connection factory.
     *
     * @param node
     *            Scalaris node to connect with
     * @throws ConnectionException
     *             if the connection fails or the connection policy is not
     *             cloneable
     */
    public Monitor(final String node) throws ConnectionException {
        final ConnectionFactory cf = ConnectionFactory.getInstance();
        final String fixedNode = ConnectionFactory.fixLocalhostName(node);
        connection = cf.createConnection(new FixedNodeConnectionPolicy(fixedNode));
    }

    /**
     * Plain old data object for results of {@link Monitor#getNodeInfo()}.
     *
     * @author Nico Kruber, kruber@zib.de
     * @version 3.11
     * @since 3.11
     */
    public static class GetNodeInfoResult {
        /**
         * Scalaris version string.
         */
        public final String scalarisVersion;
        /**
         * Erlang version string.
         */
        public final String erlangVersion;
        /**
         * Number of DHT nodes in the node.
         */
        public final int dhtNodes;

        /**
         * @param scalarisVersion
         *            Scalaris version string
         * @param erlangVersion
         *            Erlang version string
         * @param dhtNodes
         *            number of DHT nodes in the node
         */
        public GetNodeInfoResult(final String scalarisVersion, final String erlangVersion,
                final int dhtNodes) {
            super();
            this.scalarisVersion = scalarisVersion;
            this.erlangVersion = erlangVersion;
            this.dhtNodes = dhtNodes;
        }
    }

    /**
     * Gets some information about the VM and Scalaris.
     *
     * @return VM information
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public GetNodeInfoResult getNodeInfo()
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_monitor", "get_node_info",
                    new OtpErlangObject[] {});
        try {
            final OtpErlangList received = (OtpErlangList) received_raw;
            final Map<String, OtpErlangObject> result = new LinkedHashMap<String, OtpErlangObject>(
                    received.arity());
            for (final OtpErlangObject iter : received) {
                final OtpErlangTuple iter_tpl = (OtpErlangTuple) iter;
                if (iter_tpl.arity() == 2) {
                    final String key = ((OtpErlangAtom) (iter_tpl.elementAt(0))).atomValue();
                    result.put(key, iter_tpl.elementAt(1));
                } else {
                    throw new UnknownException(received_raw);
                }
            }
            final String scalarisVersion = new ErlangValue(result.get("scalaris_version")).stringValue();
            final String erlangVersion = new ErlangValue(result.get("erlang_version")).stringValue();
            final int dhtNodes = new ErlangValue(result.get("dht_nodes")).intValue();
            return new GetNodeInfoResult(scalarisVersion, erlangVersion, dhtNodes);
        } catch (final ClassCastException e) {
            throw new UnknownException(e, received_raw);
        } catch (final NullPointerException e) {
            throw new UnknownException(e, received_raw);
        }
    }

    /**
     * Plain old data object for results of {@link Monitor#getNodePerformance()}.
     *
     * @author Nico Kruber, kruber@zib.de
     * @version 3.11
     * @since 3.11
     */
    public static class GetNodePerformanceResult {
        /**
         * Average latency of transactional operations at different points in
         * time.
         */
        public final Map<Long /*time*/, Double> latencyAvg;
        /**
         * Standard deviation of the latency of transactional operations at
         * different points in time.
         */
        public final Map<Long /*time*/, Double> latencyStddev;

        /**
         * @param latencyAvg
         *            average latency of transactional operations
         * @param latencyStddev
         *            standard deviation of the latency of transactional
         *            operations
         */
        public GetNodePerformanceResult(final Map<Long, Double> latencyAvg,
                final Map<Long, Double> latencyStddev) {
            super();
            this.latencyAvg = latencyAvg;
            this.latencyStddev = latencyStddev;
        }
    }

    /**
     * Gets some information about the VM and Scalaris.
     *
     * @return VM information
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public GetNodePerformanceResult getNodePerformance()
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_monitor", "get_node_performance",
                    new OtpErlangObject[] {});
        try {
            final OtpErlangList received = (OtpErlangList) received_raw;
            final Map<String, OtpErlangObject> result = new LinkedHashMap<String, OtpErlangObject>(
                    received.arity());
            for (final OtpErlangObject iter : received) {
                final OtpErlangTuple iter_tpl = (OtpErlangTuple) iter;
                if (iter_tpl.arity() == 2) {
                    final String key = ((OtpErlangAtom) (iter_tpl.elementAt(0))).atomValue();
                    result.put(key, iter_tpl.elementAt(1));
                } else {
                    throw new UnknownException(received_raw);
                }
            }
            final Map<Long, Double> latencyAvg = tupleListToLDMap(result.get("latency_avg"));
            final Map<Long, Double> latencyStddev = tupleListToLDMap(result.get("latency_stddev"));
            return new GetNodePerformanceResult(latencyAvg, latencyStddev);
        } catch (final ClassCastException e) {
            throw new UnknownException(e, received_raw);
        } catch (final NullPointerException e) {
            throw new UnknownException(e, received_raw);
        }
    }

    /**
     * Plain old data object for results of {@link Monitor#getServiceInfo()}.
     *
     * @author Nico Kruber, kruber@zib.de
     * @version 3.11
     * @since 3.11
     */
    public static class GetServiceInfoResult {
        /**
         * Total load accumulated among all Scalaris nodes in the ring.
         */
        public final Long totalLoad;
        /**
         * Number of Scalaris nodes in the ring.
         */
        public final Long nodes;

        /**
         * @param totalLoad
         *            total load accumulated among all Scalaris nodes in the
         *            ring
         * @param nodes
         *            number of Scalaris nodes in the ring
         */
        public GetServiceInfoResult(final Long totalLoad, final Long nodes) {
            super();
            this.totalLoad = totalLoad;
            this.nodes = nodes;
        }
    }

    /**
     * Gets some information about the Scalaris ring.
     *
     * @return Scalaris ring information
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public GetServiceInfoResult getServiceInfo()
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_monitor", "get_service_info",
                    new OtpErlangObject[] {});
        try {
            final OtpErlangList received = (OtpErlangList) received_raw;
            final Map<String, OtpErlangObject> result = new LinkedHashMap<String, OtpErlangObject>(
                    received.arity());
            for (final OtpErlangObject iter : received) {
                final OtpErlangTuple iter_tpl = (OtpErlangTuple) iter;
                if (iter_tpl.arity() == 2) {
                    final String key = ((OtpErlangAtom) (iter_tpl.elementAt(0))).atomValue();
                    result.put(key, iter_tpl.elementAt(1));
                } else {
                    throw new UnknownException(received_raw);
                }
            }
            final Long totalLoad = new ErlangValue(result.get("total_load")).longValue();
            final Long nodes = new ErlangValue(result.get("nodes")).longValue();
            return new GetServiceInfoResult(totalLoad, nodes);
        } catch (final ClassCastException e) {
            throw new UnknownException(e, received_raw);
        } catch (final NullPointerException e) {
            throw new UnknownException(e, received_raw);
        }
    }

    /**
     * Plain old data object for results of {@link Monitor#getServicePerformance()}.
     *
     * @author Nico Kruber, kruber@zib.de
     * @version 3.11
     * @since 3.11
     */
    public static class GetServicePerformanceResult {
        /**
         * Average latency of transactional operations at different points in
         * time.
         */
        public final Map<Long /*time*/, Double> latencyAvg;
        /**
         * Standard deviation of the latency of transactional operations at
         * different points in time.
         */
        public final Map<Long /*time*/, Double> latencyStddev;

        /**
         * @param latencyAvg
         *            average latency of transactional operations
         * @param latencyStddev
         *            standard deviation of the latency of transactional
         *            operations
         */
        public GetServicePerformanceResult(final Map<Long, Double> latencyAvg,
                final Map<Long, Double> latencyStddev) {
            super();
            this.latencyAvg = latencyAvg;
            this.latencyStddev = latencyStddev;
        }
    }

    /**
     * Gets some information about the Scalaris ring.
     *
     * @return Scalaris ring information
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public GetServicePerformanceResult getServicePerformance()
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_monitor", "get_service_performance",
                    new OtpErlangObject[] {});
        try {
            final OtpErlangList received = (OtpErlangList) received_raw;
            final Map<String, OtpErlangObject> result = new LinkedHashMap<String, OtpErlangObject>(
                    received.arity());
            for (final OtpErlangObject iter : received) {
                final OtpErlangTuple iter_tpl = (OtpErlangTuple) iter;
                if (iter_tpl.arity() == 2) {
                    final String key = ((OtpErlangAtom) (iter_tpl.elementAt(0))).atomValue();
                    result.put(key, iter_tpl.elementAt(1));
                } else {
                    throw new UnknownException(received_raw);
                }
            }
            final Map<Long, Double> latencyAvg = tupleListToLDMap(result.get("latency_avg"));
            final Map<Long, Double> latencyStddev = tupleListToLDMap(result.get("latency_stddev"));
            return new GetServicePerformanceResult(latencyAvg, latencyStddev);
        } catch (final ClassCastException e) {
            throw new UnknownException(e, received_raw);
        } catch (final NullPointerException e) {
            throw new UnknownException(e, received_raw);
        }
    }

    /**
     * Converts a list of 2-tuples into a map using the first entry of the tuple
     * as the key and the second as the value.
     *
     * @param object
     *            the Erlang object to convert
     * @param keyConv
     *            the converter to use for the key component
     * @param valConv
     *            the converter to use for the value component
     *
     * @return the converted map
     *
     * @throws ClassCastException
     *             if the conversion fails
     */
    private static <K, V> Map<K, V> tupleListToMap(final OtpErlangObject object,
            final ListElementConverter<K> keyConv,
            final ListElementConverter<V> valConv) throws ClassCastException {
        final OtpErlangList list = ErlangValue.otpObjectToOtpList(object);
        final LinkedHashMap<K, V> result = new LinkedHashMap<K, V>(list.arity());
        for (int i = 0; i < list.arity(); ++i) {
            final OtpErlangTuple element = (OtpErlangTuple) list.elementAt(i);
            if (element.arity() != 2) {
                throw new ClassCastException("wrong tuple arity");
            }
            final K key = keyConv.convert(i, new ErlangValue(element.elementAt(0)));
            final V value = valConv.convert(i, new ErlangValue(element.elementAt(1)));
            result.put(key, value);
        }
        return result;
    }

    /**
     * Converts a list of 2-tuples with {@link Long} keys and {@link Double} to
     * a map.
     *
     * @param object
     *            the Erlang object to convert
     *
     * @return the converted map
     *
     * @throws ClassCastException
     *             if the conversion fails
     *
     * @see #tupleListToMap(OtpErlangObject, ListElementConverter, ListElementConverter)
     */
    private static Map<Long, Double> tupleListToLDMap(
            final OtpErlangObject object) throws ClassCastException {
        return tupleListToMap(object, new ListElementConverter<Long>() {
            public Long convert(final int i, final ErlangValue v) {
                return v.longValue();
            }
        }, new ListElementConverter<Double>() {
            public Double convert(final int i, final ErlangValue v) {
                return v.doubleValue();
            }
        });
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

    /**
     * Extracts the current performance value of a timestamp-to-double map like
     * in the members of {@link GetNodePerformanceResult} or
     * {@link GetServicePerformanceResult}.
     *
     * @param map
     *            the map to extract from
     *
     * @return the latest reported performance or <tt>null</tt> if there is none
     */
    public static Double getCurrentPerfValue(final Map<Long, Double> map) {
        final Set<Entry<Long, Double>> entrySet = map.entrySet();
        if (entrySet.isEmpty()) {
            return null;
        } else {
            return entrySet.iterator().next().getValue();
        }
    }
}
