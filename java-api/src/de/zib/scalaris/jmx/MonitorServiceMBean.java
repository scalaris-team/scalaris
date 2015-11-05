package de.zib.scalaris.jmx;

import java.util.Map;

import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.UnknownException;

/**
 * Provides methods to monitor a whole Scalaris ring via JMX.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.15
 * @since 3.15
 */
public interface MonitorServiceMBean {
    /**
     * Gets the total load of the whole Scalaris ring from the VM the monitor is
     * connected to.
     *
     * @return number of DHT nodes
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public abstract Long getTotalLoad() throws ConnectionException, UnknownException;

    /**
     * Gets the number of Scalaris nodes of the whole Scalaris ring from the VM
     * the monitor is connected to.
     *
     * @return number of DHT nodes
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public abstract Long getNodes() throws ConnectionException, UnknownException;

    /**
     * Gets average latency values of the whole Scalaris ring from the VM the
     * monitor is connected to.
     *
     * @return map of timestamps to average latencies
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public abstract Map<Long, Double> getLatencyAvg() throws ConnectionException, UnknownException;

    /**
     * Gets the standard deviation of the latency values of the whole Scalaris
     * ring from the VM the monitor is connected to.
     *
     * @return map of timestamps to latency (standard) deviation
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public abstract Map<Long, Double> getLatencyStddev() throws ConnectionException, UnknownException;

    /**
     * Gets the current, i.e. latest, average latency of the whole Scalaris ring
     * from the VM the monitor is connected to.
     *
     * @return latest average latency (or <tt>null</tt> if there is none)
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public abstract Double getCurLatencyAvg() throws ConnectionException, UnknownException;

    /**
     * Gets the current, i.e. latest, standard deviation of the latency of the
     * whole Scalaris ring from the VM the monitor is connected to.
     *
     * @return latest latency (standard) deviation (or <tt>null</tt> if there is
     *         none)
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public abstract Double getCurLatencyStddev() throws ConnectionException, UnknownException;

}