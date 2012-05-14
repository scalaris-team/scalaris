package de.zib.scalaris.jmx;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.UnknownException;

/**
 * Provides methods to monitor a specific Scalaris (Erlang) VM via JMX.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.15
 * @since 3.15
 */
public class MonitorNode implements MonitorNodeMBean {
    protected final de.zib.scalaris.Monitor monitor;

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
    public MonitorNode(final String node) throws ConnectionException {
        this.monitor = new de.zib.scalaris.Monitor(node);
    }

    /* (non-Javadoc)
     * @see jmx.MonitorNodeMBean#getScalarisVersion()
     */
    public String getScalarisVersion() throws ConnectionException, UnknownException {
        return monitor.getNodeInfo().scalarisVersion;
    }
    /* (non-Javadoc)
     * @see jmx.MonitorNodeMBean#getErlangVersion()
     */
    public String getErlangVersion() throws ConnectionException, UnknownException {
        return monitor.getNodeInfo().erlangVersion;
    }
    /* (non-Javadoc)
     * @see jmx.MonitorNodeMBean#getDhtNodes()
     */
    public int getDhtNodes() throws ConnectionException, UnknownException {
        return monitor.getNodeInfo().dhtNodes;
    }
    /* (non-Javadoc)
     * @see jmx.MonitorNodeMBean#getLatencyAvg()
     */
    public Map<Long, Double> getLatencyAvg() throws ConnectionException, UnknownException {
        return monitor.getNodePerformance().latencyAvg;
    }
    /* (non-Javadoc)
     * @see jmx.MonitorNodeMBean#getLatencyStddev()
     */
    public Map<Long, Double> getLatencyStddev() throws ConnectionException, UnknownException {
        return monitor.getNodePerformance().latencyStddev;
    }
    /* (non-Javadoc)
     * @see jmx.MonitorNodeMBean#getCurLatencyAvg()
     */
    public double getCurLatencyAvg() throws ConnectionException, UnknownException {
        return getCurrentPerfValue(monitor.getNodePerformance().latencyAvg);
    }
    /* (non-Javadoc)
     * @see jmx.MonitorNodeMBean#getCurLatencyStddev()
     */
    public double getCurLatencyStddev() throws ConnectionException, UnknownException {
        return getCurrentPerfValue(monitor.getNodePerformance().latencyStddev);
    }

    static Double getCurrentPerfValue(final Map<Long, Double> map) {
        final Set<Entry<Long, Double>> entrySet = map.entrySet();
        if (entrySet.isEmpty()) {
            return null;
        } else {
            return entrySet.iterator().next().getValue();
        }
    }

}
