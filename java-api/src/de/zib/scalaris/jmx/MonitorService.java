package de.zib.scalaris.jmx;

import java.util.Map;

import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.Monitor;
import de.zib.scalaris.UnknownException;

/**
 * Provides methods to monitor a specific Scalaris (Erlang) VM via JMX.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.15
 * @since 3.15
 */
public class MonitorService implements MonitorServiceMBean {
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
    public MonitorService(final String node) throws ConnectionException {
        this.monitor = new de.zib.scalaris.Monitor(node);
    }
    /* (non-Javadoc)
     * @see jmx.MonitorServiceMBean#getTotalLoad()
     */
    public Long getTotalLoad() throws ConnectionException, UnknownException {
        return monitor.getServiceInfo().totalLoad;
    }
    /* (non-Javadoc)
     * @see jmx.MonitorServiceMBean#getNodes()
     */
    public Long getNodes() throws ConnectionException, UnknownException {
        return monitor.getServiceInfo().nodes;
    }
    /* (non-Javadoc)
     * @see jmx.MonitorServiceMBean#getLatencyAvg()
     */
    public Map<Long, Double> getLatencyAvg() throws ConnectionException, UnknownException {
        return monitor.getServicePerformance().latencyAvg;
    }
    /* (non-Javadoc)
     * @see jmx.MonitorServiceMBean#getLatencyStddev()
     */
    public Map<Long, Double> getLatencyStddev() throws ConnectionException, UnknownException {
        return monitor.getServicePerformance().latencyStddev;
    }
    /* (non-Javadoc)
     * @see jmx.MonitorServiceMBean#getCurLatencyAvg()
     */
    public Double getCurLatencyAvg() throws ConnectionException, UnknownException {
        return Monitor.getCurrentPerfValue(monitor.getServicePerformance().latencyAvg);
    }
    /* (non-Javadoc)
     * @see jmx.MonitorServiceMBean#getCurLatencyStddev()
     */
    public Double getCurLatencyStddev() throws ConnectionException, UnknownException {
        return Monitor.getCurrentPerfValue(monitor.getServicePerformance().latencyStddev);
    }

}
