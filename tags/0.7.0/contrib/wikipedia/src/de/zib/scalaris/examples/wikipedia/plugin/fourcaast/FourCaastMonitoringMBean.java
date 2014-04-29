package de.zib.scalaris.examples.wikipedia.plugin.fourcaast;

/**
 * Provides methods to monitor the Wiki servlet via JMX.
 *
 * @author Nico Kruber, kruber@zib.de
 */
public interface FourCaastMonitoringMBean {

    /**
     * Class holding a single page view record.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static class Record {
        /**
         * Creates a new record.
         * 
         * @param serviceUser
         * @param timestamp
         * @param title
         * @param serverTime
         * @param dbTime
         * @param renderTime
         */
        public Record(String serviceUser, String timestamp, String title,
                long serverTime, long dbTime, long renderTime) {
            this.serviceUser = serviceUser;
            this.timestamp = timestamp;
            this.title = title;
            this.serverTime = serverTime;
            this.dbTime = dbTime;
            this.renderTime = renderTime;
        }

        protected String serviceUser;
        protected String timestamp;
        protected String title;
        protected long dbTime;
        protected long serverTime;
        protected long renderTime;
    }
    
    public abstract String getServletVersion();

    public abstract String getDbVersion();

    public abstract String getServerVersion();

    public abstract String getBlikiVersion();

    public abstract long getLastRenderTime();

    public abstract long getLastDbTime();

    public abstract String getLastTitle();

    public abstract String getLastTimestamp();

    public abstract String getLastServiceUser();

    public abstract void resetMonitoringStats();

    public abstract long getLastServerTime();

}