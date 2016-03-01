/**********************************************************************
Copyright (c) 2008 Erik Bengtson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
2013 Orange - port to Scalaris key/value store
    ...
 **********************************************************************/
package org.datanucleus.store.scalaris;

import java.util.Map;
import java.util.Properties;

import javax.transaction.xa.XAResource;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ManagedConnection;

import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.ConnectionFactory;
import de.zib.scalaris.ConnectionPool;

/**
 * Implementation of a ConnectionFactory for Scalaris.
 */
@SuppressWarnings("rawtypes")
public class ConnectionFactoryImpl extends AbstractConnectionFactory {

    /**
     * Maximum number of open connections at the same time.
     */
    private static final int DEFAULT_MAX_CONNECTIONS = 50;
    public static final String PROPERTY_MAX_CONNECTIONS = "scalaris.connection.max";

    /**
     * Period of time (in ms) which will be waited for a connection
     * to get released if maximum number of connections is reached.
     */
    private static final int DEFAULT_CONNECTION_TIMEOUT = 200;
    public static final String PROPERTY_CONNECTION_TIMEOUT = "scalaris.connection.timeout";
    private static int newConnectionTimeout;


    /**
     * Symbolic Name of property used in persistence-unit configuration file.
     * Property value Cookie used for connecting to Scalaris node. <property
     * name="..." value="..." />
     */
    public final static String PROPERTY_SCALARIS_COOKIE = "scalaris.cookie";
    /**
     * Symbolic Name of property used in persistence-unit configuration file.
     * Should be the same as defined in Scalaris server <property name="..."
     * value="..." />
     */
    public final static String PROPERTY_SCALARIS_DEBUG = "scalaris.debug";
    /**
     * Symbolic Name of property used in persistence-unit configuration file
     * <property name="..." value="..." />
     */
    public final static String PROPERTY_SCALARIS_NODE = "scalaris.node";
    /**
     * Symbolic Name of property used in persistence-unit configuration file
     * <property name="..." value="..." />
     */
    public final static String PROPERTY_SCALARIS_CLIENT_NAME = "scalaris.client.name";
    /**
     * Symbolic Name of property used in persistence-unit configuration file
     * <property name="..." value="..." />
     */
    public final static String PROPERTY_SCALARIS_CLIENT_APPENDUUID = "scalaris.client.appendUUID";

    private static volatile ConnectionPool connPool = null;

    /**
     * Constructor.
     * 
     * @param storeMgr
     *            Store Manager
     * @param resourceType
     *            Type of resource (tx, nontx)
     */
    public ConnectionFactoryImpl(final StoreManager storeMgr,
            final String resourceType) {
        super(storeMgr, resourceType);

        initConnectionPool();
    }

    private void initConnectionPool() {
        synchronized(ConnectionFactoryImpl.class) {
            if (connPool == null) {
                Properties properties = new Properties();
                copyProperty(PROPERTY_SCALARIS_NODE, properties, "scalaris.node",
                        (String) null);
                copyProperty(PROPERTY_SCALARIS_COOKIE, properties,
                        "scalaris.cookie", (String) null);
                copyProperty(PROPERTY_SCALARIS_CLIENT_APPENDUUID, properties,
                        "scalaris.client.appendUUID", true);
                ConnectionFactory connectionFactory = new ConnectionFactory(properties);

                int maxConnectionNum = getIntProperty(PROPERTY_MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS);
                newConnectionTimeout = getIntProperty(PROPERTY_CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);

                connPool = new ConnectionPool(connectionFactory, maxConnectionNum);
            }
        }
    }

    /**
     * Helper for reading a integer property with default value
     * @return
     */
    int getIntProperty(String propertyName, int defaultValue) {
        String stringVal = storeMgr.getStringProperty(propertyName);
        return stringVal == null ? defaultValue : Integer.parseInt(stringVal);
    }

    /**
     * Helper for copying setting from Datanucleus to Scalaris
     * 
     * @return
     * */
    String copyProperty(final String propertyNameFrom,
            final Properties propertiesTo, final String propertyNameTo,
            final String defaultIfNotSet) {
        final String v = storeMgr.getStringProperty(propertyNameFrom);
        if (defaultIfNotSet == null && v == null) {
            throw new NucleusDataStoreException("Property "
                    + propertyNameFrom + " is mandatory");
        }
        final String ret = v == null ? defaultIfNotSet : v;
        propertiesTo.put(propertyNameTo, ret);
        return ret;
    }

    /**
     * Helper for copying setting from Datanucleus to Scalaris
     * 
     * @return
     * */
    boolean copyProperty(final String propertyNameFrom,
            final Properties propertiesTo, final String propertyNameTo,
            final boolean defaultIfNotSet) {
        final boolean ret = storeMgr.getBooleanProperty(propertyNameFrom,
                defaultIfNotSet);
        if (propertyNameTo != null)
            propertiesTo.put(propertyNameTo, ret);
        return ret;
    }
    
    /**
     * Obtain a connection from the Factory. The connection will be enlisted
     * within the transaction associated to the ExecutionContext
     * 
     * @param ec
     *            the pool that is bound the connection during its lifecycle (or
     *            null)
     * @param txnOptions
     *            Any options for creating the connection
     * @return the {@link org.datanucleus.store.connection.ManagedConnection}
     */
    public ManagedConnection createManagedConnection(final ExecutionContext ec,
            final Map txnOptions) {
        return new ManagedConnectionImpl(txnOptions);
    }

    /**
     * Implementation of a ManagedConnection for Scalaris database.
     */
    public static class ManagedConnectionImpl extends AbstractManagedConnection {

        public ManagedConnectionImpl(final Map optionsl) {
            // TODO: handle options?
        }

        public synchronized Object getConnection() {
            try {
                if (conn == null) {
                    conn = connPool.getConnection(newConnectionTimeout);
                    if (conn == null) {
                        throw new ConnectionException("Timeout when waiting for a new connection ");
                    }
                }
            } catch (ConnectionException e) {
                throw new NucleusDataStoreException("Could not create a connection", e);
            }
            return conn;
        }

        public XAResource getXAResource() {
            return null;
        }

        public synchronized void close() {
            if (conn != null) {
                connPool.releaseConnection((de.zib.scalaris.Connection) conn);
                conn = null;
            }
        }

        @Override
        public boolean closeAfterTransactionEnd() {
            // Do NOT close immediately after datanucleus transaction ends,
            // since the corresponding scalaris transaction can be still
            // active for a short period of time, causing the connection pool
            // to release the connection a little bit to early - early enough
            // so that this connection could be reused in rare cases in a
            // different thread. This causes potentially UnknownExceptions
            // since Connection objects are not thread safe.
            return false;
        }

        @Override
        public void setCloseOnRelease(boolean closeOnRelease) {
            // Because of the override of closeAfterTransactionEnd
            // Datanucleus tries to set this to false. But this value
            // needs to be true, otherwise close() won't be called and
            // the connection pool will never release any connections.
            this.closeOnRelease = true;
        }
    }
}
