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

import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

import javax.transaction.xa.XAResource;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ManagedConnection;

import de.zib.scalaris.AbortException;
import de.zib.scalaris.Connection;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.ConnectionFactory;
import de.zib.scalaris.TimeoutException;
import de.zib.scalaris.Transaction;
import de.zib.scalaris.UnknownException;

/**
 * Implementation of a ConnectionFactory for Scalaris. The connections are only
 * created and they are not managed. All operations besides getConnection are
 * no-op.
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory {
	/**
	 * Symbolic Name of property used in persistence-unit configuration file.
	 * Property value Cookie used for connecting to Scalaris node. <property
	 * name="..." value="..." />
	 */
	public final String PROPERTY_SCALARIS_COOKIE = "scalaris.cookie";
	/**
	 * Symbolic Name of property used in persistence-unit configuration file.
	 * Should be the same as defined in Scalaris server <property name="..."
	 * value="..." />
	 */
	public final String PROPERTY_SCALARIS_DEBUG = "scalaris.debug";
	/**
	 * Symbolic Name of property used in persistence-unit configuration file
	 * <property name="..." value="..." />
	 */
	public final String PROPERTY_SCALARIS_NODE = "scalaris.node";
	/**
	 * Symbolic Name of property used in persistence-unit configuration file
	 * <property name="..." value="..." />
	 */
	public final String PROPERTY_SCALARIS_CLIENT_NAME = "scalaris.client.name";
	/**
	 * Symbolic Name of property used in persistence-unit configuration file
	 * <property name="..." value="..." />
	 */
	public final String PROPERTY_SCALARIS_CLIENT_APPENDUUID = "scalaris.client.appendUUID";

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
	}
	
	/**
	 * A Java Instance can only create one connection per scalaris node. We need
	 * to store already created connection and reuse if necessary.
	 * 
	 * This need to be thread safe, hence volatile.
	 */
	private static final Hashtable<String, Connection> static_connectionSingletons = new Hashtable<String, Connection>();
	
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
		return new ManagedConnectionImpl(txnOptions, static_connectionSingletons);
	}

	

	/**
	 * Implementation of a ManagedConnection for Scalaris database.
	 */
	public class ManagedConnectionImpl extends AbstractManagedConnection {

		final private Map options;
		final private  Hashtable<String, Connection> connectionSingletons;

		public ManagedConnectionImpl(final Map options,
				final Hashtable<String, Connection> _connectionSingletons) {
			this.options = options;
			this.connectionSingletons = _connectionSingletons;
		}

		public void close() {
			// TODO Auto-generated method stub
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

		public Object getConnection() {

			Properties properties = new Properties();
			copyProperty(PROPERTY_SCALARIS_NODE, properties,
					"scalaris.node", (String) null);
			copyProperty(PROPERTY_SCALARIS_COOKIE, properties,
					"scalaris.cookie", (String) null);
			final String cname = copyProperty(PROPERTY_SCALARIS_CLIENT_NAME,
					properties, "scalaris.client.name", "java_client");
			copyProperty(PROPERTY_SCALARIS_CLIENT_APPENDUUID, properties,
					"scalaris.client.appendUUID", true);
			final boolean debug = copyProperty(PROPERTY_SCALARIS_DEBUG,
					properties, null, false);

			{
				Connection c = connectionSingletons.get(cname);
				if (c != null)
					return c;
			}
			System.out.println(properties.toString());

			final Connection connection;
			synchronized (connectionSingletons) {

				{// In case someone overruns me to the synchronized section
					Connection c = connectionSingletons.get(cname);
					if (c != null) {
						System.out.println("Ouch. I was overrun");
						return c;
						
					}
				}
					
				{// I am the first one.
					System.out.println("I am the first for "+cname);
					try {
						ConnectionFactory connectionFactory = new ConnectionFactory(
								properties);
						
						connectionFactory.createConnection();
						connection = connectionFactory.createConnection();
						connectionSingletons.put(cname,connection);
					} catch (ConnectionException e1) {
						throw new NucleusDataStoreException(
								"An error occured while creating scalaris connection",
								e1);
					}
				}

			}

			// urlStr = urlStr.substring(urlStr.indexOf(storeMgr
			// .getStoreManagerKey() + ":")
			// + storeMgr.getStoreManagerKey().length() + 1);
			// if (options.containsKey(STORE_JSON_URL)) {
			// if (urlStr.endsWith("/")
			// && options.get(STORE_JSON_URL).toString()
			// .startsWith("/")) {
			// urlStr += options.get(STORE_JSON_URL).toString()
			// .substring(1);
			// } else if (!urlStr.endsWith("/")
			// && !options.get(STORE_JSON_URL).toString()
			// .startsWith("/")) {
			// urlStr += "/" + options.get(STORE_JSON_URL).toString();
			// } else {
			// urlStr += options.get(STORE_JSON_URL).toString();
			// }
			// }
			// URL url;
			// try {
			// url = new URL(urlStr);
			// return url.openConnection();
			// } catch (MalformedURLException e) {
			// throw new NucleusDataStoreException(e.getMessage(), e);
			// } catch (IOException e) {
			// throw new NucleusDataStoreException(e.getMessage(), e);
			// }
			//

			if (debug) {
				// TODO: test for early development. to be removed someday.
				Transaction t1 = new Transaction(connection); // Transaction()

				try {
					t1.write("datanucleus", "connected at "
							+ new java.util.Date() + " from java_client="
							+ cname);
					t1.commit();
				} catch (ConnectionException e) {
					throw new NucleusDataStoreException(
							"an error occured in debug self test ", e);

//				} catch (TimeoutException e) {
//					throw new NucleusDataStoreException(
//							"an error occured in debug self test ", e);
				} catch (AbortException e) {
					throw new NucleusDataStoreException(
							"an error occured in debug self test ", e);
				} catch (UnknownException e) {
					throw new NucleusDataStoreException(
							"an error occured in debug self test ", e);
				}
			}

			return connection;

		}

		public XAResource getXAResource() {
			return null;
		}
	}
}

class ConnectorSingleton {
	/** Constructeur privé */
	private ConnectorSingleton() {
	}

	/** Holder */
	private static class SingletonHolder {
		/** Instance unique non préinitialisée */
		private final static ConnectorSingleton instance = new ConnectorSingleton();
	}

	/** Point d'accès pour l'instance unique du singleton */
	public static ConnectorSingleton getInstance() {
		return SingletonHolder.instance;
	}
}
