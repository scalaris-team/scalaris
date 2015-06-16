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

Contributors :
2008 Andy Jefferson - refactored JSON specific code to JSONUtils
2008 Andy Jefferson - compilation process
2013 Orange - port to Scalaris key/value store
    ...
 ***********************************************************************/
package org.datanucleus.store.scalaris.query;

import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.query.AbstractJDOQLQuery;

/**
 * JDOQL query for JSON datastores.
 */
public class JDOQLQuery extends AbstractJDOQLQuery {
	/**
	 * Constructs a new query instance that uses the given persistence manager.
	 * 
	 * @param storeMgr
	 *            StoreManager for this query
	 * @param om
	 *            the associated ExecutionContext for this query.
	 */
	public JDOQLQuery(StoreManager storeMgr, ExecutionContext om) {
		this(storeMgr, om, (JDOQLQuery) null);
	}

	/**
	 * Constructs a new query instance having the same criteria as the given
	 * query.
	 * 
	 * @param storeMgr
	 *            StoreManager for this query
	 * @param om
	 *            The ExecutionContext
	 * @param q
	 *            The query from which to copy criteria.
	 */
	public JDOQLQuery(StoreManager storeMgr, ExecutionContext om, JDOQLQuery q) {
		super(storeMgr, om, q);
	}

	/**
	 * Constructor for a JDOQL query where the query is specified using the
	 * "Single-String" format.
	 * 
	 * @param storeMgr
	 *            StoreManager for this query
	 * @param om
	 *            The persistence manager
	 * @param query
	 *            The query string
	 */
	public JDOQLQuery(StoreManager storeMgr, ExecutionContext om, String query) {
		super(storeMgr, om, query);
	}

	protected Object performExecute(Map parameters) {
		throw new NucleusException("Don't currently support JDOQL");

		// ClassLoaderResolver clr = ec.getClassLoaderResolver();
		// final AbstractClassMetaData cmd =
		// ec.getMetaDataManager().getMetaDataForClass(candidateClass, clr);
		// Properties options = new Properties();
		// options.put(ConnectionFactoryImpl.STORE_JSON_URL,
		// ((JsonPersistenceHandler)getStoreManager().getPersistenceHandler()).getURLPathForQuery(cmd));
		// ManagedConnection mconn =
		// getStoreManager().getConnection(ec,options);
		// try
		// {
		// long startTime = System.currentTimeMillis();
		// if (NucleusLogger.QUERY.isDebugEnabled())
		// {
		// NucleusLogger.QUERY.debug(LOCALISER.msg("021046", "JDOQL",
		// getSingleStringQuery(), null));
		// }
		// List candidates = null;
		// if (candidateCollection == null)
		// {
		// candidates =
		// ((JsonPersistenceHandler)getStoreManager().getPersistenceHandler()).getObjectsOfCandidateType(
		// ec, mconn, candidateClass, subclasses, ignoreCache, options);
		// }
		// else
		// {
		// candidates = new ArrayList(candidateCollection);
		// }
		//
		// JavaQueryEvaluator resultMapper = new JDOQLEvaluator(this,
		// candidates, compilation,
		// parameters, ec.getClassLoaderResolver());
		// Collection results = resultMapper.execute(true, true, true, true,
		// true);
		//
		// if (NucleusLogger.QUERY.isDebugEnabled())
		// {
		// NucleusLogger.QUERY.debug(LOCALISER.msg("021074", "JDOQL",
		// "" + (System.currentTimeMillis() - startTime)));
		// }
		//
		// return results;
		// }
		// finally
		// {
		// mconn.release();
		// }
	}
}
