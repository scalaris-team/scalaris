/**********************************************************************
Copyright (c) 2008 Andy Jefferson and others. All rights reserved.
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
2013 Orange - port to Scalaris key/value store
    ...
***********************************************************************/
package org.datanucleus.store.scalaris.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.query.evaluator.JPQLEvaluator;
import org.datanucleus.query.evaluator.JavaQueryEvaluator;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.scalaris.ConnectionFactoryImpl;
import org.datanucleus.store.scalaris.ScalarisPersistenceHandler;
import org.datanucleus.store.query.AbstractJPQLQuery;
import org.datanucleus.util.NucleusLogger;

/**
 * JPQL query for JSON datastores.
 */
public class JPQLQuery extends AbstractJPQLQuery
{
    /**
     * Constructs a new query instance that uses the given persistence manager.
     * @param storeMgr StoreManager for this query
     * @param ec the associated ExecutionContext for this query.
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec)
    {
        this(storeMgr, ec, (JPQLQuery) null);
    }

    /**
     * Constructs a new query instance having the same criteria as the given query.
     * @param storeMgr StoreManager for this query
     * @param ec The ExecutionContext
     * @param q The query from which to copy criteria.
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec, JPQLQuery q)
    {
        super(storeMgr, ec, q);
    }

    /**
     * Constructor for a JPQL query where the query is specified using the "Single-String" format.
     * @param storeMgr StoreManager for this query
     * @param ec The persistence manager
     * @param query The query string
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec, String query)
    {
        super(storeMgr, ec, query);
    }

    protected Object performExecute(Map parameters)
    {
    	
		throw new NucleusException("Don't currently support JPQL");
//        ClassLoaderResolver clr = ec.getClassLoaderResolver();
//        final AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(candidateClass, clr);
//        Properties options = new Properties();
//        options.put(ConnectionFactoryImpl.STORE_JSON_URL, ((ScalarisPersistenceHandler)getStoreManager().getPersistenceHandler()).getURLPathForQuery(cmd));
//        ManagedConnection mconn = getStoreManager().getConnection(ec,options);
//        try
//        {
//            long startTime = System.currentTimeMillis();
//            if (NucleusLogger.QUERY.isDebugEnabled())
//            {
//                NucleusLogger.QUERY.debug(LOCALISER.msg("021046", "JPQL", getSingleStringQuery(), null));
//            }
//            List candidates = null;
//            if (candidateCollection == null)
//            {
//                // TODO Cater for "subclasses" flag
//                candidates = ((ScalarisPersistenceHandler)getStoreManager().getPersistenceHandler()).getObjectsOfCandidateType(
//                    ec, mconn, candidateClass, subclasses, ignoreCache, options);
//            }
//            else
//            {
//                candidates = new ArrayList(candidateCollection);
//            }
//
//            JavaQueryEvaluator resultMapper = new JPQLEvaluator(this, candidates, compilation, 
//                parameters, ec.getClassLoaderResolver());
//            Collection results = resultMapper.execute(true, true, true, true, true);
//
//            if (NucleusLogger.QUERY.isDebugEnabled())
//            {
//                NucleusLogger.QUERY.debug(LOCALISER.msg("021074", "JPQL", 
//                    "" + (System.currentTimeMillis() - startTime)));
//            }
//
//            if (type == BULK_DELETE)
//            {
//                ec.deleteObjects(results.toArray());
//                return Long.valueOf(results.size());
//            }
//            else if (type == BULK_UPDATE)
//            {
//                throw new NucleusException("Bulk Update is not yet supported");
//            }
//            else
//            {
//                return results;
//            }
//        }
//        finally
//        {
//            mconn.release();
//        }
    }
}
