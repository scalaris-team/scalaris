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

import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.query.AbstractJPQLQuery;

/**
 * JPQL query for JSON datastores.
 */
public class JPQLQuery extends AbstractJPQLQuery {
    /**
     * Default serial version....
     */
    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new query instance that uses the given persistence manager.
     * 
     * @param storeMgr
     *            StoreManager for this query
     * @param ec
     *            the associated ExecutionContext for this query.
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec) {
        this(storeMgr, ec, (JPQLQuery) null);
    }

    /**
     * Constructs a new query instance having the same criteria as the given
     * query.
     * 
     * @param storeMgr
     *            StoreManager for this query
     * @param ec
     *            The ExecutionContext
     * @param q
     *            The query from which to copy criteria.
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec, JPQLQuery q) {
        super(storeMgr, ec, q);
    }

    /**
     * Constructor for a JPQL query where the query is specified using the
     * "Single-String" format.
     * 
     * @param storeMgr
     *            StoreManager for this query
     * @param ec
     *            The persistence manager
     * @param query
     *            The query string
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec, String query) {
        super(storeMgr, ec, query);
    }

    protected Object performExecute(@SuppressWarnings("rawtypes") Map parameters) {

        throw new NucleusException("Don't currently support JPQL");
    }
}
