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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.query.evaluator.JavaQueryEvaluator;
import org.datanucleus.query.expression.DyadicExpression;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.query.expression.Expression.Operator;
import org.datanucleus.query.expression.ParameterExpression;
import org.datanucleus.query.expression.PrimaryExpression;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.query.AbstractJDOQLQuery;
import org.datanucleus.store.scalaris.ScalarisPersistenceHandler;

/**
 * JDOQL query for scalaris datastores.
 */
public class JDOQLQuery extends AbstractJDOQLQuery {
    /**
     * Default serial version....
     */
    private static final long serialVersionUID = 1L;

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

    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected Object performExecute(Map parameters) {
        AbstractClassMetaData cmd = ec.getMetaDataManager()
                .getMetaDataForClass(candidateClass,
                        ec.getClassLoaderResolver());

        // get all stored instances of class candidateClass
        Collection candidates;
        if (candidateCollection == null) {

            Expression filterExpr = compilation.getExprFilter();
            String[] memberNameValuePair = getUniqueMemberNameValuePairIfExist(cmd, filterExpr, parameters);
            if (memberNameValuePair == null) {
                // it is not possible to pre-select which objects are needed, therefore
                // all objects must be fetched first
                candidates = ((ScalarisPersistenceHandler) ec.getStoreManager().getPersistenceHandler())
                        .getObjectsOfCandidateType(ec, candidateClass, cmd);
            } else {
                // the member is marked by 'unique' annotation, which
                // means that there can only be at most one object fitting the criteria
                candidates = new ArrayList(1);
                Object uniqueObject = ((ScalarisPersistenceHandler) ec.getStoreManager().getPersistenceHandler())
                        .getObjectByUniqueMember(ec, candidateClass, memberNameValuePair[0], memberNameValuePair[1]);
                if (uniqueObject != null) {
                    candidates.add(uniqueObject);
                }
            }
        } else {
            candidates = new ArrayList<Object>(candidateCollection);
        }

        // execute query
        JavaQueryEvaluator resultMapper = new ScalarisJDOQLEvaluator(this,
                candidateClass, candidates, compilation, parameters,
                ec.getClassLoaderResolver(), ec);
        Collection result = resultMapper.execute(true, true, true, true, true);

        return result;
    }

    /**
     * Checks if the filter expression of the query contains a test for equality of a unique
     * member (marked by 'Unique' annotation) of the candidate class. If this is the case
     * returns the member name and tested value as String array. If there is no such test
     * null is returned.
     * @param candidateClassMD ClassMetaData of the class which is checked for the unique member
     * @param filterExpr The filter expression used in the query (WHERE clause)
     * @param parameters The parameters passed to the query
     * @return new String{memberName, testedMemberValue} if there is such a test, null otherwise
     */
    private String[] getUniqueMemberNameValuePairIfExist(AbstractClassMetaData candidateClassMD,
            Expression filterExpr, Map<?,?> parameters) {
        if (filterExpr == null || !(filterExpr instanceof DyadicExpression)) {
            return null;
        }

        Operator op = filterExpr.getOperator();
        if (op.equals(Expression.OP_AND)) {
            // composite of two filters, check left side first
            String[] leftResult =
                    getUniqueMemberNameValuePairIfExist(candidateClassMD,
                            filterExpr.getLeft(), parameters);
            return leftResult != null? leftResult :
                    getUniqueMemberNameValuePairIfExist(candidateClassMD,
                            filterExpr.getRight(), parameters);

        } else if (op.equals(Expression.OP_EQ)) {
            PrimaryExpression priExpr;
            ParameterExpression paramExpr;
            if (filterExpr.getLeft() instanceof PrimaryExpression 
                    && filterExpr.getRight() instanceof ParameterExpression) {
                priExpr = (PrimaryExpression) filterExpr.getLeft();
                paramExpr = (ParameterExpression) filterExpr.getRight();

            } else if (filterExpr.getRight() instanceof PrimaryExpression
                    && filterExpr.getLeft() instanceof ParameterExpression) {
                priExpr = (PrimaryExpression) filterExpr.getRight();
                paramExpr = (ParameterExpression) filterExpr.getLeft();

            } else {
                return null;
            }

            if (priExpr.getSymbol() != null) {
                String memberFilteredOn = priExpr.getSymbol().getQualifiedName();
                AbstractMemberMetaData mmd = candidateClassMD.getMetaDataForMember(memberFilteredOn);
                if (mmd != null && mmd.getUniqueMetaData() != null) {

                    Object filterValue = parameters.get(paramExpr.getPosition());
                    if (filterValue != null) {
                        return new String[]{memberFilteredOn, filterValue.toString()};
                    }
                }
            }
        }

        return null;
    }
}
