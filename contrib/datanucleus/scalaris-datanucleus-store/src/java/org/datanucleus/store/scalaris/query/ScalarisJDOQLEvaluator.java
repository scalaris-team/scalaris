package org.datanucleus.store.scalaris.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.query.QueryUtils;
import org.datanucleus.query.compiler.QueryCompilation;
import org.datanucleus.query.evaluator.JDOQLEvaluator;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.query.inmemory.InMemoryExpressionEvaluator;
import org.datanucleus.query.inmemory.InMemoryFailure;
import org.datanucleus.query.inmemory.VariableNotSetException;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.scalaris.ScalarisPersistenceHandler;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ScalarisJDOQLEvaluator extends JDOQLEvaluator {

    private Class candidateClass;
    private ExecutionContext ec;

    public ScalarisJDOQLEvaluator(Query query, Class candidateClass, Collection<?> candidates,
            QueryCompilation compilation, Map parameterValues,
            ClassLoaderResolver clr, ExecutionContext ec) {
        super(query, candidates, compilation, parameterValues, clr);
        this.candidateClass = candidateClass;
        this.ec = ec;
    }

    @Override
    public Collection execute(boolean applyFilter, boolean applyOrdering, boolean applyResult, boolean applyResultClass, boolean applyRange) {
        String[] subqueryAliases = compilation.getSubqueryAliases();
        if (subqueryAliases != null) {
            for (int i=0;i<subqueryAliases.length;i++) {
                // Evaluate subquery first
                Query<?> subquery = query.getSubqueryForVariable(subqueryAliases[i]).getQuery();
                QueryCompilation subqueryCompilation =
                    compilation.getCompilationForSubquery(subqueryAliases[i]);
                if (subqueryCompilation.getExprFrom() != null) {
                    // TODO Evaluate "from"
                    NucleusLogger.QUERY.warn("In-memory evaluation of subquery with 'from'=" + 
                        StringUtils.objectArrayToString(subqueryCompilation.getExprFrom()) +
                        " but from clause evaluation not currently supported!");
                }
                Collection<?> subqueryResult = evaluateSubquery(subquery, subqueryCompilation, candidates, null);

                if (QueryUtils.queryReturnsSingleRow(subquery)) {
                    // Subquery is expected to return single value
                    state.put(subqueryAliases[i], subqueryResult.iterator().next());
                } else {
                    state.put(subqueryAliases[i], subqueryResult);
                }
            }
        }

        List resultList = new ArrayList(candidates);
        Expression filter = compilation.getExprFilter();
        if (applyFilter && filter != null) {
            // the super method to handle filters throws an VariableNotSetException when working with sub-queries
            candidates = handleFilter(resultList);
        }
        return super.execute(false, applyOrdering, applyResult, applyResultClass, applyRange);
    }

    // TODO: Execute sub-queries only once
    @Override
    protected Collection evaluateSubquery(Query query, QueryCompilation compilation, Collection candidates,
            Object outerCandidate){
        if (!query.getCandidateClass().equals(candidateClass)) {
            // if the sub-query queries over a different candidate class, all objects of this
            // class must be fetched beforehand
            ManagedConnection mconn = ec.getStoreManager().getConnection(ec);
            AbstractClassMetaData cmd = ec.getMetaDataManager()
                    .getMetaDataForClass(query.getCandidateClass(),ec.getClassLoaderResolver());
            candidates = ((ScalarisPersistenceHandler) ec.getStoreManager().getPersistenceHandler())
                    .getObjectsOfCandidateType(ec, mconn, query.getCandidateClass(), cmd);
        }
        return super.evaluateSubquery(query, compilation, candidates, outerCandidate);
    }

    /*
     * @see org.datanucleus.query.evaluator.JavaQueryEvaluator#handleFilter
     * This is virtually the same method as JavaQueryEvaluator#handleFilter, but it
     * sets the variables of the used InMemoryExpressionEvaluator as needed. This is necessary to
     * prevent an NullPointerException when executing a query with sub-queries
     */
    private List handleFilter(List set) {
        Expression filter = compilation.getExprFilter();
        if (filter == null) {
            return set;
        }

        // Store current results in case we have an aggregate in the filter
        state.put(RESULTS_SET, set);

        List result = new ArrayList();
        Iterator it = set.iterator();
        if (NucleusLogger.QUERY.isDebugEnabled()) {
            NucleusLogger.QUERY.debug("Evaluating filter for " + set.size() + " candidates");
        }

        while (it.hasNext()) {
            // Set the value of the candidate being tested, and evaluate it
            Object obj = it.next();
            if (!state.containsKey(candidateAlias)) {
                throw new NucleusUserException("Alias \"" + candidateAlias + "\" doesn't exist in the query or the candidate alias wasn't defined");
            }
            state.put(candidateAlias, obj);

            InMemoryExpressionEvaluator eval = new InMemoryExpressionEvaluator(query.getExecutionContext(), 
                    parameterValues, state, query.getParsedImports(), clr, candidateAlias, query.getLanguage());

            for (String stateKey : state.keySet()) {
                eval.setVariableValue(stateKey, state.get(stateKey));
            }

            Object evalResult = evaluateBooleanExpression(filter, eval);
            if (Boolean.TRUE.equals(evalResult)) {
                if (NucleusLogger.QUERY.isDebugEnabled()) {
                    NucleusLogger.QUERY.debug(Localiser.msg("021023", StringUtils.toJVMIDString(obj)));
                }
                result.add(obj);
            }
        }
        return result;
    }

    /* 
     * @see org.datanucleus.query.evaluator.JavaQueryEvaluator#evaluateBooleanExpression
     * This is an exact copy JavaQueryEvaluator#evaluateBooleanExpression, since it is private
     * but it is needed here because of the changed implementation of handleFilter. (This is ugly)
     */
    private Boolean evaluateBooleanExpression(Expression expr, InMemoryExpressionEvaluator eval) {
        try {
            Object result = expr.evaluate(eval);
            return ((result instanceof InMemoryFailure) ? Boolean.FALSE : (Boolean)result);
        }
        catch (VariableNotSetException vnse) {
            if (NucleusLogger.QUERY.isDebugEnabled()) {
                NucleusLogger.QUERY.debug(Localiser.msg("021024", vnse.getVariableExpression().getId(), 
                        StringUtils.objectArrayToString(vnse.getValues())));
            }

            if (vnse.getValues() == null || vnse.getValues().length == 0) {
                // No values available for this variable, so just put null and see the result
                eval.setVariableValue(vnse.getVariableExpression().getId(), null);
                if (NucleusLogger.QUERY.isDebugEnabled()) {
                    NucleusLogger.QUERY.debug(Localiser.msg("021025", vnse.getVariableExpression().getId(), "(null)"));
                }
                if (Boolean.TRUE.equals(evaluateBooleanExpression(expr, eval))) {
                    return Boolean.TRUE;
                }
            } else {
                // Set this variable and start iteration over the possible variable values
                for (int i=0;i<vnse.getValues().length;i++) {
                    eval.setVariableValue(vnse.getVariableExpression().getId(), vnse.getValues()[i]);
                    if (NucleusLogger.QUERY.isDebugEnabled()) {
                        NucleusLogger.QUERY.debug(Localiser.msg("021025", vnse.getVariableExpression().getId(), vnse.getValues()[i]));
                    }
                    if (Boolean.TRUE.equals(evaluateBooleanExpression(expr, eval))) {
                        return Boolean.TRUE;
                    }
                }
            }

            // No variable value was successful so return FALSE
            if (NucleusLogger.QUERY.isDebugEnabled()) {
                NucleusLogger.QUERY.debug(Localiser.msg("021026", vnse.getVariableExpression().getId()));
            }
            eval.removeVariableValue(vnse.getVariableExpression().getId());
            return Boolean.FALSE;
        }
    }
}