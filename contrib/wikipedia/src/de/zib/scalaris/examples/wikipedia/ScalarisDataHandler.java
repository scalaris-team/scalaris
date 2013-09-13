/**
 *  Copyright 2011-2013 Zuse Institute Berlin
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package de.zib.scalaris.examples.wikipedia;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import com.ericsson.otp.erlang.OtpErlangString;

import de.zib.scalaris.Connection;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.ErlangValue;
import de.zib.scalaris.ErlangValue.ListElementConverter;
import de.zib.scalaris.ScalarisVM;
import de.zib.scalaris.TransactionSingleOp;
import de.zib.scalaris.UnknownException;
import de.zib.scalaris.examples.wikipedia.Options.Optimisation;
import de.zib.scalaris.examples.wikipedia.Options.STORE_CONTRIB_TYPE;
import de.zib.scalaris.examples.wikipedia.bliki.MyNamespace.NamespaceEnum;
import de.zib.scalaris.examples.wikipedia.bliki.NormalisedTitle;
import de.zib.scalaris.examples.wikipedia.data.Contribution;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;
import de.zib.scalaris.operations.Operation;
import de.zib.scalaris.operations.PartialReadOp;
import de.zib.scalaris.operations.ReadOp;

/**
 * Retrieves and writes values from/to Scalaris.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class ScalarisDataHandler {
    
    /**
     * Gets the key to store {@link SiteInfo} objects at.
     * 
     * @return Scalaris key
     */
    public final static String getSiteInfoKey() {
        return "siteinfo";
    }
    
    /**
     * Gets the key to store the list of pages in the given namespace at.
     * 
     * @param namespace  the namespace ID
     * 
     * @return Scalaris key
     */
    public final static String getPageListKey(int namespace) {
        return "pages:" + namespace;
    }
    
    /**
     * Gets the key to store the number of pages at.
     * 
     * @param namespace  the namespace ID
     * 
     * @return Scalaris key
     */
    public final static String getPageCountKey(int namespace) {
        return getPageListKey(namespace) + ":count";
    }
    
    /**
     * Gets the key to store the number of articles, i.e. pages in the main
     * namespace, at.
     * 
     * @return Scalaris key
     */
    public final static String getArticleCountKey() {
        return "articles:count";
    }
    
    /**
     * Gets the key to store the number of page edits.
     * 
     * @return Scalaris key
     */
    public final static String getStatsPageEditsKey() {
        return "stats:pageedits";
    }
    
    /**
     * Gets the key to store the list of contributions of a user.
     * 
     * @param contributor  the user name or IP address of the user who created
     *                     the revision
     * 
     * @return Scalaris key
     */
    public final static String getContributionListKey(String contributor) {
        return contributor + ":user:contrib";
    }

    /**
     * Retrieves the Scalaris version string.
     * 
     * @param connection
     *            the connection to the DB
     * 
     * @return a result object with the version string on success
     */
    public final static ValueResult<String> getDbVersion(Connection connection) {
        final long timeAtStart = System.currentTimeMillis();
        final String statName = "SCALARIS_VERSION";
        List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
        if (connection == null) {
            return new ValueResult<String>(false, involvedKeys,
                    "no connection to Scalaris", true, statName,
                    System.currentTimeMillis() - timeAtStart);
        }

        String node = connection.getRemote().toString();
        try {
            ScalarisVM scalarisVm = new ScalarisVM(node);
            String version = scalarisVm.getVersion();
            return new ValueResult<String>(involvedKeys, version, statName,
                    System.currentTimeMillis() - timeAtStart);
        } catch (ConnectionException e) {
            return new ValueResult<String>(false, involvedKeys,
                    "no connection to Scalaris node \"" + node + "\"", true,
                    statName, System.currentTimeMillis() - timeAtStart);
        } catch (UnknownException e) {
            return new ValueResult<String>(false, involvedKeys,
                    e.getClass().getCanonicalName() + " reading Scalaris version from node \""
                            + node + "\"", true, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
    }

    /**
     * Retrieves a list of all available pages from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * 
     * @return a result object with the page list on success
     */
    public final static ValueResult<List<NormalisedTitle>> getPageList(Connection connection) {
        final long timeAtStart = System.currentTimeMillis();
        ArrayList<String> scalaris_keys = new ArrayList<String>(NamespaceEnum.values().length);
        for (NamespaceEnum ns : NamespaceEnum.values()) {
            scalaris_keys.add(getPageListKey(ns.getId()));
        }
        return getPageList2(connection, ScalarisOpType.PAGE_LIST,
                scalaris_keys, false, timeAtStart, "page list");
    }

    /**
     * Retrieves a list of available pages in the given namespace from Scalaris.
     * 
     * @param namespace
     *            the namespace ID
     * @param connection
     *            the connection to Scalaris
     * 
     * @return a result object with the page list on success
     */
    public final static ValueResult<List<NormalisedTitle>> getPageList(int namespace, Connection connection) {
        final long timeAtStart = System.currentTimeMillis();
        return getPageList2(connection, ScalarisOpType.PAGE_LIST,
                Arrays.asList(getPageListKey(namespace)), false, timeAtStart,
                "page list:" + namespace);
    }

    /**
     * Retrieves a list of pages linking to the given page from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param contributor
     *            the user name or IP address of the user who created the
     *            revision
     * 
     * @return a result object with the page list on success
     */
    public final static ValueResult<List<Contribution>> getContributions(
            Connection connection, String contributor) {
        final long timeAtStart = System.currentTimeMillis();
        final String statName = "CONTRIB:" + contributor;
        if (Options.getInstance().WIKI_STORE_CONTRIBUTIONS != STORE_CONTRIB_TYPE.NONE) {
            ValueResult<List<Contribution>> result = getPageList3(connection,
                    ScalarisOpType.CONTRIBUTION,
                    Arrays.asList(getContributionListKey(contributor)), false,
                    timeAtStart, statName,
                    new ErlangConverter<List<Contribution>>() {
                        @Override
                        public List<Contribution> convert(ErlangValue v)
                                throws ClassCastException {
                            return v.jsonListValue(Contribution.class);
                        }
                    },
                    new ErlangConverter<Contribution>() {
                        @Override
                        public Contribution convert(ErlangValue v)
                                throws ClassCastException {
                            return v.jsonValue(Contribution.class);
                        }
                    });
            if (result.success && result.value == null) {
                result.value = new ArrayList<Contribution>(0);
            }
            return result;
        } else {
            return new ValueResult<List<Contribution>>(
                    new ArrayList<InvolvedKey>(0), new ArrayList<Contribution>(0));
        }
    }

    /**
     * Retrieves a list of pages from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param opType
     *            operation type indicating what is being read
     * @param scalaris_keys
     *            the keys under which the page list is stored in Scalaris
     * @param failNotFound
     *            whether the operation should fail if the key is not found or
     *            not
     * @param timeAtStart
     *            the start time of the method using this method
     * @param statName
     *            name for the time measurement statistics
     * 
     * @return a result object with the page list on success
     */
    protected final static ValueResult<List<NormalisedTitle>> getPageList2(
            Connection connection, ScalarisOpType opType,
            Collection<String> scalaris_keys, boolean failNotFound,
            final long timeAtStart, String statName) {
        ValueResult<List<NormalisedTitle>> result = getPageList3(connection,
                opType, scalaris_keys, failNotFound, timeAtStart, statName,
                new ErlangConverter<List<NormalisedTitle>>() {
                    @Override
                    public List<NormalisedTitle> convert(ErlangValue v)
                            throws ClassCastException {
                        return v.listValue(new ListElementConverter<NormalisedTitle>() {
                            public NormalisedTitle convert(final int i,
                                    final ErlangValue v) {
                                return NormalisedTitle.fromNormalised(v
                                        .stringValue());
                            }
                        });
                    }
                },
                new ErlangConverter<NormalisedTitle>() {
                    @Override
                    public NormalisedTitle convert(ErlangValue v)
                            throws ClassCastException {
                        return NormalisedTitle.fromNormalised(v
                                        .stringValue());
                    }
                });
        if (result.success && result.value == null) {
            result.value = new ArrayList<NormalisedTitle>(0);
        }
        return result;
    }

    /**
     * Retrieves a list of pages from Scalaris.
     * 
     * @param <T>
     *            list type
     * 
     * @param connection
     *            the connection to Scalaris
     * @param opType
     *            operation type indicating what is being read
     * @param scalaris_keys
     *            the keys under which the page list is stored in Scalaris
     * @param failNotFound
     *            whether the operation should fail if no key is found (in which
     *            case the value is <tt>null</tt>) or not
     * @param timeAtStart
     *            the start time of the method using this method
     * @param statName
     *            name for the time measurement statistics
     * @param listConv
     *            converter to make an {@link ErlangValue} to a {@link List} of
     *            <tt>T</tt>
     * @param elemConv
     *            converter to make an {@link ErlangValue} to a <tt>T</tt>
     * 
     * @return a result object with the page list on success
     */
    protected final static <T> ValueResult<List<T>> getPageList3(
            Connection connection, ScalarisOpType opType,
            Collection<String> scalaris_keys, boolean failNotFound,
            final long timeAtStart, String statName,
            ErlangConverter<List<T>> listConv, ErlangConverter<T> elemConv) {
        List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
        
        if (connection == null) {
            return new ValueResult<List<T>>(false, involvedKeys,
                    "no connection to Scalaris", true, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        
        final MyScalarisSingleOpExecutor executor = new MyScalarisSingleOpExecutor(
                new TransactionSingleOp(connection), involvedKeys);

        final ScalarisReadListOp1<T> readOp = new ScalarisReadListOp1<T>(
                scalaris_keys, Options.getInstance().OPTIMISATIONS.get(opType),
                listConv, elemConv, failNotFound);
        executor.addOp(readOp);
        try {
            executor.run();
        } catch (Exception e) {
            return new ValueResult<List<T>>(false, involvedKeys,
                    e.getClass().getCanonicalName() + " reading page list at \""
                            + involvedKeys.toString() + "\" from Scalaris: "
                            + e.getMessage(), e instanceof ConnectionException,
                    statName, System.currentTimeMillis() - timeAtStart);
        }
        
        return new ValueResult<List<T>>(involvedKeys, readOp.getValue(), statName,
                System.currentTimeMillis() - timeAtStart);
    }

    /**
     * Retrieves the number of all available pages from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * 
     * @return a result object with the number of pages on success
     */
    public final static ValueResult<BigInteger> getPageCount(Connection connection) {
        final long timeAtStart = System.currentTimeMillis();
        ArrayList<String> scalaris_keys = new ArrayList<String>(NamespaceEnum.values().length);
        for (NamespaceEnum ns : NamespaceEnum.values()) {
            scalaris_keys.add(getPageCountKey(ns.getId()));
        }
        final String statName = "PAGE_COUNT";
        return getInteger2(connection, ScalarisOpType.PAGE_COUNT, scalaris_keys,
                false, timeAtStart, statName);
    }

    /**
     * Retrieves the number of available pages in the given namespace from
     * Scalaris.
     * 
     * @param namespace
     *            the namespace ID
     * @param connection
     *            the connection to Scalaris
     * 
     * @return a result object with the number of pages on success
     */
    public final static ValueResult<BigInteger> getPageCount(int namespace, Connection connection) {
        final long timeAtStart = System.currentTimeMillis();
        final String statName = "PAGE_COUNT:" + namespace;
        return getInteger2(connection, ScalarisOpType.PAGE_COUNT,
                getPageCountKey(namespace), false, timeAtStart, statName);
    }

    /**
     * Retrieves the number of available articles, i.e. pages in the main
     * namespace, from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * 
     * @return a result object with the number of articles on success
     */
    public final static ValueResult<BigInteger> getArticleCount(Connection connection) {
        final long timeAtStart = System.currentTimeMillis();
        final String statName = "ARTICLE_COUNT";
        return getInteger2(connection, ScalarisOpType.ARTICLE_COUNT,
                getArticleCountKey(), false, timeAtStart, statName);
    }

    /**
     * Retrieves the number of available articles, i.e. pages in the main
     * namespace, from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * 
     * @return a result object with the number of articles on success
     */
    public final static ValueResult<BigInteger> getStatsPageEdits(Connection connection) {
        final long timeAtStart = System.currentTimeMillis();
        final String statName = "PAGE_EDITS";
        return getInteger2(connection, ScalarisOpType.EDIT_STAT,
                getStatsPageEditsKey(), false, timeAtStart, statName);
    }

    /**
     * Retrieves a random page title from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param random
     *            the random number generator to use
     * 
     * @return a result object with the page list on success
     */
    public final static ValueResult<NormalisedTitle> getRandomArticle(Connection connection, Random random) {
        final long timeAtStart = System.currentTimeMillis();
        final String statName = "RANDOM_PAGE";
        
        final Optimisation optimisation = Options.getInstance().OPTIMISATIONS.get(ScalarisOpType.PAGE_LIST);
        final ErlangConverter<List<ErlangValue>> listConv = new ErlangConverter<List<ErlangValue>>() {
            @Override
            public List<ErlangValue> convert(ErlangValue v)
                    throws ClassCastException {
                return v.listValue();
            }
        };
        final ErlangConverter<ErlangValue> elemConv = new ErlangConverter<ErlangValue>() {
            @Override
            public ErlangValue convert(ErlangValue v) {
                return v;
            }
        };
        final List<String> scalarisKeys = Arrays.asList(getPageListKey(NamespaceEnum.MAIN_NAMESPACE_KEY.getId()));
        List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
        
        if (connection == null) {
            return new ValueResult<NormalisedTitle>(false, involvedKeys,
                    "no connection to Scalaris", true, statName,
                    System.currentTimeMillis() - timeAtStart);
        }

        // first try reading from one bucket only, if it fails, try to read from all
        for (boolean readOnlyOneBucket : new boolean[] {true, false}) {
            final MyScalarisSingleOpExecutor executor = new MyScalarisSingleOpExecutor(
                    new TransactionSingleOp(connection), involvedKeys);

            final ScalarisReadRandomListEntryOp1<ErlangValue> readOp = new ScalarisReadRandomListEntryOp1<ErlangValue>(
                    scalarisKeys, optimisation, readOnlyOneBucket, elemConv,
                    listConv, false, random);
            executor.addOp(readOp);
            try {
                executor.run();
            } catch (Exception e) {
                return new ValueResult<NormalisedTitle>(false, involvedKeys,
                        e.getClass().getCanonicalName() + " reading page list at \""
                                + involvedKeys.toString() + "\" from Scalaris: "
                                + e.getMessage(), e instanceof ConnectionException,
                        statName, System.currentTimeMillis() - timeAtStart);
            }
            
            // return if successful, otherwise fall back and read the whole list
            // as with no optimisation
            if (readOp.getValue() != null) {
                return new ValueResult<NormalisedTitle>(involvedKeys,
                        NormalisedTitle.fromNormalised(readOp.getValue()
                                .stringValue()), statName,
                        System.currentTimeMillis() - timeAtStart);
            }
        }
        return new ValueResult<NormalisedTitle>(false, involvedKeys,
                "unable to retrieve random page (no articles?)", false,
                statName, System.currentTimeMillis() - timeAtStart);
    }

    /**
     * Retrieves an integral number from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param scalaris_key
     *            the key under which the number is stored in Scalaris
     * @param failNotFound
     *            whether the operation should fail if the key is not found or
     *            not
     * @param timeAtStart
     *            the start time of the method using this method
     * @param statName
     *            name for the time measurement statistics
     * 
     * @return a result object with the number on success
     */
    protected final static ValueResult<BigInteger> getInteger2(
            Connection connection, ScalarisOpType opType, String scalaris_key,
            boolean failNotFound, final long timeAtStart, String statName) {
        return getInteger2(connection, opType, Arrays.asList(scalaris_key),
                failNotFound, timeAtStart, statName);
    }

    /**
     * Retrieves an integral number from Scalaris.
     * 
     * @param connection
     *            the connection to Scalaris
     * @param opType
     *            operation type indicating what is being read
     * @param scalaris_keys
     *            the keys under which the number is stored in Scalaris
     * @param failNotFound
     *            whether the operation should fail if the key is not found or
     *            not
     * @param timeAtStart
     *            the start time of the method using this method
     * @param statName
     *            name for the time measurement statistics
     * 
     * @return a result object with the number on success
     */
    protected final static ValueResult<BigInteger> getInteger2(
            Connection connection, ScalarisOpType opType,
            Collection<String> scalaris_keys, boolean failNotFound,
            final long timeAtStart, String statName) {
        List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
        if (connection == null) {
            return new ValueResult<BigInteger>(false, involvedKeys,
                    "no connection to Scalaris", true, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        
        final MyScalarisSingleOpExecutor executor = new MyScalarisSingleOpExecutor(
                new TransactionSingleOp(connection), involvedKeys);

        Optimisation optimisation = Options.getInstance().OPTIMISATIONS.get(opType);
        if (optimisation == null) {
            switch (opType) {
                case PAGE_COUNT:
                    // fall back to PAGE_LIST optimisation as in this case, the counter
                    // follows the partitions
                    optimisation = Options.getInstance().OPTIMISATIONS.get(ScalarisOpType.PAGE_LIST);
                    break;
                case CATEGORY_PAGE_COUNT:
                    // fall back to CATEGORY_PAGE_LIST optimisation as in this case, the counter
                    // follows the partitions
                    optimisation = Options.getInstance().OPTIMISATIONS.get(ScalarisOpType.CATEGORY_PAGE_LIST);
                    break;
                default:
                    break;
            }
        }
        final ScalarisReadNumberOp1 readOp = new ScalarisReadNumberOp1(scalaris_keys,
                optimisation, failNotFound);
        executor.addOp(readOp);
        try {
            executor.run();
        } catch (Exception e) {
            return new ValueResult<BigInteger>(false, involvedKeys,
                    e.getClass().getCanonicalName() + " reading page list at \""
                            + involvedKeys.toString() + "\" from Scalaris: "
                            + e.getMessage(), e instanceof ConnectionException,
                    statName, System.currentTimeMillis() - timeAtStart);
        }
        
        return new ValueResult<BigInteger>(involvedKeys, readOp.getValue(), statName,
                System.currentTimeMillis() - timeAtStart);
    }

    /**
     * Adds all keys from the given operation list to the list of involved keys.
     * 
     * @param involvedKeys
     *            list of involved keys
     * @param ops
     *            new operations
     */
    public static void addInvolvedKeys(List<InvolvedKey> involvedKeys, Collection<? extends Operation> ops) {
        assert involvedKeys != null;
        assert ops != null;
        for (Operation op : ops) {
            final OtpErlangString key = op.getKey();
            if (key != null) {
                if (op instanceof ReadOp) {
                    involvedKeys.add(new InvolvedKey(InvolvedKey.OP.READ, key.stringValue()));
                } else if (op instanceof PartialReadOp) {
                    involvedKeys.add(new InvolvedKey(InvolvedKey.OP.READ, key.stringValue()));
                } else {
                    involvedKeys.add(new InvolvedKey(InvolvedKey.OP.WRITE, key.stringValue()));
                }
            }
        }
    }
}
