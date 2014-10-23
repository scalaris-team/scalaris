package de.zib.scalaris.examples.wikipedia;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import de.zib.scalaris.examples.wikipedia.Options.APPEND_INCREMENT_BUCKETS;
import de.zib.scalaris.examples.wikipedia.Options.APPEND_INCREMENT_BUCKETS_WITH_WCACHE;
import de.zib.scalaris.examples.wikipedia.Options.APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY;
import de.zib.scalaris.examples.wikipedia.Options.APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY_RANDOM;
import de.zib.scalaris.examples.wikipedia.Options.IAppendIncrement;
import de.zib.scalaris.examples.wikipedia.Options.IBuckets;
import de.zib.scalaris.examples.wikipedia.Options.IReadBuckets;
import de.zib.scalaris.examples.wikipedia.Options.Optimisation;
import de.zib.scalaris.executor.ScalarisIncrementOp1;
import de.zib.scalaris.executor.ScalarisIncrementOp2;
import de.zib.scalaris.executor.ScalarisListAppendRemoveOp1;
import de.zib.scalaris.executor.ScalarisListAppendRemoveOp2;
import de.zib.scalaris.executor.ScalarisOpExecutor;
import de.zib.scalaris.executor.ScalarisWriteOp;
import de.zib.tools.LinkedMultiHashMap;

/**
 * Wraps {@link ScalarisOpExecutor} and adds the different operations
 * based on the current configuration.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class MyScalarisOpExecWrapper {
    protected final ScalarisOpExecutor executor;
    /**
     * Creates a new wrapper with the given executor.
     * 
     * @param executor the executor to use.
     */
    public MyScalarisOpExecWrapper(MyScalarisTxOpExecutor executor) {
        this.executor = executor;
    }

    /**
     * Creates a new write operation.
     * 
     * @param opType    the type of the operation
     * @param key       the key to write the value to
     * @param value     the value to write
     * 
     * @param <T>       type of the value
     */
    public <T> void addWrite(ScalarisOpType opType, String key, T value) {
        switch (opType) {
        default:
            executor.addOp(new ScalarisWriteOp<T>(key, value));
        }
    }

    /**
     * Creates a new list append operation.
     * 
     * @param opType      the type of the operation
     * @param key         the key to append the value to
     * @param toAdd       the value to add
     * @param countOpType the type of the countKey operation (may be the same
     *                    as <tt>opType</tt> or <tt>null</tt> if
     *                    <tt>countKey</tt> is)
     * @param countKey    the key for the counter of the entries in the list
     *                    (may be <tt>null</tt>)
     * 
     * @param <T>         type of the value to add
     */
    @SuppressWarnings("unchecked")
    public <T> void addAppend(ScalarisOpType opType, String key, T toAdd, ScalarisOpType countOpType, String countKey) {
        addAppendRemove(opType, key, Arrays.asList(toAdd), new ArrayList<T>(0), countOpType, countKey);
    }

    /**
     * Creates a new list remove operation.
     * 
     * @param opType      the type of the operation
     * @param key         the key to remove the value from
     * @param toRemove    the value to remove
     * @param countOpType the type of the countKey operation (may be the same
     *                    as <tt>opType</tt> or <tt>null</tt> if
     *                    <tt>countKey</tt> is)
     * @param countKey    the key for the counter of the entries in the list
     *                    (may be <tt>null</tt>)
     * 
     * @param <T>         type of the value to remove
     */
    @SuppressWarnings("unchecked")
    public <T> void addRemove(ScalarisOpType opType, String key, T toRemove, ScalarisOpType countOpType, String countKey) {
        addAppendRemove(opType, key, new ArrayList<T>(0), Arrays.asList(toRemove), countOpType, countKey);
    }

    /**
     * Creates a new number increment operation.
     * 
     * @param opType    the type of the operation
     * @param key       the key of the value to increment
     * @param toAdd     the value to increment by
     * @param belongsTo the object this number belongs to, e.g. a page title to
     *                  determine the partition the value gets added to or
     *                  removed from
     * 
     * @param <T>       type of the value to add
     */
    public <T extends Number, U> void addIncrement(final ScalarisOpType opType,
            final String key, final T toAdd, final U belongsTo) {
        final Optimisation optimisation = Options.getInstance().OPTIMISATIONS.get(opType);
        if (optimisation instanceof APPEND_INCREMENT_BUCKETS) {
            final APPEND_INCREMENT_BUCKETS optimisation2 = (APPEND_INCREMENT_BUCKETS) optimisation;
            final String key2 = key + optimisation2.getBucketString(belongsTo);
            executor.addOp(new ScalarisIncrementOp2<T>(key2, toAdd));
        } else if (optimisation instanceof APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY) {
            final APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY optimisation2 = (APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY) optimisation;
            final String key2 = key + optimisation2.getWriteBucketString();
            executor.addOp(new ScalarisIncrementOp2<T>(key2, toAdd));
        } else if (optimisation instanceof APPEND_INCREMENT_BUCKETS_WITH_WCACHE) {
            final APPEND_INCREMENT_BUCKETS_WITH_WCACHE optimisation2 = (APPEND_INCREMENT_BUCKETS_WITH_WCACHE) optimisation;
            final String key2 = key + optimisation2.getWriteBucketString(belongsTo);
            executor.addOp(new ScalarisIncrementOp2<T>(key2, toAdd));
        } else if (optimisation instanceof IAppendIncrement) {
            executor.addOp(new ScalarisIncrementOp2<T>(key, toAdd));
        } else {
            executor.addOp(new ScalarisIncrementOp1<T>(key, toAdd));
        }
    }

    /**
     * @return the executor
     */
    public ScalarisOpExecutor getExecutor() {
        return executor;
    }

    /**
     * Creates a new append+remove operation.
     * 
     * @param opType      the type of the operation
     * @param key         the key to append/remove the values to/from
     * @param toAdd       the values to add
     * @param toRemove    the values to remove
     * @param countOpType the type of the countKey operation (may be the same
     *                    as <tt>opType</tt> or <tt>null</tt> if
     *                    <tt>countKey</tt> is)
     * @param countKey    the key for the counter of the entries in the list
     *                    (may be <tt>null</tt>)
     * 
     * @param <T>         type of the value to remove
     */
    public <T> void addAppendRemove(final ScalarisOpType opType,
            final String key, final List<T> toAdd, final List<T> toRemove,
            final ScalarisOpType countOpType, String countKey) {
        final Optimisation countOptimisation = Options.getInstance().OPTIMISATIONS.get(countOpType);

        if (countKey != null && countOptimisation != null) {
            // separate optimisation for the count key
            int countInc = toAdd.size() - toRemove.size();
            if (countOptimisation instanceof IBuckets) {
                Random rand = new Random();
                String bucketStr;
                if (countOptimisation instanceof IReadBuckets) {
                    final IReadBuckets optimisation2 = (IReadBuckets) countOptimisation;
                    final int writeBuckets = optimisation2.getBuckets() - optimisation2.getReadBuckets();
                    final int bucket = optimisation2.getReadBuckets() + rand.nextInt(writeBuckets);
                    bucketStr = ":" + bucket;
                } else if (countOptimisation instanceof APPEND_INCREMENT_BUCKETS) {
                    final APPEND_INCREMENT_BUCKETS optimisation2 = (APPEND_INCREMENT_BUCKETS) countOptimisation;
                    bucketStr = ":" + rand.nextInt(optimisation2.getBuckets());
                } else {
                    throw new RuntimeException("unsupported optimisation: " + countOptimisation);
                }
                if (countOptimisation instanceof IAppendIncrement) {
                    executor.addOp(new ScalarisIncrementOp2<Integer>(countKey + bucketStr, countInc));
                } else {
                    executor.addOp(new ScalarisIncrementOp1<Integer>(countKey + bucketStr, countInc));
                }
            } else if (countOptimisation instanceof IAppendIncrement) {
                executor.addOp(new ScalarisIncrementOp2<Integer>(countKey, countInc));
            } else {
                executor.addOp(new ScalarisIncrementOp1<Integer>(countKey, countInc));
            }
            // prevent the code below to write to a counter:
            countKey = null;
        }
        
        final Optimisation optimisation = Options.getInstance().OPTIMISATIONS.get(opType);
        if (optimisation instanceof APPEND_INCREMENT_BUCKETS) {
            final APPEND_INCREMENT_BUCKETS optimisation2 = (APPEND_INCREMENT_BUCKETS) optimisation;
            final HashMap<String, String> countKeys = new HashMap<String, String>(toAdd.size() + toRemove.size());
            final LinkedMultiHashMap<String, T> kvAdd = new LinkedMultiHashMap<String, T>();
            final LinkedMultiHashMap<String, T> kvRemove = new LinkedMultiHashMap<String, T>();
            for (T t : toAdd) {
                final String bucketStr = optimisation2.getBucketString(t);
                final String key2 = key + bucketStr;
                if (countKey != null) {
                    countKeys.put(key2, countKey + bucketStr);
                } else {
                    countKeys.put(key2, null);
                }
                kvAdd.put1(key2, t);
            }
            for (T t : toRemove) {
                final String bucketStr = optimisation2.getBucketString(t);
                final String key2 = key + bucketStr;
                if (countKey != null) {
                    countKeys.put(key2, countKey + bucketStr);
                } else {
                    countKeys.put(key2, null);
                }
                kvRemove.put1(key2, t);
            }
            for (Entry<String, String> entry : countKeys.entrySet()) {
                final String key2 = entry.getKey();
                List<T> toAdd2 = kvAdd.get(key2);
                if (toAdd2 == null) {
                    toAdd2 = new ArrayList<T>(0);
                }
                List<T> toRemove2 = kvRemove.get(key2);
                if (toRemove2 == null) {
                    toRemove2 = new ArrayList<T>(0);
                }
                executor.addOp(new ScalarisListAppendRemoveOp2<T>(key2, toAdd2, toRemove2, entry.getValue()));
            }
        } else if (optimisation instanceof APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY) {
            final APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY optimisation2 = (APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY) optimisation;
            final String bucketStr = optimisation2.getWriteBucketString();
            final String key2 = key + bucketStr;
            String countKey2 = null;
            if (countKey != null) {
                countKey2 = countKey + bucketStr;
            }
            if (optimisation instanceof APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY_RANDOM
                    && !toRemove.isEmpty()) {
                System.err.println("deleting entries is not supported with APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY_RANDOM");
            }
            executor.addOp(new ScalarisListAppendRemoveOp2<T>(key2, toAdd, toRemove, countKey2));
        } else if (optimisation instanceof APPEND_INCREMENT_BUCKETS_WITH_WCACHE) {
            final APPEND_INCREMENT_BUCKETS_WITH_WCACHE optimisation2 = (APPEND_INCREMENT_BUCKETS_WITH_WCACHE) optimisation;
            final HashMap<String, String> countKeys = new HashMap<String, String>(2 * (toAdd.size() + toRemove.size()));
            final LinkedMultiHashMap<String, Object> kvAdd = new LinkedMultiHashMap<String, Object>();
            final LinkedMultiHashMap<String, Object> kvRemove = new LinkedMultiHashMap<String, Object>();
            for (T t : toAdd) {
                // add to add write-bucket
                final String bucketStr = optimisation2.getWriteBucketString(t);
                final String key2 = key + bucketStr;
                if (countKey != null) {
                    countKeys.put(key2, countKey + bucketStr);
                } else {
                    countKeys.put(key2, null);
                }
                kvAdd.put1(key2, optimisation2.makeAdd(t));
                // also need to remove any previous delete op from the write-bucket (if present!)
                kvRemove.put1(key2, optimisation2.makeDelete(t));
            }
            for (T t : toRemove) {
                // add to delete write-bucket
                final String bucketStr = optimisation2.getWriteBucketString(t);
                final String key2 = key + bucketStr;
                if (countKey != null) {
                    countKeys.put(key2, countKey + bucketStr);
                } else {
                    countKeys.put(key2, null);
                }
                kvAdd.put1(key2, optimisation2.makeDelete(t));
                // also need to remove any previous add op from the write-bucket (if present!)
                kvRemove.put1(key2, optimisation2.makeAdd(t));
            }
            for (Entry<String, String> entry : countKeys.entrySet()) {
                final String key2 = entry.getKey();
                List<Object> toAdd2 = kvAdd.get(key2);
                if (toAdd2 == null) {
                    toAdd2 = new ArrayList<Object>(0);
                }
                List<Object> toRemove2 = kvRemove.get(key2);
                if (toRemove2 == null) {
                    toRemove2 = new ArrayList<Object>(0);
                }
                executor.addOp(new ScalarisListAppendRemoveOp2<Object>(key2, toAdd2, toRemove2, entry.getValue()));
            }
        } else if (optimisation instanceof IAppendIncrement) {
            executor.addOp(new ScalarisListAppendRemoveOp2<T>(key, toAdd, toRemove, countKey));
        } else {
            executor.addOp(new ScalarisListAppendRemoveOp1<T>(key, toAdd, toRemove, countKey));
        }
    }
}