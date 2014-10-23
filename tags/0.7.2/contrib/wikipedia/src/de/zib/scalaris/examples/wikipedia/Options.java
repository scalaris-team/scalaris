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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import de.zib.scalaris.ErlangValue;
import de.zib.scalaris.examples.wikipedia.bliki.ExistingPagesCache;
import de.zib.scalaris.examples.wikipedia.bliki.ExistingPagesCache.ExistingPagesCacheBloom;
import de.zib.scalaris.examples.wikipedia.bliki.ExistingPagesCache.ExistingPagesCacheFull;


/**
 * @author Nico Kruber, kruber@zib.de
 *
 */
public class Options {
    
    private final static Options instance = new Options();
    protected static final Pattern CONFIG_SINGLE_OPTIMISATION = Pattern.compile("([a-zA-Z_0-9]*):([a-zA-Z_0-9]*)(?:\\(([a-zA-Z_0-9,]*)\\))?");

    /**
     * The name of the server (part of the URL), e.g. <tt>en.wikipedia.org</tt>.
     */
    public String SERVERNAME = "localhost:8080";
    /**
     * The path on the server (part of the URL), e.g. <tt>/wiki</tt>.
     */
    public String SERVERPATH = "/scalaris-wiki/wiki";
    
    /**
     * Whether to support back-links ("what links here?") or not.
     */
    public boolean WIKI_USE_BACKLINKS = true;
    
    /**
     * How often to re-try a "sage page" operation in case of failures, e.g.
     * concurrent edits.
     * 
     * @see #WIKI_SAVEPAGE_RETRY_DELAY
     */
    public int WIKI_SAVEPAGE_RETRIES = 0;
    
    /**
     * How long to wait after a failed "sage page" operation before trying
     * again (in milliseconds).
     * 
     * @see #WIKI_SAVEPAGE_RETRIES
     */
    public int WIKI_SAVEPAGE_RETRY_DELAY = 10;
    
    /**
     * Which implementation to use for the pages cache.
     * 
     * @see #WIKI_REBUILD_PAGES_CACHE
     */
    public Class<? extends ExistingPagesCache> WIKI_PAGES_CACHE_IMPL = ExistingPagesCacheBloom.class;
    
    /**
     * How often to re-create the pages cache with the existing pages (in
     * seconds). The pages cache will be disabled if a value less than or equal
     * to 0 is provided.
     * 
     * @see #WIKI_PAGES_CACHE_IMPL
     */
    public int WIKI_REBUILD_PAGES_CACHE = 10 * 60;
    
    /**
     * Whether and how to store user contributions in the DB.
     */
    public STORE_CONTRIB_TYPE WIKI_STORE_CONTRIBUTIONS = STORE_CONTRIB_TYPE.OUTSIDE_TX;
    
    /**
     * Optimisations to use for the different Scalaris operations.
     */
    final public EnumMap<ScalarisOpType, Optimisation> OPTIMISATIONS = new EnumMap<ScalarisOpType, Options.Optimisation>(
            ScalarisOpType.class);
    
    /**
     * Store user requests in a log for the last x minutes before the last
     * request.
     */
    public int LOG_USER_REQS = 0;
    
    /**
     * Time (in seconds) between executions of the node discovery daemon of
     * {@link de.zib.scalaris.NodeDiscovery} to look for new Scalaris nodes (
     * <tt>0</tt> to disable).
     */
    public int SCALARIS_NODE_DISCOVERY = 60;
    
    /**
     * Creates a new default option object.
     */
    public Options() {
        for (ScalarisOpType op : ScalarisOpType.values()) {
            OPTIMISATIONS.put(op, new APPEND_INCREMENT());
        }
        OPTIMISATIONS.put(ScalarisOpType.PAGE_COUNT, null);
        OPTIMISATIONS.put(ScalarisOpType.CATEGORY_PAGE_COUNT, null);
    }

    /**
     * Gets the static instance used throughout the wiki implementation.
     * 
     * @return the instance
     */
    public static Options getInstance() {
        return instance;
    }
    
    /**
     * Type of storing user contributions in the DB.
     */
    public static enum STORE_CONTRIB_TYPE {
        /**
         * Do not store user contributions.
         */
        NONE("NONE"),
        /**
         * Store user contributions outside the main transaction used during
         * save.
         */
        OUTSIDE_TX("OUTSIDE_TX");

        private final String text;

        STORE_CONTRIB_TYPE(String text) {
            this.text = text;
        }

        /**
         * Converts the enum to text.
         */
        public String toString() {
            return this.text;
        }

        /**
         * Tries to convert a text to the according enum value.
         * 
         * @param text the text to convert
         * 
         * @return the according enum value
         */
        public static STORE_CONTRIB_TYPE fromString(String text) {
            if (text != null) {
                for (STORE_CONTRIB_TYPE b : STORE_CONTRIB_TYPE.values()) {
                    if (text.equalsIgnoreCase(b.text)) {
                        return b;
                    }
                }
            }
            throw new IllegalArgumentException("No constant with text " + text
                    + " found");
        }
    }
    
    /**
     * Indicates a generic optimisation implementation.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static interface Optimisation {
    }
    
    /**
     * Indicates that the traditional read/write operations of Scalaris should
     * be used, i.e. no append/increment.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static class TRADITIONAL implements Optimisation {
        @Override
        public String toString() {
            return "TRADITIONAL";
        }
    }

    /**
     * Indicates that the new append and increment operations of Scalaris should
     * be used, i.e.
     * {@link de.zib.scalaris.Transaction#addDelOnList(String, java.util.List, java.util.List)}
     * and {@link de.zib.scalaris.Transaction#addOnNr(String, Object)}.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static interface IAppendIncrement {
    }

    /**
     * Indicates that the new append and increment operations of Scalaris should
     * be used, i.e.
     * {@link de.zib.scalaris.Transaction#addDelOnList(String, java.util.List, java.util.List)}
     * and {@link de.zib.scalaris.Transaction#addOnNr(String, Object)}.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static class APPEND_INCREMENT implements Optimisation,
            IAppendIncrement {
        @Override
        public String toString() {
            return "APPEND_INCREMENT";
        }
    }

    /**
     * Indicates that the new partial reads for random elements of a list
     * {@link de.zib.scalaris.operations.ReadRandomFromListOp} and sublists
     * {@link de.zib.scalaris.operations.ReadSublistOp} should be used.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static interface IPartialRead {
    }

    /**
     * Indicates that the new append and increment operations of Scalaris should
     * be used, i.e.
     * {@link de.zib.scalaris.Transaction#addDelOnList(String, java.util.List, java.util.List)}
     * and {@link de.zib.scalaris.Transaction#addOnNr(String, Object)} as well
     * as partial reads for random elements of a list
     * {@link de.zib.scalaris.operations.ReadRandomFromListOp} and sublists
     * {@link de.zib.scalaris.operations.ReadSublistOp}.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static class APPEND_INCREMENT_PARTIALREAD extends APPEND_INCREMENT
            implements IPartialRead {
        @Override
        public String toString() {
            return "APPEND_INCREMENT_PARTIALREAD";
        }
    }

    /**
     * Indicates that the optimisation partitions data into buckets.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static interface IBuckets {
        /**
         * Gets the number of available buckets
         * 
         * @return number of buckets
         */
        public abstract int getBuckets();
    }

    /**
     * Indicates that the optimisation partitions data into buckets split into
     * read-buckets and other buckets.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static interface IReadBuckets extends IBuckets {
        /**
         * Gets the number of available read-buckets.
         * 
         * @return number of buckets not used for the write caches etc.
         */
        public abstract int getReadBuckets();
    }

    /**
     * Indicates that the new append and increment operations of Scalaris should
     * be used and list values should be distributed among several partitions,
     * i.e. buckets.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static abstract class APPEND_INCREMENT_BUCKETS implements
            Optimisation, IAppendIncrement, IBuckets {
        final protected int buckets;
        
        /**
         * Constructor.
         * 
         * @param buckets
         *            number of available buckets
         */
        public APPEND_INCREMENT_BUCKETS(int buckets) {
            this.buckets = buckets;
        }

        public int getBuckets() {
            return buckets;
        }
        
        /**
         * Gets the string to append to the key in order to point to the bucket
         * for the given value.
         * 
         * @param value
         *            the value to check the bucket for
         * 
         * @return the bucket string, e.g. ":0"
         */
        abstract public <T> String getBucketString(final T value);
    }

    /**
     * Indicates that the new append and increment operations of Scalaris should
     * be used and list values should be randomly distributed among several
     * partitions, i.e. buckets.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static class APPEND_INCREMENT_BUCKETS_RANDOM extends
            APPEND_INCREMENT_BUCKETS {
        final static protected Random rand = new Random();
        /**
         * Constructor.
         * 
         * @param buckets
         *            number of available buckets
         */
        public APPEND_INCREMENT_BUCKETS_RANDOM(int buckets) {
            super(buckets);
        }

        @Override
        public <T> String getBucketString(final T value) {
            return ":" + rand.nextInt(buckets);
        }
        
        @Override
        public String toString() {
            return "APPEND_INCREMENT_BUCKETS_RANDOM(" + buckets + ")";
        }
    }

    /**
     * Indicates that the new append, increment and partial read operations of
     * Scalaris should be used and list values should be randomly distributed
     * among several partitions, i.e. buckets.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static class APPEND_INCREMENT_PARTIALREAD_BUCKETS_RANDOM extends
            APPEND_INCREMENT_BUCKETS_RANDOM implements IPartialRead {
        /**
         * Constructor.
         * 
         * @param buckets
         *            number of available buckets
         */
        public APPEND_INCREMENT_PARTIALREAD_BUCKETS_RANDOM(int buckets) {
            super(buckets);
        }
        
        @Override
        public String toString() {
            return "APPEND_INCREMENT_PARTIALREAD_BUCKETS_RANDOM(" + buckets + ")";
        }
    }
    
    /**
     * Indicates that the new append and increment operations of Scalaris should
     * be used and list values should be distributed among several partitions,
     * i.e. buckets, depending on the value's hash.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static class APPEND_INCREMENT_BUCKETS_WITH_HASH extends
            APPEND_INCREMENT_BUCKETS {
        /**
         * Constructor.
         * 
         * @param buckets
         *            number of available buckets
         */
        public APPEND_INCREMENT_BUCKETS_WITH_HASH(int buckets) {
            super(buckets);
        }
        
        @Override
        public <T> String getBucketString(final T value) {
            return ":" + Math.abs((value.hashCode() % buckets));
        }
        
        @Override
        public String toString() {
            return "APPEND_INCREMENT_BUCKETS_WITH_HASH(" + buckets + ")";
        }
    }

    /**
     * Indicates that the new append, increment and partial read operations of
     * Scalaris should be used and list values should be distributed among
     * several partitions, i.e. buckets, depending on the value's hash.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static class APPEND_INCREMENT_PARTIALREAD_BUCKETS_WITH_HASH extends
            APPEND_INCREMENT_BUCKETS_WITH_HASH implements IPartialRead {
        /**
         * Constructor.
         * 
         * @param buckets
         *            number of available buckets
         */
        public APPEND_INCREMENT_PARTIALREAD_BUCKETS_WITH_HASH(int buckets) {
            super(buckets);
        }
        
        @Override
        public String toString() {
            return "APPEND_INCREMENT_PARTIALREAD_BUCKETS_WITH_HASH(" + buckets + ")";
        }
    }

    /**
     * Indicates that the new append and increment operations of Scalaris should
     * be used and list values should be distributed among several partitions,
     * i.e. buckets, using a set of read-buckets and another set as a small
     * write cache (write-buckets).
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static abstract class APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY
            implements Optimisation, IAppendIncrement, IReadBuckets {
        final protected int readBuckets;
        final protected int writeBuckets;
        
        /**
         * Constructor.
         * 
         * @param readBuckets
         *            number of available read buckets
         * @param writeBuckets
         *            number of available read buckets
         */
        public APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY(int readBuckets,
                int writeBuckets) {
            assert (readBuckets >= 1 && writeBuckets >= 1);
            this.readBuckets = readBuckets;
            this.writeBuckets = writeBuckets;
        }

        public int getBuckets() {
            return readBuckets + writeBuckets;
        }

        public int getReadBuckets() {
            return readBuckets;
        }

        /**
         * Gets the string to append to the key in order to point to a
         * read-bucket for the given value.
         * 
         * @return the bucket string, e.g. ":0"
         */
        abstract public <T> String getReadBucketString();
        
        /**
         * Gets the string to append to the key in order to point to a
         * write-bucket for the given value.
         * 
         * @return the bucket string, e.g. ":0"
         */
        abstract public <T> String getWriteBucketString();
    }

    /**
     * Indicates that the new append and increment operations of Scalaris should
     * be used and list values should be distributed among several partitions,
     * i.e. buckets, using a set of read-buckets and another set as a small
     * write cache (write-buckets). Supports only list add operations (no
     * removal!)
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static class APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY_RANDOM extends
            APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY {
        final static protected Random rand = new Random();

        /**
         * Constructor.
         * 
         * @param readBuckets
         *            number of available read buckets
         * @param writeBuckets
         *            number of available write buckets
         */
        public APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY_RANDOM(
                int readBuckets, int writeBuckets) {
            super(readBuckets, writeBuckets);
        }

        @Override
        public <T> String getReadBucketString() {
            return ":" + (rand.nextInt(readBuckets));
        }

        @Override
        public <T> String getWriteBucketString() {
            return ":" + (rand.nextInt(writeBuckets) + readBuckets);
        }
        
        @Override
        public String toString() {
            return "APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY_RANDOM("
                    + readBuckets + "," + writeBuckets + ")";
        }
    }

    /**
     * Indicates that the new append, increment and partial read operations of
     * Scalaris should be used and list values should be distributed among
     * several partitions, i.e. buckets, using a set of read-buckets and another
     * set as a small write cache (write-buckets). Supports only list add
     * operations (no removal!)
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static class APPEND_INCREMENT_PARTIALREAD_BUCKETS_WITH_WCACHE_ADDONLY_RANDOM
            extends APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY_RANDOM
            implements IPartialRead {
        /**
         * Constructor.
         * 
         * @param readBuckets
         *            number of available read buckets
         * @param writeBuckets
         *            number of available write buckets
         */
        public APPEND_INCREMENT_PARTIALREAD_BUCKETS_WITH_WCACHE_ADDONLY_RANDOM(
                int readBuckets, int writeBuckets) {
            super(readBuckets, writeBuckets);
        }

        @Override
        public String toString() {
            return "APPEND_INCREMENT_PARTIALREAD_BUCKETS_WITH_WCACHE_ADDONLY_RANDOM("
                    + readBuckets + "," + writeBuckets + ")";
        }
    }

    /**
     * Indicates that the new append and increment operations of Scalaris should
     * be used and list values should be distributed among several partitions,
     * i.e. buckets, using a set of read-buckets and another set as a small
     * write cache (write-buckets). In contrast to
     * {@link APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY} this class also
     * supports add and delete operations by tagging values with
     * {@link #makeAdd(Object)} and {@link #makeDelete(Object)}, respectively,
     * when writing to any of the write buckets.
     * 
     * Make sure to only use the tagged values when writing values to a write
     * bucket! Use {@link WriteCacheDiffConv} to get a converter that
     * creates a {@link WriteCacheDiff} object which can then be used to create
     * a global view of the whole stored list including the write cache.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static abstract class APPEND_INCREMENT_BUCKETS_WITH_WCACHE implements
            Optimisation, IAppendIncrement, IReadBuckets {
        final protected int readBuckets;
        final protected int writeBuckets;

        /**
         * Constructor.
         * 
         * @param readBuckets
         *            number of available read buckets
         * @param writeBuckets
         *            number of available write buckets for new/deleted elements
         *            (compared to the data in the read buckets);
         *            two different write buckets of this size will be used
         */
        public APPEND_INCREMENT_BUCKETS_WITH_WCACHE(int readBuckets, int writeBuckets) {
            assert (readBuckets >= 1 && writeBuckets >= 1);
            this.readBuckets = readBuckets;
            this.writeBuckets = writeBuckets;
        }

        public int getBuckets() {
            return readBuckets + writeBuckets;
        }

        public int getReadBuckets() {
            return readBuckets;
        }

        /**
         * Gets the number write buckets.
         * 
         * @return number of buckets to use for each write bucket type (add and
         *         del)
         */
        public int getWriteBuckets() {
            return writeBuckets;
        }

        /**
         * Hashes the given value to a bucket.
         * 
         * @param value
         *            the value to check the bucket for
         * @param buckets
         *            number of available buckets for this hash operation
         * @param offset
         *            first bucket number
         * 
         * @return the bucket, e.g. 0
         */
        abstract protected <T> int hashToBucket(T value, int buckets, int offset);

        /**
         * Gets the string to append to the key in order to point to a
         * read-bucket for the given value.
         * 
         * @param value
         *            the value to check the bucket for
         * 
         * @return the bucket string, e.g. ":0"
         */
        public <T> String getReadBucketString(final T value) {
            return ":" + hashToBucket(value, readBuckets, 0);
        }

        /**
         * Gets the string to append to the key in order to point to a
         * write-bucket for the given value.
         * 
         * @param value
         *            the value to check the bucket for
         * 
         * @return the bucket string, e.g. ":0"
         */
        public <T> String getWriteBucketString(final T value) {
            return ":" + hashToBucket(value, writeBuckets, readBuckets);
        }
        
        /**
         * Tags this value as an "add" operation to be written to the write
         * cache.
         * 
         * @param value
         *            the value to tag
         * 
         * @return the value to use when writing to the write cache
         */
        public final <T> Object makeAdd(final T value) {
            return Arrays.asList(1, value);
        }

        /**
         * Tags this value as a "delete" operation to be written to the write
         * cache.
         * 
         * @param value
         *            the value to tag
         * 
         * @return the value to use when writing to the write cache
         */
        public final <T> Object makeDelete(final T value) {
            return Arrays.asList(-1, value);
        }

        /**
         * Result object that represents the write cache.
         * 
         * @param <T>
         *            the element type
         * 
         * @see WriteCacheDiffConv
         */
        public static class WriteCacheDiff<T> {
            /**
             * List of elements to add to the read-buckets.
             */
            final public List<T> toAdd;
            /**
             * List of elements to remove from the read-buckets.
             */
            final public Set<T> toDelete;
            
            protected WriteCacheDiff(List<T> toAdd, Set<T> toDelete) {
                this.toAdd = toAdd;
                this.toDelete = toDelete;
            }
        }
        
        /**
         * Converter that converts the write cache to a {@link WriteCacheDiff}
         * object.
         * 
         * @author Nico Kruber, kruber@zib.de
         * 
         * @param <T>
         *            element type to convert
         */
        public static class WriteCacheDiffConv<T>
                implements
                ErlangConverter<Options.APPEND_INCREMENT_BUCKETS_WITH_WCACHE.WriteCacheDiff<T>> {
            private final ErlangConverter<T> elemConv;

            /**
             * Creates a converter to use for simplified access to the write
             * cache.
             * 
             * @param elemConv
             *            converter for a single entry of the list
             */
            public WriteCacheDiffConv(ErlangConverter<T> elemConv) {
                this.elemConv = elemConv;
            }

            @Override
            public WriteCacheDiff<T> convert(ErlangValue v)
                    throws ClassCastException {
                final List<ErlangValue> listValue = v.listValue();
                final ArrayList<T> toAdd = new ArrayList<T>(listValue.size());
                final HashSet<T> toDelete = new HashSet<T>(listValue.size());
                for(ErlangValue elem : listValue) {
                    List<ErlangValue> diffObj = elem.listValue();
                    if (diffObj.size() != 2) {
                        throw new ClassCastException();
                    }
                    int tag = diffObj.get(0).intValue();
                    T value = elemConv.convert(diffObj.get(1));
                    switch (tag) {
                        case 1:
                            toAdd.add(value);
                            break;
                        case -1:
                            toDelete.add(value);
                            break;
                        default:
                            throw new ClassCastException();
                    }
                }
                return new WriteCacheDiff<T>(toAdd, toDelete);
            }
        }
    }

    /**
     * Indicates that the new append and increment operations of Scalaris should
     * be used and list values should be distributed among several partitions,
     * i.e. buckets, using a set of read-buckets and another set as a small
     * write cache (write-buckets) for added and removed values. The values will
     * be added to the write cache based on their hash.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static class APPEND_INCREMENT_BUCKETS_WITH_WCACHE_HASH extends
            APPEND_INCREMENT_BUCKETS_WITH_WCACHE {
        final static protected Random rand = new Random();

        /**
         * Constructor.
         * 
         * @param readBuckets
         *            number of available read buckets
         * @param writeBuckets
         *            number of available write buckets for new/deleted elements
         *            (compared to the data in the read buckets);
         *            two different write buckets of this size will be used
         */
        public APPEND_INCREMENT_BUCKETS_WITH_WCACHE_HASH(int readBuckets,
                int writeBuckets) {
            super(readBuckets, writeBuckets);
        }

        @Override
        protected <T> int hashToBucket(T value, int buckets, int offset) {
            return Math.abs((value.hashCode() % buckets)) + offset;
        }

        @Override
        public String toString() {
            return "APPEND_INCREMENT_BUCKETS_WITH_WCACHE_HASH(" + readBuckets
                    + "," + writeBuckets + ")";
        }
    }

    
    /**
     * Parses the given option strings into their appropriate properties.
     * 
     * @param options
     *            the {@link Options} object to parse into
     * @param SERVERNAME
     *            {@link Options#SERVERNAME}
     * @param SERVERPATH
     *            {@link Options#SERVERPATH}
     * @param WIKI_USE_BACKLINKS
     *            {@link Options#WIKI_USE_BACKLINKS}
     * @param WIKI_SAVEPAGE_RETRIES
     *            {@link Options#WIKI_SAVEPAGE_RETRIES}
     * @param WIKI_SAVEPAGE_RETRY_DELAY
     *            {@link Options#WIKI_SAVEPAGE_RETRY_DELAY}
     * @param WIKI_PAGES_CACHE_IMPL
     *            {@link Options#WIKI_PAGES_CACHE_IMPL}
     * @param WIKI_REBUILD_PAGES_CACHE
     *            {@link Options#WIKI_REBUILD_PAGES_CACHE}
     * @param WIKI_STORE_CONTRIBUTIONS
     *            {@link Options#WIKI_STORE_CONTRIBUTIONS}
     * @param OPTIMISATIONS
     *            {@link Options#OPTIMISATIONS}
     * @param LOG_USER_REQS
     *            {@link Options#LOG_USER_REQS}
     * @param SCALARIS_NODE_DISCOVERY
     *            {@link Options#SCALARIS_NODE_DISCOVERY}
     */
    public static void parseOptions(Options options, final String SERVERNAME, final String SERVERPATH,
            final String WIKI_USE_BACKLINKS,
            final String WIKI_SAVEPAGE_RETRIES,
            final String WIKI_SAVEPAGE_RETRY_DELAY,
            final String WIKI_PAGES_CACHE_IMPL,
            final String WIKI_REBUILD_PAGES_CACHE,
            final String WIKI_STORE_CONTRIBUTIONS, final String OPTIMISATIONS,
            final String LOG_USER_REQS, final String SCALARIS_NODE_DISCOVERY) {
        if (SERVERNAME != null) {
            options.SERVERNAME = SERVERNAME;
        }
        if (SERVERPATH != null) {
            options.SERVERPATH = SERVERPATH;
        }
        if (WIKI_USE_BACKLINKS != null) {
            options.WIKI_USE_BACKLINKS = Boolean.parseBoolean(WIKI_USE_BACKLINKS);
        }
        if (WIKI_SAVEPAGE_RETRIES != null) {
            options.WIKI_SAVEPAGE_RETRIES = Integer.parseInt(WIKI_SAVEPAGE_RETRIES);
        }
        if (WIKI_SAVEPAGE_RETRY_DELAY != null) {
            options.WIKI_SAVEPAGE_RETRY_DELAY = Integer.parseInt(WIKI_SAVEPAGE_RETRY_DELAY);
        }
        if (WIKI_PAGES_CACHE_IMPL != null) {
            if (WIKI_PAGES_CACHE_IMPL.equals("BLOOM")) {
                options.WIKI_PAGES_CACHE_IMPL = ExistingPagesCacheBloom.class;
            } else if (WIKI_PAGES_CACHE_IMPL.equals("FULL_SET")) {
                options.WIKI_PAGES_CACHE_IMPL = ExistingPagesCacheFull.class;
            } else {
                System.err.println("unknown WIKI_PAGES_CACHE_IMPL found: " + WIKI_PAGES_CACHE_IMPL);
            }
        }
        if (WIKI_REBUILD_PAGES_CACHE != null) {
            options.WIKI_REBUILD_PAGES_CACHE = Integer.parseInt(WIKI_REBUILD_PAGES_CACHE);
        }
        if (WIKI_STORE_CONTRIBUTIONS != null) {
            options.WIKI_STORE_CONTRIBUTIONS = STORE_CONTRIB_TYPE.fromString(WIKI_STORE_CONTRIBUTIONS);
        }
        if (OPTIMISATIONS != null) {
            for (String singleOpt : OPTIMISATIONS.split("\\|")) {
                final Matcher matcher = CONFIG_SINGLE_OPTIMISATION.matcher(singleOpt);
                if (matcher.matches()) {
                    final String operationStr = matcher.group(1);
                    if (operationStr.equals("ALL")) {
                        Optimisation optimisation = parseOptimisationString(matcher);
                        if (optimisation == null) {
                            // fall back if not parsed correctly:
                            optimisation = new APPEND_INCREMENT();
                        }
                        for (ScalarisOpType op : ScalarisOpType.values()) {
                            if (!(op.equals(ScalarisOpType.PAGE_COUNT) ||
                                    op.equals(ScalarisOpType.CATEGORY_PAGE_COUNT))) {
                                options.OPTIMISATIONS.put(op, optimisation);
                            }
                        }
                    } else {
                        ScalarisOpType operation = ScalarisOpType.fromString(operationStr);
                        Optimisation optimisation = parseOptimisationString(matcher);
                        if (optimisation != null) {
                            options.OPTIMISATIONS.put(operation, optimisation);
                        }
                    }
                }
            }
        }
        if (LOG_USER_REQS != null) {
            options.LOG_USER_REQS = Integer.parseInt(LOG_USER_REQS);
        }
        if (SCALARIS_NODE_DISCOVERY != null) {
            options.SCALARIS_NODE_DISCOVERY = Integer.parseInt(SCALARIS_NODE_DISCOVERY);
        }
    }

    /**
     * Parses an optimisation string into an {@link Optimisation} object.
     * 
     * @param matcher
     *            matcher (1st group: group of keys to apply to, 2nd group:
     *            optimisation class, 3rd group: optimisation parameters
     * 
     * @return an {@link Optimisation} implementation or <tt>null</tt> if no
     *         matching optimisation was found
     * @throws NumberFormatException
     *             if an integer parameter was wrong
     */
    public static Optimisation parseOptimisationString(final Matcher matcher)
            throws NumberFormatException {
        String optimisationStr = matcher.group(2);
        String parameterStr = matcher.group(3);
        if (optimisationStr.equals("TRADITIONAL") && parameterStr == null) {
            return new Options.TRADITIONAL();
        } else if (optimisationStr.equals("APPEND_INCREMENT")
                && parameterStr == null) {
            return new Options.APPEND_INCREMENT();
        } else if (optimisationStr.equals("APPEND_INCREMENT_PARTIALREAD")
                && parameterStr == null) {
            return new Options.APPEND_INCREMENT_PARTIALREAD();
        } else if (optimisationStr.equals("APPEND_INCREMENT_BUCKETS_RANDOM")
                && parameterStr != null) {
            String[] parameters = parameterStr.split(",");
            return new Options.APPEND_INCREMENT_BUCKETS_RANDOM(
                    Integer.parseInt(parameters[0]));
        } else if (optimisationStr.equals("APPEND_INCREMENT_PARTIALREAD_BUCKETS_RANDOM")
                && parameterStr != null) {
            String[] parameters = parameterStr.split(",");
            return new Options.APPEND_INCREMENT_PARTIALREAD_BUCKETS_RANDOM(
                    Integer.parseInt(parameters[0]));
        } else if (optimisationStr.equals("APPEND_INCREMENT_BUCKETS_WITH_HASH")
                && parameterStr != null) {
            String[] parameters = parameterStr.split(",");
            return new Options.APPEND_INCREMENT_BUCKETS_WITH_HASH(
                    Integer.parseInt(parameters[0]));
        } else if (optimisationStr.equals("APPEND_INCREMENT_PARTIALREAD_BUCKETS_WITH_HASH")
                && parameterStr != null) {
            String[] parameters = parameterStr.split(",");
            return new Options.APPEND_INCREMENT_PARTIALREAD_BUCKETS_WITH_HASH(
                    Integer.parseInt(parameters[0]));
        } else if (optimisationStr.equals("APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY_RANDOM")
                && parameterStr != null) {
            String[] parameters = parameterStr.split(",");
            return new Options.APPEND_INCREMENT_BUCKETS_WITH_WCACHE_ADDONLY_RANDOM(
                    Integer.parseInt(parameters[0]),
                    Integer.parseInt(parameters[1]));
        } else if (optimisationStr.equals("APPEND_INCREMENT_PARTIALREAD_BUCKETS_WITH_WCACHE_ADDONLY_RANDOM")
                && parameterStr != null) {
            String[] parameters = parameterStr.split(",");
            return new Options.APPEND_INCREMENT_PARTIALREAD_BUCKETS_WITH_WCACHE_ADDONLY_RANDOM(
                    Integer.parseInt(parameters[0]),
                    Integer.parseInt(parameters[1]));
        } else if (optimisationStr.equals("APPEND_INCREMENT_BUCKETS_WITH_WCACHE_HASH") && parameterStr != null) {
            String[] parameters = parameterStr.split(",");
            return new Options.APPEND_INCREMENT_BUCKETS_WITH_WCACHE_HASH(
                    Integer.parseInt(parameters[0]),
                    Integer.parseInt(parameters[1]));
        }
        System.err.println("unknown optimisation found: " + matcher.group());
        return null;
    }

    protected static class WebXmlInitParamHandler extends DefaultHandler {
        public final Map<String, String> initParams = new HashMap<String, String>();
        private final Map<String, String> curInitParams = new HashMap<String, String>();
        protected StringBuilder curString = new StringBuilder();
        protected String curInitParamName = null;
        protected String curInitParamValue = null;
        private boolean inWebApp = false;
        private boolean inServlet = false;
        private boolean inWikiServlet = false;
        private boolean inInitParam = false;
        private boolean parseContent = false;
        

        /* (non-Javadoc)
         * @see org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
         */
        @Override
        public void startElement(String uri, String localName, String qName,
                Attributes attributes) throws SAXException {
            /*
             * <web-app ...>
             *  <display-name>...</display-name>
             *  <servlet>
             *   <description>...</description>
             *   <display-name>...</display-name>
             *   <servlet-name>...</servlet-name>
             *   <servlet-class>de.zib.scalaris.examples.wikipedia.bliki.WikiServletScalaris</servlet-class>
             *   <init-param>
             *    <param-name>
             *     SERVERNAME|LOG_USER_REQS|SCALARIS_NODE_DISCOVERY|SERVERPATH|
             *     WIKI_USE_BACKLINKS|WIKI_SAVEPAGE_RETRIES|WIKI_SAVEPAGE_RETRY_DELAY|
             *     WIKI_PAGES_CACHE_IMPL|WIKI_REBUILD_PAGES_CACHE|WIKI_STORE_CONTRIBUTIONS|
             *     WIKI_OPTIMISATIONS|...
             *    </param-name>
             *    <param-value>...</param-value>
             *   </init-param>
             *  </servlet>
             *  ...
             * </web-app>
             */
            if (inWebApp) {
                if (inServlet) {
                    if (inInitParam) {
                        if (localName.equals("param-name")) {
                            parseContent();
                        } else if (localName.equals("param-value")) {
                            parseContent();
                        } else {
                            throw new SAXException("unknown tag in <init-param>: " + localName);
                        }
                    } else if (localName.equals("servlet-class")) {
                        parseContent();
                    } else if (localName.equals("init-param")) {
                        inInitParam = true;
                    }
                    // ignore other tags
                } else if (localName.equals("servlet")) {
                    inServlet = true;
                }
                // ignore other tags
            } else if (localName.equals("web-app")) {
                inWebApp = true;
            } else {
                throw new SAXException("unknown tag in root: " + localName);
            }
        }

        private void parseContent() {
            parseContent = true;
            curString.setLength(0);
        }

        /* (non-Javadoc)
         * @see org.xml.sax.helpers.DefaultHandler#characters(char[], int, int)
         */
        @Override
        public void characters(char[] ch, int start, int length)
                throws SAXException {
            if (parseContent) {
                curString.append(ch, start, length);
            }
        }

        /* (non-Javadoc)
         * @see org.xml.sax.helpers.DefaultHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
         */
        @Override
        public void endElement(String uri, String localName, String qName)
                throws SAXException {
            if (inWebApp) {
                if (inServlet) {
                    if (inInitParam) {
                        if (localName.equals("param-name")) {
                            parseContent = false;
                            curInitParamName = curString.toString();
                        } else if (localName.equals("param-value")) {
                            parseContent = false;
                            curInitParamValue = curString.toString();
                        } else if (localName.equals("init-param")) {
                            inInitParam = false;
                            curInitParams.put(curInitParamName, curInitParamValue);
                        } else {
                            throw new SAXException("unknown tag in <init-param>: " + localName);
                        }
                    } else if (localName.equals("servlet-class")) {
                        parseContent = false;
                        String servletClass = curString.toString();
                        if (servletClass.equals("de.zib.scalaris.examples.wikipedia.bliki.WikiServletScalaris")) {
                            inWikiServlet = true;
                        }
                    } else if (localName.equals("init-param")) {
                        throw new SAXException("closing </init-param> without matching start");
                    } else if (localName.equals("servlet")) {
                        if (inWikiServlet) {
                            initParams.putAll(curInitParams);
                        }
                        inServlet = false;
                        inWikiServlet = false;
                    }
                    // ignore other tags
                } else if (localName.equals("servlet")) {
                    throw new SAXException("closing </servlet> without matching start");
                }
                // ignore other tags
            } else if (localName.equals("web-app")) {
                inWebApp = false;
            } else {
                throw new SAXException("unknown tag in root: " + localName);
            }
        }
        
    }

    /**
     * Parses the given input file as a <tt>web.xml</tt> servlet descriptor for
     * servlet configuration options.
     * 
     * @param options
     *            the {@link Options} object to parse into
     * @param filename
     *            the name of the file to parse
     */
    public static void parseOptions(Options options, String filename) {
        WebXmlInitParamHandler handler;
        try {
            FileInputStream is = new FileInputStream(filename);
            BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            handler = new WebXmlInitParamHandler();
            XMLReader reader = XMLReaderFactory.createXMLReader();
            reader.setContentHandler(handler);
            reader.parse(new InputSource(br));
            parseOptions(options,
                    handler.initParams.get("SERVERNAME"),
                    handler.initParams.get("SERVERPATH"),
                    handler.initParams.get("WIKI_USE_BACKLINKS"),
                    handler.initParams.get("WIKI_SAVEPAGE_RETRIES"),
                    handler.initParams.get("WIKI_SAVEPAGE_RETRY_DELAY"),
                    handler.initParams.get("WIKI_PAGES_CACHE_IMPL"),
                    handler.initParams.get("WIKI_REBUILD_PAGES_CACHE"),
                    handler.initParams.get("WIKI_STORE_CONTRIBUTIONS"),
                    handler.initParams.get("WIKI_OPTIMISATIONS"),
                    handler.initParams.get("LOG_USER_REQS"),
                    handler.initParams.get("SCALARIS_NODE_DISCOVERY"));
        } catch (Exception e) {
            System.err.println("parsing failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
