/**
 *  Copyright 2011 Zuse Institute Berlin
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

import java.util.EnumMap;


/**
 * @author Nico Kruber, kruber@zib.de
 *
 */
public class Options {
    
    /**
     * Whether to support back-links ("what links here?") or not.
     */
    public static boolean WIKI_USE_BACKLINKS = true;
    
    /**
     * How often to re-try a "sage page" operation in case of failures, e.g.
     * concurrent edits.
     * 
     * @see #WIKI_SAVEPAGE_RETRY_DELAY
     */
    public static int WIKI_SAVEPAGE_RETRIES = 0;
    
    /**
     * How long to wait after a failed "sage page" operation before trying
     * again (in milliseconds).
     * 
     * @see #WIKI_SAVEPAGE_RETRIES
     */
    public static int WIKI_SAVEPAGE_RETRY_DELAY = 10;
    
    /**
     * How often to re-create the bloom filter with the existing pages (in
     * seconds). The bloom filter will be disabled if a value less than or equal
     * to 0 is provided.
     */
    public static int WIKI_REBUILD_PAGES_CACHE = 10 * 60;
    
    /**
     * How often to re-create the bloom filter with the existing pages (in
     * seconds). The bloom filter will be disabled if a value less than or equal
     * to 0 is provided.
     */
    public static STORE_CONTRIB_TYPE WIKI_STORE_CONTRIBUTIONS = STORE_CONTRIB_TYPE.OUTSIDE_TX;
    
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
    }

    /**
     * Indicates that the new append and increment operations of Scalaris should
     * be used, i.e.
     * {@link de.zib.scalaris.Transaction#addDelOnList(String, java.util.List, java.util.List)}
     * and {@link de.zib.scalaris.Transaction#addOnNr(String, Object)}.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static class APPEND_INCREMENT implements Optimisation {
    }


    /**
     * Indicates that the new append and increment operations of Scalaris should
     * be used and list values should be split among several partions, i.e.
     * buckets, depending on the value's hash.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static class APPEND_INCREMENT_BUCKETS_WITH_HASH implements Optimisation {
        final protected int buckets;
        
        /**
         * Constructor.
         * 
         * @param buckets
         *            number of available buckets
         */
        public APPEND_INCREMENT_BUCKETS_WITH_HASH(int buckets) {
            this.buckets = buckets;
        }

        /**
         * Gets the number of available buckets
         * 
         * @return number of buckets
         */
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
        public <T> String getBucketString(final T value) {
            if (buckets > 1) {
                return ":" + (value.hashCode() % buckets);
            } else {
                return "";
            }
        }
    }
    
    /**
     * Optimisations to use for the different Scalaris operations.
     */
    final public static EnumMap<ScalarisOpType, Optimisation> OPTIMISATIONS = new EnumMap<ScalarisOpType, Options.Optimisation>(
            ScalarisOpType.class);
    
    static {
        for (ScalarisOpType op : ScalarisOpType.values()) {
            OPTIMISATIONS.put(op, new APPEND_INCREMENT());
        }
    }
}
