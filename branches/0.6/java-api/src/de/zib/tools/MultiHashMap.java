/**
 *  Copyright 2012-2013 Zuse Institute Berlin
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
package de.zib.tools;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides a multi-map, i.e. a map associating multiple values with a single
 * key. Wraps the {@link HashMap} class.
 *
 * @param <K>
 *            key type
 * @param <V>
 *            value type
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.18
 * @since 3.18
 */
public class MultiHashMap<K, V> extends MultiMap<HashMap<K, List<V>>, K, V> {
    /**
     * Constructs an empty map.
     */
    public MultiHashMap() {
        super(HashMap.class);
    }

    /**
     * Constructs an empty map.
     *
     * @param initialCapacity
     *            the initial capacity of the hash table
     */
    public MultiHashMap(final int initialCapacity) {
        super(HashMap.class, initialCapacity);
    }

    /**
     * Creates a map containing all values from another map.
     *
     * @param other
     *            the map to copy the values from
     *
     * @see #putAll(Map)
     */
    public MultiHashMap(final Map<? extends K, ? extends List<V>> other) {
        super(HashMap.class, other);
    }
}
