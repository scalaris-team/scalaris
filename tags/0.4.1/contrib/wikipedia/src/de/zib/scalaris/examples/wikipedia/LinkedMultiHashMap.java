/**
 *  Copyright 2012 Zuse Institute Berlin
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Provides a multi-map, i.e. a map associating multiple values with a single
 * key. Wraps the {@link LinkedHashMap} class.
 * 
 * @param <K>
 *            key type
 * @param <V>
 *            value type
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class LinkedMultiHashMap<K, V> implements Map<K, List<V>> {
    final protected Map<K, List<V>> stats = new LinkedHashMap<K, List<V>>();

    /**
     * Constructs an empty map.
     */
    public LinkedMultiHashMap() {
    }
    
    /**
     * Creates a map containing all values from another map.
     * 
     * @param other
     *            the map to copy the values from
     * 
     * @see #putAll(Map)
     */
    public LinkedMultiHashMap(Map<? extends K, ? extends List<V>> other) {
        putAll(other);
    }
    
    @Override
    public void clear() {
        stats.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        return stats.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return stats.containsValue(value);
    }

    @Override
    public Set<java.util.Map.Entry<K, List<V>>> entrySet() {
        return stats.entrySet();
    }

    @Override
    public List<V> get(Object key) {
        return stats.get(key);
    }

    @Override
    public boolean isEmpty() {
        return stats.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return stats.keySet();
    }

    /**
     * Convenience method for inserting a single value into the map.
     * 
     * @param key
     *            the key to insert at
     * @param value
     *            the value to insert
     * 
     * @return the previous value associated with key, or null if there was no
     *         mapping for key. (A null return can also indicate that the map
     *         previously associated null with key, if the implementation
     *         supports null values.)
     */
    public List<V> put(K key, V value) {
        ArrayList<V> list = new ArrayList<V>();
        list.add(value);
        return put(key, list);
    }

    @Override
    public List<V> put(K key, List<V> value) {
        List<V> l = stats.get(key);
        if (l == null) {
            return stats.put(key, value);
        } else {
            l.addAll(value);
            return stats.put(key, l);
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends List<V>> m) {
        for (Entry<? extends K, ? extends List<V>> value : m.entrySet()) {
            put(value.getKey(), value.getValue());
        }
    }

    @Override
    public List<V> remove(Object key) {
        return stats.remove(key);
    }

    @Override
    public int size() {
        return stats.size();
    }

    @Override
    public Collection<List<V>> values() {
        return stats.values();
    }
}
