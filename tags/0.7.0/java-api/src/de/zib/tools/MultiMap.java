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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Provides a multi-map, i.e. a map associating multiple values with a single
 * key. Wraps the an arbitrary {@link Map}-class.
 *
 * @param <T>
 *            wrapped map type
 * @param <K>
 *            key type
 * @param <V>
 *            value type
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.18
 * @since 3.18
 */
public class MultiMap<T extends Map<K, List<V>>, K, V> implements Map<K, List<V>> {

    protected final T data;

    /**
     * Constructs an empty map.
     *
     * @param clazz
     *            the {@link Map}-class to use for the internal representation
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <U extends Map> MultiMap(final Class<U> clazz) {
        super();
        try {
            data = (T) clazz.getConstructor().newInstance();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Constructs an empty map.
     *
     * @param clazz
     *            the {@link Map}-class to use for the internal representation
     * @param initialCapacity
     *            the initial capacity of the hash table
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <U extends Map> MultiMap(final Class<U> clazz, final int initialCapacity) {
        super();
        try {
            data = (T) clazz.getConstructor(int.class).newInstance(initialCapacity);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a map containing all values from another map.
     *
     * @param clazz
     *            the {@link Map}-class to use for the internal representation
     * @param other
     *            the map to copy the values from
     *
     * @see #putAll(Map)
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <U extends Map> MultiMap(final Class<U> clazz, final Map<? extends K, ? extends List<V>> other) {
        super();
        try {
            data = (T) clazz.getConstructor().newInstance();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        putAll(other);
    }

    /**
     * {@inheritDoc}
     */
    public void clear() {
        data.clear();
    }

    /**
     * {@inheritDoc}
     */
    public boolean containsKey(final Object key) {
        return data.containsKey(key);
    }

    /**
     * {@inheritDoc}
     */
    public boolean containsValue(final Object value) {
        for (final Entry<? extends K, ? extends List<V>> stat : data.entrySet()) {
            if (stat.getValue().contains(value)) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public Set<java.util.Map.Entry<K, List<V>>> entrySet() {
        return data.entrySet();
    }

    /**
     * {@inheritDoc}
     */
    public List<V> get(final Object key) {
        return data.get(key);
    }

    /**
     * {@inheritDoc}
     */
    public boolean isEmpty() {
        return data.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    public Set<K> keySet() {
        return data.keySet();
    }

    /**
     * Convenience method for inserting a single value into the map.
     *
     * @param key
     *            the key to insert at
     * @param value
     *            the value to insert
     *
     * @return the previous value associated with key, or <tt>null</tt> if there
     *         was no mapping for key. (A <tt>null</tt> return can also indicate
     *         that the map previously associated <tt>null</tt> with key, if the
     *         implementation supports <tt>null</tt> values.)
     */
    public List<V> put1(final K key, final V value) {
        final ArrayList<V> list = new ArrayList<V>();
        list.add(value);
        return put(key, list);
    }

    /**
     * {@inheritDoc}
     */
    public List<V> put(final K key, final List<V> value) {
        final List<V> l = data.get(key);
        if (l == null) {
            return data.put(key, value);
        } else {
            l.addAll(value);
            return data.put(key, l);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void putAll(final Map<? extends K, ? extends List<V>> m) {
        for (final Entry<? extends K, ? extends List<V>> value : m.entrySet()) {
            put(value.getKey(), value.getValue());
        }
    }

    /**
     * {@inheritDoc}
     */
    public List<V> remove(final Object key) {
        return data.remove(key);
    }

    /**
     * {@inheritDoc}
     */
    public int size() {
        return data.size();
    }

    /**
     * {@inheritDoc}
     */
    public Collection<List<V>> values() {
        return data.values();
    }

    @Override
    public String toString() {
        return data.toString();
    }

}