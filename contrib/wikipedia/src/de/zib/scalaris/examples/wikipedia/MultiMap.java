package de.zib.scalaris.examples.wikipedia;

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
    public <U extends Map> MultiMap(Class<U> clazz) {
        super();
        try {
            data = (T) clazz.getConstructor().newInstance();
        } catch (Exception e) {
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
    public <U extends Map> MultiMap(Class<U> clazz, Map<? extends K, ? extends List<V>> other) {
        super();
        try {
            data = (T) clazz.getConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        putAll(other);
    }

    @Override
    public void clear() {
        data.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        return data.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        for (Entry<? extends K, ? extends List<V>> stat : data.entrySet()) {
            if (stat.getValue().contains(value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Set<java.util.Map.Entry<K, List<V>>> entrySet() {
        return data.entrySet();
    }

    @Override
    public List<V> get(Object key) {
        return data.get(key);
    }

    @Override
    public boolean isEmpty() {
        return data.isEmpty();
    }

    @Override
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
        List<V> l = data.get(key);
        if (l == null) {
            return data.put(key, value);
        } else {
            l.addAll(value);
            return data.put(key, l);
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
        return data.remove(key);
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public Collection<List<V>> values() {
        return data.values();
    }

}