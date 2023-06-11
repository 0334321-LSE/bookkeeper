package org.apache.bookkeeper.client.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class InvalidMap<K, V> implements Map<K, V> {

    private Map<String, Double> doubleMap;

    public InvalidMap(){
        this.doubleMap = new HashMap<>();
    }

/** <p>I try to overwrite put: now it writes a double (so not bytes) mandatory <br />
 * Like this i can create an invalidMap with right <K,V> types, but indeed type are chosen default by me.</p>
         */
    @Override
    public Object put(Object key, Object value) {
        String invalidTypeKey = key.toString();
        double invalidValue = 66.6;
        return doubleMap.put(invalidTypeKey,invalidValue);
    }

    @Override
    public V get(Object key) {
        return (V) doubleMap.get(key);
    }


    @Override
    public int size() {
        return 1;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(Object key) {
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public V remove(Object key) {
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {

    }

    @Override
    public void clear() {

    }

    @Override
    public Set<K> keySet() {
        return null;
    }

    @Override
    public Collection<V> values() {
        return null;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return null;
    }



}
