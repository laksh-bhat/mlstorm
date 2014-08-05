package utils;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by lakshmisha.bhat on 7/28/14.
 */
public class Pair<K, V> implements Map.Entry<K, V>, Serializable {
    private K key;
    private V value;

    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V v) {
        value = v;
        return value;
    }
}
