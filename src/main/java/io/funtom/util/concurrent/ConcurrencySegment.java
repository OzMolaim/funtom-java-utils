package io.funtom.util.concurrent;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

final class ConcurrencySegment<K, V> {

    private final Map<K, V> keyToValue = new HashMap<>();
    private final Map<K, Integer> keyUsersCount = new HashMap<>();
    private final Supplier<V> valuesSupplier;

    ConcurrencySegment(Supplier<V> valuesSupplier) {
        this.valuesSupplier = valuesSupplier;
    }

    synchronized V getValue(K key) {
        V result;
        Integer currentUsers = keyUsersCount.get(key);
        if (currentUsers == null) {
            keyUsersCount.put(key, 1);
            result = valuesSupplier.get();
            keyToValue.put(key, result);
        } else {
            keyUsersCount.put(key, currentUsers + 1);
            result = keyToValue.get(key);
        }
        return result;
    }

    synchronized void releaseKey(K key) {
        int currentUsers = keyUsersCount.get(key);
        if (currentUsers == 1) {
            keyUsersCount.remove(key);
            keyToValue.remove(key);
        } else {
            keyUsersCount.put(key, currentUsers - 1);
        }
    }
}
