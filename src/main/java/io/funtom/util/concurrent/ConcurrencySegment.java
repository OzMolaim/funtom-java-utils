package io.funtom.util.concurrent;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

final class ConcurrencySegment<K, V> {

    private final Map<K, V> executors = new HashMap<>();
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
            executors.put(key, result);
        } else {
            keyUsersCount.put(key, currentUsers + 1);
            result = executors.get(key);
        }
        return result;
    }

    synchronized void releaseKey(K key) {
        int currentUsers = keyUsersCount.get(key);
        if (currentUsers == 1) {
            keyUsersCount.remove(key);
            executors.remove(key);
        } else {
            keyUsersCount.put(key, currentUsers - 1);
        }
    }
}
