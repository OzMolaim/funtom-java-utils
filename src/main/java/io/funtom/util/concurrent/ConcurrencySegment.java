package io.funtom.util.concurrent;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

final class ConcurrencySegment<K, V> {

    private final Map<K, Entry> store = new HashMap<K, Entry>();
    private final Supplier<V> valuesSupplier;

    ConcurrencySegment(Supplier<V> valuesSupplier) {
        this.valuesSupplier = valuesSupplier;
    }

    synchronized V getValue(K key) {
        Entry current = store.get(key);
        if (current == null) {
            current = new Entry();
            store.put(key, current);
        } else {
            current.users++;
        }
        return current.value;
    }

    synchronized void releaseKey(K key) {
        Entry current = store.get(key);
        if (current.users == 1) {
            store.remove(key);
        } else {
            current.users--;
        }
    }

    private class Entry {
        private int users = 1;
        private V value = valuesSupplier.get();
    }
}
