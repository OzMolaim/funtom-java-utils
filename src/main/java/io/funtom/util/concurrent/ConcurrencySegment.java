package io.funtom.util.concurrent;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

final class ConcurrencySegment<K, V> {

    private final ConcurrentMap<K, Entry> store = new ConcurrentHashMap<>();
    private final Supplier<V> valuesSupplier;

    ConcurrencySegment(Supplier<V> valuesSupplier) {
        this.valuesSupplier = valuesSupplier;
    }

    V getValue(K key) {
        Entry newEntry;
        Entry oldEntry;
        do {
            oldEntry = store.get(key);
            if (oldEntry == null) {
                return getValueWhenNoEntry(key);
            } else {
                newEntry = new Entry(oldEntry.users + 1, oldEntry.value);
            }
        } while (!store.replace(key, oldEntry, newEntry));

        return newEntry.value;
    }

    private V getValueWhenNoEntry(K key) {
        Entry newEntry = new Entry(1, valuesSupplier.get());
        Entry prevEntry = store.putIfAbsent(key, newEntry);

        if (prevEntry == null) {
            return newEntry.value;
        } else {
            return getValue(key);
        }
    }

    void releaseKey(K key) {
        Entry newEntry;
        Entry oldEntry;
        do {
            oldEntry = store.get(key);
            newEntry = new Entry(oldEntry.users - 1, oldEntry.value);
        } while (!store.replace(key, oldEntry, newEntry));
    }

    private class Entry {

        private final int users;
        private final V value;

        Entry(int users, V value) {
            this.users = users;
            this.value = value;
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean equals(Object obj) {
            return ((Entry) obj).users == this.users;
        }

        @Override
        public int hashCode() {
            return users;
        }
    }
}
