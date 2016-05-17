package io.funtom.util.concurrent;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class ConcurrentBuffer<T> {

    private final Queue<T> buffer = new ConcurrentLinkedQueue<>();

    public void add(T e) {
        buffer.add(e);
    }

    public void addAll(Collection<T> elements) {
        buffer.addAll(elements);
    }

    public synchronized List<T> getAndRemoveAll() {
        List<T> result = new ArrayList<>();
        for (Iterator<T> it = buffer.iterator() ; it.hasNext() ; ) {
            result.add(it.next());
            it.remove();
        }
        return result;
    }
}
