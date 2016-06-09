package io.funtom.util.concurrent;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A concurrent buffer which supports additions of single elements or collections of elements, and also supports removals of all the elements or up to 'n' elements.
 * Addition operations are non-blocking. Adding a collection of elements is not guaranteed to be atomic.
 * Removal operations are always synchronized and performed in an atomic batch. Thread may be suspended if a batch is currently removed from the buffer.
 * The elements being remove from the buffer returned FIFO ordered.
 * A thread that removes elements from the buffer is guaranteed to see the most updated values in the Buffer at the time the removal began.
 * Subsequent additions during the removals may be returned but are not guaranteed to be returned.
 *
 * @param <T> The type of the elements in the Buffer
 */
public final class ConcurrentBuffer<T> {

    private final Queue<T> buffer = new ConcurrentLinkedQueue<>();

    /**
     * Add single element to the buffer
     * @param e An element to be added to the buffer.
     */
    public void add(T e) {
        buffer.add(e);
    }

    /**
     * Add collection of elements to the buffer.
     * The operation is not guaranteed to be atomic.
     * @param elements A collection of elements to be added to the buffer.
     */
    public void addAll(Collection<T> elements) {
        buffer.addAll(elements);
    }

    /**
     * Get an remove all the current elements in the buffer.
     * Calling thread may be suspended since only one thread can remove from the buffer at a time.
     *
     * @return A list contains all the element in the buffer at the time the removal began.
     *          Elements that where added to the buffer during the the removal may be returned but it is not guaranteed
     */
    public synchronized List<T> getAndRemoveAll() {
        List<T> result = new ArrayList<>();
        for (Iterator<T> it = buffer.iterator() ; it.hasNext() ; ) {
            result.add(it.next());
            it.remove();
        }
        return result;
    }

    /**
     * Same as {@link #getAndRemoveAll} but only up to maxElementsToRemove elements will be returned.
     *
     * @param maxElementsToRemove - The maximum number elements to be returned from the buffer in the batch. If maxElementsToRemove = 0 returns an empty list.
     *
     * @throws IllegalArgumentException - If maxElementsToRemove is negative.
     *
     * @return A list contains up to maxElementsToRemove elements, which where in the buffer at the time the removal began.
     *          If they where maxElementsToRemove elements in the buffer at the time the removal began they are guaranteed to be returned.
     *          Elements that where added to the buffer during the the removal may be returned but that is not guaranteed.
     */
    public synchronized List<T> getAndRemove(int maxElementsToRemove) {

        if (maxElementsToRemove < 0)
            throw new IllegalArgumentException(Integer.toString(maxElementsToRemove));

        int remaining = maxElementsToRemove;

        List<T> result = new ArrayList<>();
        for (Iterator<T> it = buffer.iterator() ; it.hasNext() && remaining > 0 ; ) {
            result.add(it.next());
            it.remove();
            remaining--;
        }
        return result;
    }
}
