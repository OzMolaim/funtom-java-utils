package io.funtom.util.concurrent;

final class HashUtil {

    private HashUtil() {
    }

    static int boundedHash(Object o, int upperBoundExclusive) {
        int h = o.hashCode();

        // Protection against poor hash functions.
        // Used by java.util.concurrent.ConcurrentHashMap
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h << 3);
        h ^= (h >>> 6);
        h += (h << 2) + (h << 14);
        h ^= (h >>> 16);

        return Math.abs(h % upperBoundExclusive);
    }
}
