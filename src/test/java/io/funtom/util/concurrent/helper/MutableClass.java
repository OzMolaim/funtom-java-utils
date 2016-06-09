package io.funtom.util.concurrent.helper;

import org.junit.Assert;

public class MutableClass {

    private long n1 = 0;
    private long n2 = 0;
    private long n3 = 0;
    private long n4 = 0;
    private long n5 = 0;

    public void executeNonAtomicMutation() {
        n1++;
        n2++;
        n3++;
        n4++;
        n5++;
    }

    public void assertConsistency() {
        assertNumberOfMutations(n1);
    }

    public void assertNumberOfMutations(long expected) {
        Assert.assertEquals(expected, n1);
        Assert.assertEquals(expected, n2);
        Assert.assertEquals(expected, n3);
        Assert.assertEquals(expected, n4);
        Assert.assertEquals(expected, n5);
    }

    @Override
    public String toString() {
        return String.format("%d, %d, %d, %d, %d", n1, n2, n3, n4, n5);
    }
}