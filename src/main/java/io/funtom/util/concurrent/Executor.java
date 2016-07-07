package io.funtom.util.concurrent;

import java.util.function.Supplier;

/**
 * An extension to the java.util.concurrent.Executor.
 * This executor adds the ability to execute tasks (- code blocks) that return a value.
 */
public interface Executor extends java.util.concurrent.Executor {
    <R> R execute(Supplier<R> task);
}
