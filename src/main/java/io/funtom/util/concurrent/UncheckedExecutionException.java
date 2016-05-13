package io.funtom.util.concurrent;

public class UncheckedExecutionException extends RuntimeException {

    private static final long serialVersionUID = -9113509948641626834L;

    UncheckedExecutionException(Throwable cause) {
        super("Exception during task execution", cause);
    }
}
