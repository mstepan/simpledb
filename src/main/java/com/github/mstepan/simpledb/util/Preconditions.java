package com.github.mstepan.simpledb.util;

public final class Preconditions {

    private Preconditions() {
        throw new AssertionError("Can't instantiate utility-only class");
    }

    public static void checkArguments(boolean predicate, String errorMessage) {
        if (!predicate) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    public static void checkState(boolean predicate, String errorMessage) {
        if (!predicate) {
            throw new IllegalStateException(errorMessage);
        }
    }
}
