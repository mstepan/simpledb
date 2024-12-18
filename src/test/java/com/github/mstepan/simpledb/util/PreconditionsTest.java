package com.github.mstepan.simpledb.util;

import static com.github.mstepan.simpledb.util.Preconditions.checkArguments;
import static com.github.mstepan.simpledb.util.Preconditions.checkState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class PreconditionsTest {

    @Test
    @SuppressWarnings("all")
    void checkArgumentNormalCases() {
        checkArguments("".isEmpty(), "string not empty");
        checkArguments(20 > 10, "20 <= 10");
    }

    @Test
    @SuppressWarnings("all")
    void checkArgumentFailedCase() {
        Throwable ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> checkArguments("hello".isEmpty(), "string not empty"));

        assertEquals("string not empty", ex.getMessage());
    }

    @Test
    @SuppressWarnings("all")
    void checkStateNormalCases() {
        checkState("".isEmpty(), "string not empty");
        checkState(20 > 10, "20 <= 10");
    }

    @Test
    @SuppressWarnings("all")
    void checkStateFailedCase() {
        Throwable ex =
                assertThrows(
                        IllegalStateException.class,
                        () -> checkState("hello".isEmpty(), "string not empty"));

        assertEquals("string not empty", ex.getMessage());
    }
}
