package com.github.mstepan.simpledb.storage;

public record BlockId(String fileName, int blockNumber) {

    @Override
    public String toString() {
        return "[file='%s', no='%d']".formatted(fileName, blockNumber);
    }
}
