package com.github.mstepan.simpledb.storage;

public record AccessStatisticSnapshot(int blocksReadCount, int blocksWriteCount) {}
