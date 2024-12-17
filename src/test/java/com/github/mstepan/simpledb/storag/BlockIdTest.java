package com.github.mstepan.simpledb.storag;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.github.mstepan.simpledb.storage.BlockId;
import org.junit.jupiter.api.Test;

final class BlockIdTest {

    @Test
    void equalsAndHashCodeTest() {
        BlockId baseBlock = new BlockId("file1.dat", 1);
        BlockId similarBlock = new BlockId("file1.dat", 1);

        assertEquals(baseBlock, similarBlock);
        assertEquals(similarBlock, baseBlock);
        assertEquals(baseBlock.hashCode(), similarBlock.hashCode());

        BlockId sameFileDifNumber = new BlockId("file1.dat", 2);
        assertNotEquals(baseBlock, sameFileDifNumber);
        assertNotEquals(sameFileDifNumber, baseBlock);
        assertNotEquals(baseBlock.hashCode(), sameFileDifNumber.hashCode());

        BlockId difFileSameNumber = new BlockId("file2.dat", 1);
        assertNotEquals(baseBlock, difFileSameNumber);
        assertNotEquals(difFileSameNumber, baseBlock);
        assertNotEquals(baseBlock.hashCode(), difFileSameNumber.hashCode());

        BlockId difFileDifNumber = new BlockId("file2.dat", 333);
        assertNotEquals(baseBlock, difFileDifNumber);
        assertNotEquals(difFileDifNumber, baseBlock);
        assertNotEquals(baseBlock.hashCode(), difFileDifNumber.hashCode());
    }

    @Test
    void toStringTest() {
        BlockId block = new BlockId("file1.dat", 5);
        assertEquals("[file='file1.dat', no='5']", block.toString());
    }
}
