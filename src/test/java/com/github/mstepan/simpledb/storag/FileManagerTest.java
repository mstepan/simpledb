package com.github.mstepan.simpledb.storag;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.mstepan.simpledb.storage.AccessStatisticSnapshot;
import com.github.mstepan.simpledb.storage.BlockId;
import com.github.mstepan.simpledb.storage.FileManager;
import com.github.mstepan.simpledb.storage.Page;
import java.util.UUID;
import org.junit.jupiter.api.Test;

final class FileManagerTest {

    @Test
    void storeLoadMixedData() {

        final String fileName = "%s.dat".formatted(UUID.randomUUID().toString());

        FileManager fileManager =
                FileManager.getInstance("C:\\Users\\maksym\\repo\\simpledb\\data", 100);

        Page page = new Page(fileManager.blockSize());
        page.putString(0, "Hello, world!!!");
        page.putInt(50, 123);

        fileManager.write(new BlockId(fileName, 0), page);

        Page newPage = new Page(fileManager.blockSize());
        fileManager.read(new BlockId(fileName, 0), newPage);

        assertEquals("Hello, world!!!", newPage.getString(0));
        assertEquals(123, newPage.getInt(50));

        assertEquals(new AccessStatisticSnapshot(1, 1), fileManager.stats());
    }
}
