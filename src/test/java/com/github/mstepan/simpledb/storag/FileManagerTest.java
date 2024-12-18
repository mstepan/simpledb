package com.github.mstepan.simpledb.storag;

import static com.github.mstepan.simpledb.util.Preconditions.checkState;
import static org.junit.jupiter.api.Assertions.*;

import com.github.mstepan.simpledb.storage.AccessStatisticSnapshot;
import com.github.mstepan.simpledb.storage.BlockId;
import com.github.mstepan.simpledb.storage.FileManager;
import com.github.mstepan.simpledb.storage.Page;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Date;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

final class FileManagerTest {

    private static final File DB_FOLDER = createDbFolderIfNotExist();
    private static final String DB_FOLDER_PATH = DB_FOLDER.getAbsolutePath();

    private static File createDbFolderIfNotExist() {

        ClassLoader classLoader = FileManagerTest.class.getClassLoader();

        // get root of 'resources' folder under test
        URL resource = classLoader.getResource("");

        checkState(resource != null, "Root resources folder not found");

        File dbFolder = Paths.get(resource.getPath().replaceAll(":", "")).resolve("data").toFile();

        if (!dbFolder.exists()) {
            checkState(dbFolder.mkdirs(), "Can't create DB folder for unit tests");
        }

        return dbFolder;
    }

    @BeforeAll
    static void beforeAll() {
        try {
            deleteDirectory(DB_FOLDER);
        } catch (IOException ioEx) {
            throw new IllegalStateException(
                    "Can't delete DB folder %s".formatted(DB_FOLDER_PATH), ioEx);
        }
    }

    @Test
    void storeLoadMixedData() {

        final String fileName = "storeLoadMixedData.dat";

        FileManager fileManager = FileManager.getInstance(DB_FOLDER_PATH, 512);

        Page page = new Page(fileManager.blockSize());

        // put primitive values
        page.putChar(0, 'A');
        page.putInt(5, 111);
        page.putLong(10, 777L);
        page.putBoolean(20, true);
        page.putBoolean(21, false);

        // put byte[] array and String (including C-like string)
        page.putBytes(100, new byte[] {10, 20, 30, 40, 50});
        page.putString(120, "Hello, world!!!");
        page.putStringC(150, "c-like string");

        // put Date
        final Date now = new Date();
        page.putDate(200, now);

        fileManager.write(new BlockId(fileName, 0), page);

        Page newPage = new Page(fileManager.blockSize());
        fileManager.read(new BlockId(fileName, 0), newPage);

        // check primitive values
        assertEquals('A', newPage.getChar(0));
        assertEquals(111, newPage.getInt(5));
        assertEquals(777, newPage.getLong(10));
        assertTrue(newPage.getBoolean(20));
        assertFalse(newPage.getBoolean(21));

        // check byte[] array and Strings
        assertArrayEquals(new byte[] {10, 20, 30, 40, 50}, newPage.getBytes(100));
        assertEquals("Hello, world!!!", newPage.getString(120));
        assertEquals("c-like string", newPage.getStringC(150));

        // check Date
        assertEquals(now, newPage.getDate(200));

        assertEquals(new AccessStatisticSnapshot(1, 1), fileManager.stats());
    }

    private static void deleteDirectory(File dirOrFile) throws IOException {
        if (dirOrFile.isDirectory()) {
            File[] files = dirOrFile.listFiles();
            if (files != null) {
                for (File singleFile : files) {
                    deleteDirectory(singleFile);
                }
            }
        } else {
            assertTrue(
                    dirOrFile.delete(),
                    "Can't delete file or folder %s".formatted(dirOrFile.getAbsolutePath()));
        }
    }
}
