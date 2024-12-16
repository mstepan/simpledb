package com.github.mstepan.simpledb;

import com.github.mstepan.simpledb.storage.BlockId;
import com.github.mstepan.simpledb.storage.FileManager;
import com.github.mstepan.simpledb.storage.Page;

public class SimpleDbMain {

    public static void main() {

        final String fileName = "1.dat";

        FileManager fileManager =
                FileManager.getInstance("C:\\Users\\maksym\\repo\\simpledb\\data", 100);

        Page page = new Page(fileManager.blockSize());
        page.putString(0, "Hello, world!!!");
        page.putInt(50, 123);

        fileManager.write(new BlockId(fileName, 0), page);

        Page newPage = new Page(fileManager.blockSize());
        fileManager.read(new BlockId(fileName, 0), newPage);

        String strFromPage = newPage.getString(0);
        int intFromPage = newPage.getInt(50);
        System.out.printf("Read from file, str: '%s', int: %d %n", strFromPage, intFromPage);

        System.out.println("SimpleDb main done...");
    }
}
