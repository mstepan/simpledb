package com.github.mstepan.simpledb.storage;

import static com.github.mstepan.simpledb.util.Preconditions.checkArguments;
import static com.github.mstepan.simpledb.util.Preconditions.checkState;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class FileManager {

    private static FileManager INST;

    public static synchronized FileManager getInstance(String folder, int blockSize) {
        if (INST == null) {
            INST = new FileManager(folder, blockSize);
        }

        return INST;
    }

    private final Map<String, RandomAccessFile> openedFiles = new HashMap<>();

    private final String folderPath;
    private final int blockSize;
    private final AccessStatistic stats = new AccessStatistic();

    private FileManager(String folderPath, int blockSize) {
        checkArguments(folderPath != null, "Can't create DB with null DB folder");
        checkArguments(
                blockSize > 0,
                "Block size should be greater than 0, blockSize: %d".formatted(blockSize));

        this.folderPath = folderPath;
        this.blockSize = blockSize;

        final File folder = Path.of(folderPath).toFile();

        checkState(
                folder.isDirectory(),
                "DB folder should be a directory path, but found '%s'"
                        .formatted(folder.getAbsolutePath()));

        if (!folder.exists()) {
            checkState(
                    folder.mkdirs(),
                    "Can't create directory '%s'".formatted(folder.getAbsolutePath()));
        }

        for (File tempFile :
                Objects.requireNonNull(
                        folder.listFiles((dirNotUsed, fileName) -> fileName.startsWith("temp")))) {
            checkState(
                    tempFile.delete(),
                    "Can't delete temp file '%s'".formatted(tempFile.getAbsolutePath()));
        }
    }

    public int blockSize() {
        return blockSize;
    }

    public AccessStatisticSnapshot stats() {
        return new AccessStatisticSnapshot(stats.blocksReadCount, stats.blocksWriteCount);
    }

    /** Reads block content from disk into in-memory page. */
    public synchronized void read(BlockId blockId, Page page) {
        checkArguments(blockId != null, "blockId is null");
        checkArguments(page != null, "page is null");

        RandomAccessFile randomAccessFile = getFile(blockId.fileName());

        try {
            randomAccessFile.seek((long) blockId.blockNumber() * blockSize);
            randomAccessFile.getChannel().read(page.content());
            ++stats.blocksReadCount;
        } catch (IOException ioEx) {
            throw new IllegalStateException("Can't read block %s".formatted(blockId), ioEx);
        }
    }

    /** Write in-memory page content into file block */
    public synchronized void write(BlockId blockId, Page page) {
        RandomAccessFile randomAccessFile = getFile(blockId.fileName());
        try {
            randomAccessFile.seek((long) blockId.blockNumber() * blockSize);
            int writtenBytes = randomAccessFile.getChannel().write(page.content());
            checkState(
                    writtenBytes == blockSize,
                    "Page was partially written to a block, expected bytes to be "
                            + "written %d but actually was written %d"
                                    .formatted(blockSize, writtenBytes));
            ++stats.blocksWriteCount;
        } catch (IOException ioEx) {
            throw new IllegalStateException("Can't write block %s".formatted(blockId), ioEx);
        }
    }

    /** Append one page sized block at the end of this file. */
    public synchronized BlockId append(String fileName) {
        RandomAccessFile randomAccessFile = getFile(fileName);

        try {
            final long newBlockNumber = randomAccessFile.length() / blockSize;

            randomAccessFile.seek(newBlockNumber * blockSize);
            randomAccessFile.write(new byte[blockSize]);
            ++stats.blocksWriteCount;

            return new BlockId(fileName, (int) newBlockNumber);
        } catch (IOException ioEx) {
            throw new IllegalStateException("Can't append to file %s".formatted(fileName), ioEx);
        }
    }

    private RandomAccessFile getFile(String fileName) {
        File fileToSearch = Path.of(folderPath, fileName).toFile();

        try {
            String fileKey = fileToSearch.getAbsolutePath();
            RandomAccessFile randomAccessFileFromCache = openedFiles.get(fileKey);

            if (randomAccessFileFromCache != null) {
                return randomAccessFileFromCache;
            }

            RandomAccessFile newFile = new RandomAccessFile(fileToSearch, "rws");

            openedFiles.put(fileKey, newFile);

            return newFile;
        } catch (FileNotFoundException fileNotFoundEx) {
            throw new IllegalStateException(fileNotFoundEx);
        }
    }

    /** Track statistic related to count of read/write blocks. */
    private static final class AccessStatistic {
        int blocksReadCount;
        int blocksWriteCount;
    }
}
