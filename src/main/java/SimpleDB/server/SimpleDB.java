package SimpleDB.server;

import SimpleDB.file.FileManager;
import SimpleDB.log.LogManager;

import java.io.File;

public class SimpleDB {


    public static int BLOCK_SIZE = 400;
    public static int BUFFER_SIZE = 8;
    public static String LOG_FILE = "simpledb.log";

    private final FileManager fileManager;
    private final LogManager logManager;

    public SimpleDB(String dirname, int blockSize, int bufferSize) {
        File dbDirectory = new File(dirname);
        this.fileManager = new FileManager(dbDirectory, blockSize);
        this.logManager = new LogManager(fileManager, LOG_FILE);
    }

    public FileManager fileManager() {
        return this.fileManager;
    }

    public LogManager logManager() {
        return this.logManager;
    }
}
