package SimpleDB.server;

import SimpleDB.buffer.BufferManager;
import SimpleDB.file.FileManager;
import SimpleDB.log.LogManager;
import SimpleDB.metadata.MetadataManager;
import SimpleDB.transaction.Transaction;

import java.io.File;

public class SimpleDB {


    public static int BLOCK_SIZE = 400;
    public static int BUFFER_SIZE = 8;
    public static String LOG_FILE = "simpledb.log";

    private final FileManager fileManager;
    private final LogManager logManager;
    private final BufferManager bufferManager;
    private MetadataManager metadataManager;

    public SimpleDB(String dirname, int blockSize, int bufferSize) {
        File dbDirectory = new File(dirname);
        this.fileManager = new FileManager(dbDirectory, blockSize);
        this.logManager = new LogManager(fileManager, LOG_FILE);
        this.bufferManager = new BufferManager(fileManager, logManager, BUFFER_SIZE);
    }

    public SimpleDB(String directoryName) {
        this(directoryName, BLOCK_SIZE, BUFFER_SIZE);
        Transaction tx = new Transaction(fileManager, logManager, bufferManager);
        boolean isNew = fileManager.isNew();
        if (isNew) {
            System.out.println("creating new database");
        } else {
            System.out.println("recovering existing database");
            tx.recover();
        }
        metadataManager = new MetadataManager(isNew, tx);
        tx.commit();
    }

    public FileManager fileManager() {
        return this.fileManager;
    }

    public LogManager logManager() {
        return this.logManager;
    }
}
