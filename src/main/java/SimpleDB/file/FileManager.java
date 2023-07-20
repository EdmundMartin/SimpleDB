package SimpleDB.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

public class FileManager {
    private final File dbDirectory;
    private final int blockSize;
    private final boolean isNew;
    private final Map<String, RandomAccessFile> openFiles = new HashMap<>();


    // TODO better error handling
    public FileManager(File dbDirectory, int blockSize) {
        this.dbDirectory = dbDirectory;
        this.blockSize = blockSize;
        this.isNew = !dbDirectory.exists();

        if (this.isNew) {
            dbDirectory.mkdirs();
        }

        // remove temporary tables
        for (String filename: dbDirectory.list()) {
            if (filename.startsWith("temp")) {
                new File(dbDirectory, filename).delete();
            }
        }
    }

    public synchronized void read(BlockId blockId, Page page) {
        try {
            RandomAccessFile f = getFile(blockId.fileName());
            f.seek(blockId.number() * blockSize);
            f.getChannel().read(page.contents());
        } catch (IOException e) {
            throw new RuntimeException("cannot read block: " + blockId);
        }
    }

    public synchronized void write(BlockId blockId, Page page) {
        try {
            RandomAccessFile f = getFile(blockId.fileName());
            f.seek(blockId.number() * blockSize);
            f.getChannel().write(page.contents());
        } catch (IOException e) {
            throw new RuntimeException("cannot write block: " + blockId);
        }
    }

    public synchronized BlockId append(String filename) {
        int newBlockNum = length(filename);
        BlockId blk = new BlockId(filename, newBlockNum);
        byte[] bytes = new byte[blockSize];
        try {
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * blockSize);
            f.write(bytes);
        } catch (IOException e) {
            throw new RuntimeException("cannot append block: " + blk);
        }
        return blk;
    }

    public int length(String filename) {
        try {
            RandomAccessFile f = getFile(filename);
            return (int)(f.length() / blockSize);
        } catch (IOException e) {
            throw new RuntimeException("cannot access: " + filename);
        }
    }

    public boolean isNew() {
        return isNew;
    }

    public int blockSize() {
        return blockSize;
    }

    private RandomAccessFile getFile(String filename) throws IOException {
        RandomAccessFile f = openFiles.get(filename);
        if (f == null) {
            File dbTable = new File(dbDirectory, filename);
            f = new RandomAccessFile(dbTable, "rws");
            openFiles.put(filename, f);
        }
        return f;
    }
}
