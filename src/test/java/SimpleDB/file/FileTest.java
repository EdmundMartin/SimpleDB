package SimpleDB.file;

import SimpleDB.server.SimpleDB;

import java.io.IOException;

public class FileTest {

    // TODO - Turn into a unit test
    public static void main(String[] args) throws IOException {
        SimpleDB db = new SimpleDB("filetest", 400, 8);
        FileManager fm = db.fileManager();

        BlockId blk = new BlockId("testfile", 2);
        int position1 = 88;

        Page p1 = new Page(fm.blockSize());
        p1.setString(position1, "abcdefghijklm");
        int size = Page.maxLength("abcdefghijklm".length());
        int pos2 = position1 + size;
        p1.setInt(pos2, 345);
        fm.write(blk, p1);

        Page p2 = new Page(fm.blockSize());
        fm.read(blk, p2);
        System.out.println("offset " + pos2 + " contains " + p2.getInt(pos2));
        System.out.println("offset " + position1 + " contains " + p2.getString(position1));
    }
}
