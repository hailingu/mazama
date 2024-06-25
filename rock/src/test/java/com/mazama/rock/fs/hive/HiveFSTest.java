package com.mazama.rock.fs.hive;

import org.junit.jupiter.api.Test;

public class HiveFSTest {

    /**
     * Test method for {@link com.mazama.rock.fs.hive.HiveFS#getFileSystem()}.
     * This test method tests the getFileSystem method of the HiveFS class.
     * The getFileSystem method should return the file system type.
     */
    @Test
    public void testGetFileSystem() {
        HiveFS hiveFS = new HiveFS();
        assert hiveFS.getFileSystem().equals("hive");
    }
}
