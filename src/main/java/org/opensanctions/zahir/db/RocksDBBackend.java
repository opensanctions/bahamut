package org.opensanctions.zahir.db;

import java.io.File;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;


public class RocksDBBackend {
    private final String path;
    private RocksDB rocksDB;

    public RocksDBBackend(String path) {
        this.path = path;
        // Load the RocksDB C++ library
        RocksDB.loadLibrary();
    }

    public void initDB() throws RocksDBException {
        // Configure database options
        Options options = new Options();
        options.setCreateIfMissing(true);
        // Optimize for SSD if using one
        options.setLevelCompactionDynamicLevelBytes(true);

        // Open the database
        File dbFile = new File(path);
        if (!dbFile.isDirectory()) {
            dbFile.mkdirs();
        }
        rocksDB = RocksDB.open(options, path);
    }

    public void cleanup() {
        // Close the database
        if (rocksDB != null) {
            rocksDB.close();
        }
    }
}
