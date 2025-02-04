package org.opensanctions.zahir.db;

import java.io.File;
import java.util.List;

import org.opensanctions.zahir.ftm.model.Model;
import org.opensanctions.zahir.ftm.resolver.Linker;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class Store {
    protected static final String STATEMENT_KEY = "s";
    protected static final String ENTITY_KEY = "e";
    protected static final String INVERTED_KEY = "i";

    public static final String XXX_VERSION = "xxx";

    private final String path;
    private final Model model;
    private RocksDB rocksDB;

    public Store(Model model, String path) {
        this.model = model;
        this.path = path;
        // Load the RocksDB C++ library
        RocksDB.loadLibrary();
    }

    public void initDB() throws RocksDBException {
        // Configure database options
        Options options = new Options();
        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);
        // Optimize for SSD if using one
        options.setLevelCompactionDynamicLevelBytes(true);

        // Open the database
        File dbFile = new File(path);
        if (!dbFile.isDirectory()) {
            dbFile.mkdirs();
        }
        rocksDB = RocksDB.open(options, path);
    }

    protected RocksDB getDB() throws RocksDBException {
        if (rocksDB == null) {
            initDB();
        }
        return rocksDB;
    }

    protected Model getModel() {
        return model;
    }

    public StoreWriter getWriter(String dataset, String version) {
        return new StoreWriter(this, dataset, version);
    }

    public StoreView getView(Linker linker, List<String> datasets) {
        return new StoreView(this, linker, datasets);
    }

    public void close() {
        // Close the database
        if (rocksDB != null) {
            rocksDB.close();
        }
    }
}
