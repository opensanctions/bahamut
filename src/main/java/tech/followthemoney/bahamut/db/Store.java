package tech.followthemoney.bahamut.db;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.followthemoney.bahamut.resolver.Linker;
import tech.followthemoney.model.Model;

public class Store {
    private final static Logger log = LoggerFactory.getLogger(Store.class);

    protected static final String STATEMENT_KEY = "s";
    protected static final String ENTITY_KEY = "e";
    protected static final String INVERTED_KEY = "i";
    protected static final String DATA_KEY = "d";
    protected static final String VIEW_KEY = "v";
    protected static final String VERSIONS_KEY = "meta.versions";
    protected static final String LOCKS_KEY = "meta.locks";

    protected static final int WRITE_BATCH_SIZE = 50000;

    private final String path;
    private final Model model;
    private RocksDB rocksDB;
    protected final WriteOptions writeOptions;
    

    public Store(Model model, String path) {
        this.model = model;
        this.path = path;

        // Load the RocksDB C++ library
        RocksDB.loadLibrary();

        writeOptions = new WriteOptions();
        writeOptions.setSync(false);
        writeOptions.setDisableWAL(false);
    }

    public void initDB() throws RocksDBException {
        // Configure database options
        Options options = new Options();
        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);
        // Optimize for SSD if using one
        options.setLevelCompactionDynamicLevelBytes(true);

        options.setMemtablePrefixBloomSizeRatio(0.1);
        options.setWriteBufferSize(256 * 1024 * 1024); // 256MB
        options.setMaxWriteBufferNumber(4);
        options.setMinWriteBufferNumberToMerge(2);

        options.setMaxBackgroundJobs(4);
        // options.setAllowConcurrentMemtableWrite(true);

        // Additional performance optimizations
        // options.setUseFsync(false);

        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockSize(32 * 1024);  // 32KB
        tableConfig.setBlockCache(new LRUCache(512 * 1024 * 1024));  // 512MB block cache
        tableConfig.setCacheIndexAndFilterBlocks(true);
        tableConfig.setPinL0FilterAndIndexBlocksInCache(true);
        tableConfig.setFilterPolicy(new BloomFilter(10));  // ~1% false positive rate
        options.setTableFormatConfig(tableConfig);

        // Open the database
        File dbFile = new File(path);
        if (!dbFile.isDirectory()) {
            dbFile.mkdirs();
        }
        rocksDB = RocksDB.open(options, path);
    }

    public String getPath() {
        return path;
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

    public StoreLock getLock() {
        return new StoreLock(this);
    }

    public StoreWriter getWriter(String dataset, String version) throws RocksDBException {
        return new StoreWriter(this, dataset, version);
    }

    public StoreView getView(Linker linker, boolean withExternal) throws RocksDBException {
        return new StoreView(this, linker, getDatasets(), withExternal);
    }

    public StoreView getView(Linker linker, List<String> datasets, boolean withExternal) throws RocksDBException {
        Map<String, String> datasetMap = new HashMap<>();
        for (String dataset : datasets) {
            Optional<String> version = getLatestDatasetVersion(dataset);
            if (version.isEmpty()) {
                log.warn("No version found for dataset: {}", dataset);
                continue;
            }
            datasetMap.put(dataset, version.get());
        }
        return getView(linker, datasetMap, withExternal);
    }

    public StoreView getView(Linker linker, Map<String, String> datasets, boolean withExternal) {
        return new StoreView(this, linker, datasets, withExternal);
    }

    public void releaseDatasetVersion(String dataset, String version) throws RocksDBException {
        RocksDB db = getDB();
        byte[] key = Key.makeKey(VERSIONS_KEY, dataset, version);
        db.put(key, CoreUtil.getTimestampValue());
        // byte[] startPrefix = Key.makePrefix(dataset, version);
        // byte[] endPrefix = Key.makePrefixRangeEnd(dataset, version);
        // db.compactRange(startPrefix, endPrefix);
        log.info("Released dataset version [{}]: {}", dataset, version);
    }

    public Map<String, String> getDatasets() throws RocksDBException {
        RocksDB db = getDB();
        byte[] prefix = Key.makePrefix(VERSIONS_KEY);
        RocksIterator iterator = db.newIterator();
        iterator.seek(prefix);
        Map<String, String> datasets = new HashMap<>();
        while (iterator.isValid()) {
            byte[] key = iterator.key();
            if (!Key.hasPrefix(key, prefix)) {
                break;
            }
            String[] parts = Key.splitKey(key);
            // this should be correct (the latest version) because it's sorted, right?
            datasets.put(parts[1].intern(), parts[2]);
            iterator.next();
        }
        return datasets;
    }

    public List<String> getDatasetVersions(String dataset) throws RocksDBException {
        RocksDB db = getDB();
        byte[] prefix = Key.makePrefix(VERSIONS_KEY, dataset);
        RocksIterator iterator = db.newIterator();
        iterator.seek(prefix);
        List<String> versions = new ArrayList<>();
        while (iterator.isValid()) {
            byte[] key = iterator.key();
            if (!Key.hasPrefix(key, prefix)) {
                break;
            }
            String[] parts = Key.splitKey(key);
            versions.add(parts[2]);
            iterator.next();
        }
        return versions;
    }

    public Optional<String> getLatestDatasetVersion(String dataset) throws RocksDBException {
        List<String> versions = getDatasetVersions(dataset);
        if (versions.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(Collections.max(versions));
    }

    public boolean deleteDatasetVersion(String dataset, String version) throws RocksDBException {
        StoreLock lock = getLock();
        if (lock.isLocked(dataset, version)) {
            log.warn("Dataset version [{}]: {} is locked, cannot delete", dataset, version);
            return false;
        }
        log.info("Deleting dataset version [{}]: {}", dataset, version);
        lock.releaseAll(dataset, version);
        RocksDB db = getDB();
        byte[] startPrefix = Key.makePrefix(dataset, version);
        byte[] endPrefix = Key.makePrefixRangeEnd(dataset, version);
        db.deleteRange(startPrefix, endPrefix);
        byte[] versionKey = Key.makeKey(VERSIONS_KEY, dataset, version);
        db.delete(versionKey);
        return true;
    }

    public void close() {
        writeOptions.close();
        // Close the database
        if (rocksDB != null) {
            rocksDB.close();
        }
    }
}
