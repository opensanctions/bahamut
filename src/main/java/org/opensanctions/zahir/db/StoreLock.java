package org.opensanctions.zahir.db;

import org.opensanctions.zahir.Config;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

public class StoreLock {
    private final Store store;

    public StoreLock(Store store) {
        this.store = store;
    }

    public void acquire(String dataset, String version, String lockId) throws RocksDBException {
        RocksDB db = store.getDB();
        byte[] key = Key.makeKey(Store.LOCKS_KEY, dataset, version, lockId);
        db.put(key, CoreUtil.getTimestampValue());
    }

    public void release(String dataset, String version, String lockId) throws RocksDBException {
        RocksDB db = store.getDB();
        byte[] key = Key.makeKey(Store.LOCKS_KEY, dataset, version, lockId);
        db.delete(key);
    }

    public void releaseAll(String dataset, String version) throws RocksDBException {
        RocksDB db = store.getDB();
        byte[] startPrefix = Key.makePrefix(Store.LOCKS_KEY, dataset, version);
        byte[] endPrefix = Key.makePrefixRangeEnd(Store.LOCKS_KEY, dataset, version);
        db.deleteRange(startPrefix, endPrefix);
    }

    private long getMaxValue(String dataset, String version) throws RocksDBException {
        RocksDB db = store.getDB();
        byte[] prefix = Key.makePrefix(Store.LOCKS_KEY, dataset, version);
        RocksIterator iterator = db.newIterator();
        iterator.seek(prefix);
        long maxValue = 0;
        while (iterator.isValid()) {
            byte[] key = iterator.key();
            if (!Key.hasPrefix(key, prefix)) {
                break;
            }
            long value = Long.parseLong(new String(iterator.value()));
            if (value > maxValue) {
                maxValue = value;
            }
            iterator.next();
        }
        return maxValue;
    }

    public boolean isLocked(String dataset, String version) throws RocksDBException {
        long newestLock = getMaxValue(dataset, version);
        if (newestLock == 0) {
            return false;
        }
        long currentTime = CoreUtil.getTimestamp();
        return (currentTime - newestLock) < Config.LOCK_TIMEOUT;
    }
}
