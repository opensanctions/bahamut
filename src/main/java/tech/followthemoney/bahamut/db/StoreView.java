package tech.followthemoney.bahamut.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.opensanctions.zahir.db.proto.StatementValue;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import tech.followthemoney.bahamut.resolver.Identifier;
import tech.followthemoney.bahamut.resolver.Linker;
import tech.followthemoney.entity.StatementEntity;
import tech.followthemoney.exc.SchemaException;
import tech.followthemoney.exc.ViewException;
import tech.followthemoney.model.Model;
import tech.followthemoney.model.Property;
import tech.followthemoney.model.Schema;
import tech.followthemoney.statement.Statement;
import tech.followthemoney.store.Adjacency;
import tech.followthemoney.store.View;

public class StoreView extends View<StatementEntity> {
    private final static Logger log = LoggerFactory.getLogger(StoreView.class);

    private final Store store;
    private final Linker linker;
    private final Model model;
    private final Map<String, String> datasets;
    private final ReadOptions readOptions;
    private final boolean withExternal;
    private String lockId;

    public StoreView(Store store, Linker linker, Map<String, String> datasets, boolean withExternal) {
        this.store = store;
        this.model = store.getModel();
        this.linker = linker;
        this.datasets = datasets;
        this.withExternal = withExternal;

        readOptions = new ReadOptions();
        // readOptions.setPrefixSameAsStart(true);
        readOptions.setVerifyChecksums(false);
        // readOptions.setFillCache(true);
    }

    public String lockScope() throws RocksDBException {
        lockId = CoreUtil.makeRandomId();
        for (String dataset : datasets.keySet()) {
            String version = datasets.get(dataset);
            store.getLock().acquire(dataset, version, lockId);
        }
        return lockId;
    }

    public void resumeLock(String lockId) {
        this.lockId = lockId;
    }

    @Override
    public void close() throws ViewException {
        if (lockId == null) {
            return;
        }
        for (String dataset : datasets.keySet()) {
            String version = datasets.get(dataset);
            try {
                store.getLock().release(dataset, version, lockId);    
            } catch (RocksDBException e) {
                log.error("Failed to release view lock", e);
            }
        }
        byte[] rangeStart = Key.makePrefix(Store.VIEW_KEY, lockId);
        byte[] rangeEnd = Key.makePrefixRangeEnd(Store.VIEW_KEY, lockId);
        try {
            store.getDB().deleteRange(rangeStart, rangeEnd);
        } catch (RocksDBException e) {
            log.error("Failed to delete view keys", e);
        }
    }

    private List<Statement> getLocalStatements(RocksDB db, String canonicalId, String dataset, String version, String localId) throws RocksDBException {
        List<Statement> statements = new ArrayList<>();
        byte[] stmtPrefix = Key.makePrefix(Store.DATA_KEY, dataset, version, Store.STATEMENT_KEY, localId);
        try (RocksIterator iterator = db.newIterator(readOptions)) {
            iterator.seek(stmtPrefix);
            while (iterator.isValid()) {
                byte[] itKey = iterator.key();
                if (!Key.hasPrefix(itKey, stmtPrefix)) {
                    break;
                }
                String[] stmtKey = Key.splitKey(itKey);
                boolean external = stmtKey[5].equals("x");
                if (external && !withExternal) {
                    iterator.next();
                    continue;
                }
                String stmtId = stmtKey[6];
                String schemaName = stmtKey[7];
                Schema schema = model.getSchema(schemaName);
                if (schema == null) {
                    log.warn("Schema not found: {} (Dataset: {}, Entity: {})", schemaName, dataset, localId);
                    iterator.next();
                    continue;
                }
                String propertyName = stmtKey[8];
                try {
                    StatementValue stmtValue = StatementValue.parseFrom(iterator.value());
                    Statement stmt = new Statement(stmtId, localId, canonicalId, schema, propertyName, dataset, stmtValue.getValue(), stmtValue.getLang(), stmtValue.getOriginalValue(), external, stmtValue.getFirstSeen(), stmtValue.getLastSeen());
                    statements.add(stmt);
                } catch (InvalidProtocolBufferException e) {
                    log.warn("Failed to parse statement value: {}", e.getMessage());
                    iterator.next();
                    continue;
                }
                iterator.next();
            }
        }
        return statements;
    }

    public List<Statement> getStatements(String entityId) throws RocksDBException {
        // Check entity ID existence:
        List<byte[]> keys = new ArrayList<>();
        Set<Identifier> connected = linker.getConnected(entityId);
        String canonicalId = Collections.max(connected).toString();
        for (String dataset : datasets.keySet()) {
            String version = datasets.get(dataset);
            for (Identifier identifier : connected) {
                byte[] key = Key.makeKey(Store.DATA_KEY, dataset, version, Store.ENTITY_KEY, identifier.toString());
                keys.add(key);
            }
        }
        RocksDB db = store.getDB();
        List<Statement> statements = new ArrayList<>();
        List<byte[]> values = db.multiGetAsList(keys);
        for (int i = 0; i < keys.size(); i++) {
            byte[] value = values.get(i);
            if (value == null) {
                continue;
            }
            String[] key = Key.splitKey(keys.get(i));
            String dataset = key[1];
            String version = key[2];
            String localId = key[4];
            statements.addAll(getLocalStatements(db, canonicalId, dataset, version, localId));
        }
        return statements;
    }

    @Override
    public Optional<StatementEntity> getEntity(String entityId) throws ViewException {
        try {
            List<Statement> statements = getStatements(entityId);
            if (statements.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(StatementEntity.fromStatements(statements));    
        } catch (RocksDBException e) {
            throw new ViewException("Database error", e);
        } catch (SchemaException e) {
            throw new ViewException("Schema error", e);
        }
        
    }

    @Override
    public boolean hasEntity(String entityId) throws ViewException {
        // nb: This doesn't actually need to retrieve the statements, we just need 
        // to check if there are any entity keys. Since we're not using this in the
        // current implementation, we'll leave it as is.
        try {
            List<Statement> statements = getStatements(entityId);
            return !statements.isEmpty();
        } catch (RocksDBException e) {
            throw new ViewException("Database error", e);
        }
    }

    @Override
    public Stream<Adjacency<StatementEntity>> getInverted(String entityId) throws ViewException {
        try {
            // Collect all the entities with a property pointing to the given entityId, and 
            // generate canonical Ids for each.
            RocksDB db = store.getDB();
            RocksIterator iterator = db.newIterator(readOptions);
            Set<Identifier> connected = linker.getConnected(entityId);
            Set<Map.Entry<String, String>> inverse = new HashSet<>();
            for (String dataset : datasets.keySet()) {
                String version = datasets.get(dataset);
                for (Identifier identifier : connected) {
                    byte[] prefix = Key.makePrefix(Store.DATA_KEY, dataset, version, Store.INVERTED_KEY, identifier.toString());
                    iterator.seek(prefix);
                    while (iterator.isValid()) {
                        byte[] key = iterator.key();
                        if (!Key.hasPrefix(key, prefix)) {
                            break;
                        }
                        String[] keyParts = Key.splitKey(key);
                        String localId = keyParts[5];
                        String property = new String(iterator.value());
                        // nb. the property is in the value, can be used to filter
                        String canonicalId = linker.getCanonicalIdentifier(localId).toString();
                        inverse.add(Map.entry(canonicalId, property));
                        iterator.next();
                    }
                }
            }

            // For each entity, retrieve the property and build an adjacency object.
            return inverse.stream()
                .map(entry -> {
                    try {
                        Optional<StatementEntity> fEntity = getEntity(entry.getKey());
                        if (fEntity.isEmpty()) {
                            return null;
                        }
                        StatementEntity entity = fEntity.get();
                        Property property = entity.getSchema().getProperty(entry.getValue());
                        if (property == null) {
                            return null;
                        }
                        return new Adjacency<StatementEntity>(property, entity);
                    } catch (ViewException e) {
                        log.error("Failed to retrieve entity: {}", entry.getKey(), e);
                        return null;
                    }
                })
                .filter(adj -> adj != null);
        } catch (RocksDBException e) {
            throw new ViewException("Database error", e);
        }
    }

    private StatementEntity collectCanonical(RocksDB db, RocksIterator it, byte[] prefix) throws NoSuchElementException, RocksDBException {
        while (true) {
            List<Statement> statements = new ArrayList<>(100);
            String canonicalId = null;
            while (true) {
                if (!it.isValid()) {
                    throw new NoSuchElementException();
                }
                byte[] key = it.key();
                if (!Key.hasPrefix(key, prefix)) {
                    throw new NoSuchElementException();
                }
                String[] keyParts = Key.splitKey(key);
                String nextCanonicalId = keyParts[2];
                String dataset = keyParts[3];
                String version = keyParts[4];
                String localId = keyParts[5];
                if (canonicalId == null) {
                    canonicalId = nextCanonicalId;
                } else if (!nextCanonicalId.equals(canonicalId)) {
                    break;
                }
                statements.addAll(getLocalStatements(db, canonicalId, dataset, version, localId));
                it.next();
            }
            if (statements.isEmpty()) {
                continue;
            }
            try {
                return StatementEntity.fromStatements(statements);    
            } catch (SchemaException e) {
                log.warn("Schema error building: " + canonicalId, e);
            }
        }
    }

    @Override
    public Stream<StatementEntity> allEntities() throws ViewException {
        try {
            RocksDB db = store.getDB();
            RocksIterator iterator = db.newIterator(readOptions);
            WriteBatch batch = new WriteBatch();
            long entityParts = 0;
            byte[] stub = new byte[0];
            for (var entry : datasets.entrySet()) {
                String dataset = entry.getKey();
                String version = entry.getValue();
                byte[] prefix = Key.makePrefix(Store.DATA_KEY, dataset, version, Store.ENTITY_KEY);
                iterator.seek(prefix);
                while (iterator.isValid()) {
                    byte[] key = iterator.key();
                    if (!Key.hasPrefix(key, prefix)) {
                        break;
                    }
                    String[] keyParts = Key.splitKey(key);
                    String entityId = keyParts[4];
                    String canonicalId = linker.getCanonicalIdentifier(entityId).toString();
                    byte[] viewKey = Key.makeKey(Store.VIEW_KEY, lockId, canonicalId, dataset, version, entityId);
                    batch.put(viewKey, stub);
                    entityParts += 1;
                    if (batch.count() >= Store.WRITE_BATCH_SIZE) {
                        db.write(store.writeOptions, batch);
                        batch.close();
                        batch = new WriteBatch();
                    }
                    iterator.next();
                }
            }
            if (batch.count() >= 0) {
                db.write(store.writeOptions, batch);
                batch.close();
            }
            log.info("Generated {} entity parts, {} datasets", entityParts, datasets.size());

            RocksIterator resolvedIterator = db.newIterator(readOptions);
            byte[] prefix = Key.makePrefix(Store.VIEW_KEY, lockId);
            resolvedIterator.seek(prefix);
            return Stream.generate(() -> {
                try {
                    return collectCanonical(db, resolvedIterator, prefix);
                } catch (RocksDBException e) {
                    log.error("Database error", e);
                    return null;
                } catch (NoSuchElementException e) {
                    return null;
                }
            }).takeWhile(entity -> entity != null);
        } catch (RocksDBException e) {
            throw new ViewException("Database error", e);
        }
    }
}
