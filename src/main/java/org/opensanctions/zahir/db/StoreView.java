package org.opensanctions.zahir.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.opensanctions.zahir.db.proto.StatementValue;
import org.opensanctions.zahir.ftm.entity.StatementEntity;
import org.opensanctions.zahir.ftm.exceptions.SchemaException;
import org.opensanctions.zahir.ftm.model.Model;
import org.opensanctions.zahir.ftm.model.Schema;
import org.opensanctions.zahir.ftm.resolver.Identifier;
import org.opensanctions.zahir.ftm.resolver.Linker;
import org.opensanctions.zahir.ftm.statement.Statement;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

public class StoreView {
    private final static Logger log = LoggerFactory.getLogger(StoreView.class);

    private final Store store;
    private final Linker linker;
    private final Map<String, String> datasets;

    public StoreView(Store store, Linker linker, Map<String, String> datasets) {
        this.store = store;
        this.linker = linker;
        this.datasets = datasets;
    }

    public List<Statement> getStatements(String entityId) throws RocksDBException {
        // Check entity ID existence:
        List<byte[]> keys = new ArrayList<>();
        Set<Identifier> connected = linker.getConnected(entityId);
        String canonicalId = Collections.max(connected).toString();
        for (String dataset : datasets.keySet()) {
            String version = datasets.get(dataset);
            for (Identifier identifier : connected) {
                byte[] key = Key.makeKey(dataset, version, Store.ENTITY_KEY, identifier.toString());
                keys.add(key);
            }
        }

        RocksDB db = store.getDB();
        Model model = store.getModel();
        List<Statement> statements = new ArrayList<>();
        List<byte[]> values = db.multiGetAsList(keys);
        for (int i = 0; i < keys.size(); i++) {
            byte[] value = values.get(i);
            if (value == null) {
                continue;
            }
            String[] key = Key.splitKey(keys.get(i));
            String dataset = key[0];
            String version = key[1];
            String localId = key[3];

            byte[] stmtPrefix = Key.makePrefix(dataset, version, Store.STATEMENT_KEY, localId);
            try (var iterator = db.newIterator()) {
                iterator.seek(stmtPrefix);
                while (iterator.isValid()) {
                    byte[] itKey = iterator.key();
                    if (!Key.hasPrefix(itKey, stmtPrefix)) {
                        break;
                    }
                    String[] stmtKey = Key.splitKey(itKey);
                    boolean external = stmtKey[4].equals("x");
                    String stmtId = stmtKey[5];
                    String schemaName = stmtKey[6];
                    Schema schema = model.getSchema(schemaName);
                    if (schema == null) {
                        log.warn("Schema not found: {} (Dataset: {}, Entity: {})", schemaName, dataset, localId);
                        continue;
                    }
                    String propertyName = stmtKey[7];
                    try {
                        StatementValue stmtValue = StatementValue.parseFrom(iterator.value());
                        Statement stmt = new Statement(stmtId, localId, canonicalId, schema, propertyName, dataset, stmtValue.getValue(), stmtValue.getLang(), stmtValue.getOriginalValue(), external, stmtValue.getFirstSeen(), stmtValue.getLastSeen());
                        statements.add(stmt);
                    } catch (InvalidProtocolBufferException e) {
                        log.warn("Failed to parse statement value: {}", e.getMessage());
                        continue;
                    }
                    iterator.next();
                }
            }   
        }
        return statements;
    }

    public Optional<StatementEntity> getEntity(String entityId) throws RocksDBException, SchemaException {
        List<Statement> statements = getStatements(entityId);
        if (statements.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(StatementEntity.fromStatements(statements));
    }

    private class EntityIterator implements Iterator<StatementEntity> {
        private final RocksIterator iterator;
        private final Set<String> seen;
        private final LinkedList<String> remainingDatasets;
        private byte[] prefix;
        private StatementEntity nextEntity;

        public EntityIterator() throws RocksDBException {
            this.iterator = store.getDB().newIterator();
            this.seen = new HashSet<>();
            this.remainingDatasets = new LinkedList<>(datasets.keySet());
            loadNext();
        }

        private void loadNext() throws RocksDBException {
            nextEntity = null;
            while (true) { 
                if (this.prefix == null) {
                    if (remainingDatasets.isEmpty()) {
                        return;
                    }
                    String dataset = remainingDatasets.removeLast();
                    String version = datasets.get(dataset);
                    log.warn("Loading entities for dataset: {} (Version: {})", dataset, version);
                    this.prefix = Key.makePrefix(dataset, version, Store.ENTITY_KEY);
                    iterator.seek(this.prefix);
                }
                if (!iterator.isValid()) {
                    prefix = null;
                    return;
                }
                byte[] key = iterator.key();
                // System.out.println("XXXX key: " + new String(key));
                iterator.next();
                
                if (!Key.hasPrefix(key, prefix)) {
                    prefix = null;
                    continue;
                }
                String[] keyParts = Key.splitKey(key);
                String entityId = keyParts[3];
                Identifier canonical = linker.getCanonicalIdentifier(entityId);
                String canonicalId = canonical.toString();
                if (canonical.isCanonical()) {
                    if (seen.contains(canonicalId)) {
                        continue;
                    }
                    seen.add(canonicalId);
                }
                List<Statement> statements = getStatements(entityId);
                if (statements.isEmpty()) {
                    log.info("Entity {} has no statements", entityId);
                    continue;
                }
                try {
                    nextEntity = StatementEntity.fromStatements(statements);
                    return;    
                } catch (SchemaException e) {
                    log.error("Failed to create entity: {} (Entity: {})", e.getMessage(), entityId);
                }
            }
        }
        
        @Override
        public boolean hasNext() {
            return nextEntity != null;
        }
        
        @Override
        public StatementEntity next() {
            StatementEntity entity = this.nextEntity;
            try {
                loadNext();    
            } catch (RocksDBException e) {
                log.error("Failed to load next entity: {}", e.getMessage());
                nextEntity = null;
            }
            return entity;
        }
    }

    public Iterator<StatementEntity> entities() throws RocksDBException {
        return new EntityIterator();
    }
}
