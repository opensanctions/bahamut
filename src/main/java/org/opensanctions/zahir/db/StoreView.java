package org.opensanctions.zahir.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.opensanctions.zahir.db.proto.StatementValue;
import org.opensanctions.zahir.ftm.entity.StatementEntity;
import org.opensanctions.zahir.ftm.model.Model;
import org.opensanctions.zahir.ftm.model.Schema;
import org.opensanctions.zahir.ftm.resolver.Identifier;
import org.opensanctions.zahir.ftm.resolver.Linker;
import org.opensanctions.zahir.ftm.statement.Statement;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import com.google.protobuf.InvalidProtocolBufferException;

public class StoreView {
    private final Store store;
    private final Linker linker;
    private final Map<String, String> datasets;

    public StoreView(Store store, Linker linker, List<String> datasets) {
        this.store = store;
        this.linker = linker;
        this.datasets = new HashMap<>();
        for (String datasetName : datasets) {
            this.datasets.put(datasetName, Store.XXX_VERSION);
        }
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
                        // TODO: log to warn.
                        continue;
                    }
                    String propertyName = stmtKey[7];
                    try {
                        StatementValue stmtValue = StatementValue.parseFrom(iterator.value());
                        Statement stmt = new Statement(stmtId, localId, canonicalId, schema, propertyName, dataset, stmtValue.getValue(), stmtValue.getLang(), stmtValue.getOriginalValue(), external, stmtValue.getFirstSeen(), stmtValue.getLastSeen());
                        statements.add(stmt);
                    } catch (InvalidProtocolBufferException e) {
                        // TODO: log to warn.
                        continue;
                    }
                    iterator.next();
                }
            }   
        }
        return statements;
    }

    public Optional<StatementEntity> getEntity(String entityId) throws RocksDBException {
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
            // System.out.println("load next");
            while (prefix == null || iterator.isValid()) { 
                if (this.prefix == null) {
                    if (remainingDatasets.isEmpty()) {
                        return;
                    }
                    String dataset = remainingDatasets.removeLast();
                    String version = datasets.get(dataset);
                    this.prefix = Key.makePrefix(dataset, version, Store.ENTITY_KEY);
                    // System.out.println("Dataset: " + dataset);
                    iterator.seek(this.prefix);
                }
                byte[] key = iterator.key();
                iterator.next();
                
                // try {
                //     System.out.println("Key " + (new String(key, "UTF-8")));
                // } catch (UnsupportedEncodingException e) {
                //     // TODO Auto-generated catch block
                //     e.printStackTrace();
                // }
                if (!Key.hasPrefix(key, prefix)) {
                    prefix = null;
                    continue;
                }
                String[] keyParts = Key.splitKey(key);
                String entityId = keyParts[3];
                Identifier canonical = linker.getCanonicalIdentifier(entityId);
                String canonicalId = canonical.toString();
                // System.out.println("Next: " + canonicalId);
                if (canonical.isCanonical()) {
                    if (seen.contains(canonicalId)) {
                        continue;
                    }
                    seen.add(canonicalId);
                }
                List<Statement> statements = getStatements(entityId);
                if (statements.isEmpty()) {
                    continue;
                }
                // System.out.println("Statements: " + statements);
                nextEntity = StatementEntity.fromStatements(statements);
                break;
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
                // TODO: log and warn
                nextEntity = null;
            }
            return entity;
        }
    }

    public Iterator<StatementEntity> entities() throws RocksDBException {
        return new EntityIterator();
    }
}
