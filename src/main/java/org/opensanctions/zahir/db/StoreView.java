package org.opensanctions.zahir.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.opensanctions.zahir.db.proto.StatementValue;
import org.opensanctions.zahir.ftm.Statement;
import org.opensanctions.zahir.ftm.entity.StatementEntity;
import org.opensanctions.zahir.ftm.model.Model;
import org.opensanctions.zahir.ftm.model.Schema;
import org.opensanctions.zahir.ftm.resolver.Identifier;
import org.opensanctions.zahir.ftm.resolver.Linker;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

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
                while (iterator.isValid() && Key.hasPrefix(iterator.key(), stmtPrefix)) {
                    String[] stmtKey = Key.splitKey(iterator.key());
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
        System.out.println("Values: " + values.size());
        
        return statements;
    }

    public Optional<StatementEntity> getEntity(String entityId) throws RocksDBException {
        List<Statement> statements = getStatements(entityId);
        if (statements.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(StatementEntity.fromStatements(statements));
    }

}
