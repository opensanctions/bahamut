package org.opensanctions.zahir.db;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.opensanctions.zahir.ftm.Statement;
import org.opensanctions.zahir.ftm.entity.StatementEntity;
import org.opensanctions.zahir.ftm.resolver.Identifier;
import org.opensanctions.zahir.ftm.resolver.Linker;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

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
        for (String dataset : datasets.keySet()) {
            String version = datasets.get(dataset);
            for (Identifier identifier : connected) {
                byte[] key = Key.makeKey(dataset, version, Store.ENTITY_KEY, identifier.toString());
                keys.add(key);
            }
        }
        System.out.println("Keys: " + keys.size());

        RocksDB db = store.getDB();
        List<byte[]> values = db.multiGetAsList(keys);
        for (int i = 0; i < keys.size(); i++) {
            String[] key = Key.splitKey(keys.get(i));
            byte[] value = values.get(i);
            String xKey;
            try {
                xKey = new String(keys.get(i), "UTF-8");
                System.out.println("Key: " + xKey + " - Value: " + (value == null));
            } catch (UnsupportedEncodingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            
        }
        System.out.println("Values: " + values.size());
        List<Statement> statements = new ArrayList<>();
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
