package org.opensanctions.zahir.db;

import java.util.Optional;

import org.opensanctions.zahir.ftm.Entity;
import org.opensanctions.zahir.ftm.Statement;
import org.opensanctions.zahir.ftm.model.Property;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public class StoreWriter implements AutoCloseable {
    private static final int BATCH_SIZE = 10000;

    private final Store store;
    private final String dataset;
    private final String version;
    private WriteBatch batch;
    private final WriteOptions writeOptions;
    
    public StoreWriter(Store store, String dataset, String version) {
        this.store = store;
        this.dataset = dataset;
        this.version = version;
        this.writeOptions = new WriteOptions();
    }

    public void writeStatement(Statement statement) throws RocksDBException {
        if (batch == null) {
            batch = new WriteBatch();
        }
        
        // TODO: do this only once per entity
        String entityId = statement.getEntityId();
        byte[] schemaBytes = statement.getSchema().getName().getBytes();
        byte[] entityKey = Key.makeKey(dataset, version, Store.ENTITY_KEY, entityId);
        batch.put(entityKey, schemaBytes);

        String external = statement.isExternal() ? "x" : "";
        byte[] statementKey = Key.makeKey(dataset, version, Store.STATEMENT_KEY, entityId, external, statement.getId(), statement.getSchema().getName(), statement.getPropertyName());
        // TODO: serialize!!!
        batch.put(statementKey, statement.getValue().getBytes());

        Optional<Property> property = statement.getProperty();
        if (property.isPresent()) {
            Property prop = property.get();
            if (prop.getType().isEntity()) {
                String value = statement.getValue();
                byte[] invKey = Key.makeKey(dataset, version, Store.INVERTED_KEY, value, entityId);
                batch.put(invKey, prop.getName().getBytes());
            }
        }
        
        // store.write(statement);
        if (batch.count() >= BATCH_SIZE) {
            flush();
        }
    }

    public void writeEntity(Entity entity) throws RocksDBException{
        for (Statement stmt : entity.getAllStatements()) {
            writeStatement(stmt);
        }
    }

    public void flush() throws RocksDBException {
        if (batch.count() == 0) {
            return;
        }
        store.getDB().write(writeOptions, batch);
        batch.close();
    }

    @Override
    public void close() throws RocksDBException {
        // flush();
        writeOptions.close();
    }

}
