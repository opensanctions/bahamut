package org.opensanctions.zahir.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.opensanctions.zahir.db.proto.StatementValue;
import org.opensanctions.zahir.ftm.entity.StatementEntity;
import org.opensanctions.zahir.ftm.exceptions.SchemaException;
import org.opensanctions.zahir.ftm.model.Property;
import org.opensanctions.zahir.ftm.model.Schema;
import org.opensanctions.zahir.ftm.statement.Statement;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoreWriter implements AutoCloseable {
    private static final int BATCH_SIZE = 100000;
    private final static Logger log = LoggerFactory.getLogger(StoreWriter.class);

    private final Store store;
    private final String dataset;
    private final String version;
    private final String lockId;
    private WriteBatch batch;
    private final WriteOptions writeOptions;
    private final Map<String, Schema> entitySchemata;
    
    public StoreWriter(Store store, String dataset, String version) throws RocksDBException{
        this.store = store;
        this.dataset = dataset;
        this.version = version;
        this.batch = new WriteBatch();
        this.writeOptions = new WriteOptions();
        this.entitySchemata = new HashMap<>();
        this.lockId = CoreUtil.makeRandomId();
        store.getLock().acquire(dataset, version, this.lockId);
    }

    public void writeStatement(Statement statement) throws RocksDBException {
        // TODO: check statements are in the right dataset

        // Do this only once per entity and batch
        String entityId = statement.getEntityId();
        Schema schema = statement.getSchema();
        Schema existing = entitySchemata.get(entityId);
        if (existing != schema) {
            try {
                this.entitySchemata.put(entityId, schema.commonWith(existing));    
            } catch (SchemaException e) {
                log.warn("Schema mismatch for entity {}: {}", entityId, e.getMessage());
            }
        }
        
        String external = statement.isExternal() ? "x" : "";
        byte[] statementKey = Key.makeKey(dataset, version, Store.STATEMENT_KEY, entityId, external, statement.getId(), statement.getSchema().getName(), statement.getPropertyName());

        StatementValue stmtValue = StatementValue.newBuilder()
            .setValue(statement.getValue())
            .setLang(statement.getLang())
            .setOriginalValue(statement.getOriginalValue())
            .setFirstSeen(statement.getFirstSeen())
            .setLastSeen(statement.getLastSeen())
            .build();
        batch.put(statementKey, stmtValue.toByteArray());

        Optional<Property> property = statement.getProperty();
        if (property.isPresent()) {
            Property prop = property.get();
            if (prop.getType().isEntity()) {
                String value = statement.getValue();
                byte[] invKey = Key.makeKey(dataset, version, Store.INVERTED_KEY, value, entityId);
                batch.put(invKey, prop.getName().getBytes());
            }
        }
        
        if (batch.count() >= BATCH_SIZE) {
            flush();
        }
    }

    public void writeEntity(StatementEntity entity) throws RocksDBException{
        for (Statement stmt : entity.getAllStatements()) {
            writeStatement(stmt);
        }
    }

    public void flush() throws RocksDBException {
        if (batch.count() == 0 && entitySchemata.isEmpty()) {
            return;
        }
        for (String entityId : entitySchemata.keySet()) {
            byte[] schemaBytes = entitySchemata.get(entityId).getName().getBytes();
            byte[] entityKey = Key.makeKey(dataset, version, Store.ENTITY_KEY, entityId);
            batch.put(entityKey, schemaBytes);
        }
        entitySchemata.clear();
        store.getDB().write(writeOptions, batch);
        batch.close();
        batch = new WriteBatch();
    }

    @Override
    public void close() throws RocksDBException {
        // flush();
        batch.close();
        writeOptions.close();
        store.getLock().release(dataset, version, lockId);
    }
}
