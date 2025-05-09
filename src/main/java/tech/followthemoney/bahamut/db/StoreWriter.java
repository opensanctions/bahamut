package tech.followthemoney.bahamut.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.followthemoney.bahamut.db.proto.StatementValue;
import tech.followthemoney.entity.StatementEntity;
import tech.followthemoney.exc.SchemaException;
import tech.followthemoney.model.Property;
import tech.followthemoney.model.Schema;
import tech.followthemoney.statement.Statement;

public class StoreWriter implements AutoCloseable {
    private final static Logger log = LoggerFactory.getLogger(StoreWriter.class);

    private final Store store;
    private final String dataset;
    private final String version;
    private final String lockId;
    private WriteBatch batch;
    private int writtenStatements = 0;
    private int writtenKeys = 0;
    private final Map<String, Schema> entitySchemata;
    
    public StoreWriter(Store store, String dataset, String version) throws RocksDBException{
        this.store = store;
        this.dataset = dataset;
        this.version = version;
        this.batch = new WriteBatch();

        this.entitySchemata = new HashMap<>();
        this.lockId = CoreUtil.makeRandomId();
        store.getLock().acquire(dataset, version, this.lockId);
    }

    public void writeStatement(Statement statement) throws RocksDBException {
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

        if (!statement.getDatasetName().equals(this.dataset)) {
            log.error("Statement dataset mismatch: {} != {}", statement.getDatasetName(), this.dataset);
        }
        
        String external = statement.isExternal() ? "x" : "";
        byte[] statementKey = Key.makeKey(Store.DATA_KEY, dataset, version, Store.STATEMENT_KEY, entityId, external, statement.getId(), statement.getSchema().getName(), statement.getPropertyName());

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
                byte[] invKey = Key.makeKey(Store.DATA_KEY, dataset, version, Store.INVERTED_KEY, value, entityId);
                batch.put(invKey, prop.getName().getBytes());
            }
        }
        writtenStatements++;
        
        if (batch.count() >= Store.WRITE_BATCH_SIZE) {
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
            byte[] entityKey = Key.makeKey(Store.DATA_KEY, dataset, version, Store.ENTITY_KEY, entityId);
            batch.put(entityKey, schemaBytes);
        }
        entitySchemata.clear();
        writtenKeys += batch.count();
        store.getDB().write(store.writeOptions, batch);
        log.info("Write {} [{}]: {} statements, {} keys...", dataset, lockId, writtenStatements, writtenKeys);
        batch.close();
        batch = new WriteBatch();
    }

    @Override
    public void close() throws RocksDBException {
        batch.close();
        
        store.getLock().release(dataset, version, lockId);
        log.info("Closed writer for dataset [{}]: {} ({})", dataset, version, lockId);
    }
}
