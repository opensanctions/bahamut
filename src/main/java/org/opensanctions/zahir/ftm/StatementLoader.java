package org.opensanctions.zahir.ftm;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.opensanctions.zahir.db.Store;
import org.opensanctions.zahir.db.StoreWriter;
import org.opensanctions.zahir.ftm.model.Model;
import org.opensanctions.zahir.ftm.model.Schema;
import org.opensanctions.zahir.ftm.statement.Statement;
import org.rocksdb.RocksDBException;

public class StatementLoader {
    private final static Map<String, Instant> dateCache = new HashMap<>();

    protected static Instant parseDateTime(String dateTime) {
        if (dateCache.containsKey(dateTime)) {
            return dateCache.get(dateTime);
        }
        Instant instant = Instant.parse(dateTime + ".00Z");
        dateCache.put(dateTime, instant);
        return instant;
    }
    
    public static void loadStatementsFromCSVPath(Model model, Store store, String path) throws FileNotFoundException, RocksDBException {
        Reader reader = new FileReader(path);
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader().get();
        // List<Statement> statements = new ArrayList<>();
        long count = 0;
        Map<String, StoreWriter> writers = new HashMap<>();
        // StoreWriter writer = store.getWriter("test", Store.XXX_VERSION);
        System.out.println("Loading statements from " + path);
        try (CSVParser csvParser = new CSVParser(reader, format)) {
            for (CSVRecord record : csvParser) {
                count++;
                Schema schema = model.getSchema(record.get("schema"));
                if (schema == null) {
                    System.err.println("Schema not found: " + record.get("schema"));
                    continue;
                }
                String dataset = record.get("dataset");
                if (!writers.containsKey(dataset)) {
                    writers.put(dataset, store.getWriter(dataset, Store.XXX_VERSION));
                }
                StoreWriter writer = writers.get(dataset);
                String property = record.get("prop");
                Instant firstSeen = parseDateTime(record.get("first_seen"));
                Instant lastSeen = parseDateTime(record.get("last_seen"));
                boolean external = record.get("external").startsWith("t");

                Statement stmt = new Statement(record.get("id"), record.get("entity_id"), record.get("canonical_id"), schema, property, record.get("dataset"), record.get("value"), record.get("lang"), record.get("original_value"), external, firstSeen.getEpochSecond(), lastSeen.getEpochSecond());
                // System.out.println(stmt.getEntityId());
                writer.writeStatement(stmt);
                if (count > 0 && count % 100000 == 0) {
                    System.err.println(count);
                }
            }
            System.err.println("Total: " + count);
            for (StoreWriter writer : writers.values()) {
                writer.close();
            }
            // RocksDB db = store.getDB();
            // db.comp();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}
