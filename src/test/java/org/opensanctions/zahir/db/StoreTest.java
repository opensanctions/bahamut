package org.opensanctions.zahir.db;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensanctions.zahir.DataFixtures;
import org.opensanctions.zahir.StoreFixtures;
import org.opensanctions.zahir.resolver.Linker;
import org.rocksdb.RocksDBException;

import tech.followthemoney.entity.StatementEntity;
import tech.followthemoney.exc.ViewException;
import tech.followthemoney.statement.Statement;

public class StoreTest {
    Store store;

    @BeforeEach
    public void setUp() {
        store = StoreFixtures.createStore();
    }

    @AfterEach
    public void tearDown() {
        StoreFixtures.deleteStore(store);
    }

    @Test
    @SuppressWarnings("ConvertToTryWithResources")
    public void testStoreBasics() throws RocksDBException, ViewException{
        // Test basic store functionality
        assertNotNull(store);
        assertNotNull(store.getPath());
        assertEquals(store.getDatasets().size(), 0);
        String dataset = "test_dataset1";
        String version = "20250101123456-abc";
        assertEquals(store.getDatasetVersions(dataset).size(), 0);
        StoreWriter writer = store.getWriter(dataset, version);
        for (Statement stmt : DataFixtures.getFixtureStatements("test_dataset1")) {
            writer.writeStatement(stmt);
        }
        writer.flush();
        writer.close();
        Linker linker = new Linker();
        Map<String, String> datasets = new HashMap<>();
        datasets.put(dataset, version);
        StoreView preRelease = store.getView(linker, datasets, true);
        assertNotNull(preRelease);

        assertEquals(store.getDatasets().size(), 0);
        store.releaseDatasetVersion(dataset, version);
        assertEquals(store.getDatasets().size(), 1);
        assertTrue(store.getDatasetVersions(dataset).contains(version));

        StoreView view = store.getView(linker, Arrays.asList(dataset), true);

        Optional<StatementEntity> absent = view.getEntity("Q40");
        assertFalse(absent.isPresent());

        Optional<StatementEntity> entity = view.getEntity("Q844");
        assertTrue(entity.isPresent());
        StatementEntity stmtEntity = entity.get();
        assertEquals(stmtEntity.getId(), "Q844");
        assertEquals(stmtEntity.getDatasets().size(), 1);
        assertEquals(stmtEntity.getCaption(), "James Bond");
    }

}
