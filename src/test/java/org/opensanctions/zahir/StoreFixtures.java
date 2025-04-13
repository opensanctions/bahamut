package org.opensanctions.zahir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.opensanctions.zahir.db.Store;

import tech.followthemoney.model.Model;

public class StoreFixtures {

    public static Path getTempPath() {
        // generate a temporary path for the test
        try {
            return Files.createTempDirectory("junit-test-");    
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temp directory", e);
        }
    }

    public static Store createStore() {
        Model model = ModelFixtures.getModel();
        Path path = getTempPath();
        Store store = new Store(model, path.toString());
        return store;
    }

    public static void deleteStore(Store store) {
        if (store != null) {
            String path = store.getPath();
            try {
                store.close();
            } catch (Exception e) {
                System.err.println("Failed to close store: " + e.getMessage());
            }
            deleteTempPath(Path.of(path));
        }
    }

    public static void deleteTempPath(Path path) {
        try {
            Files.walk(path)
                .sorted((p1, p2) -> -p1.compareTo(p2)) // Reverse order - deepest files first
                .forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException e) {
                        System.err.println("Failed to delete " + p + ": " + e.getMessage());
                    }
                });
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete temp directory", e);
        }
    }


}
