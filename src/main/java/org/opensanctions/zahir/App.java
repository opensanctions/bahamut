package org.opensanctions.zahir;

import java.io.IOException;

import org.opensanctions.zahir.server.ZahirServer;

public class App {

    public static void main(String[] args) {
        try {
            ZahirServer server = new ZahirServer(6674);
            server.start();
            try {
                server.blockUntilShutdown();    
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            
            // Model model = Model.loadDefault();
            // Store store = new Store(model, "/Users/pudo/Code/zahir/data/exp1");
            // Linker linker = Linker.fromJsonPath("/Users/pudo/Code/operations/etl/data/resolve.ijson");

            // System.out.println("Linker loaded: " + linker.size());
            // // StatementLoader.loadStatementsFromCSVPath(model, store, "/Users/pudo/Data/statements.csv");
            // StoreView view = store.getView(linker);
            // Iterator<StatementEntity> entities = view.entities();
            // long count = 0;
            // while (entities.hasNext()) {
            //     StatementEntity entity = entities.next();
            //     JsonNode node = entity.toValueJson();
            //     count++;
            //     if (count % 10000 == 0) {
            //         System.err.println("Generated JSON: " + count);
            //     }
            //     node.toPrettyString();
            //     // System.out.println(node.toPrettyString());
            // }
            // // Optional<StatementEntity> entity = view.getEntity("Q7747");
            // // if (entity.isPresent()) {
            // //     System.out.println(entity.get().getCaption());
            // // }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
