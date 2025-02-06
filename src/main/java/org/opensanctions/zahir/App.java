package org.opensanctions.zahir;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.opensanctions.zahir.db.Store;
import org.opensanctions.zahir.db.StoreView;
import org.opensanctions.zahir.ftm.StatementLoader;
import org.opensanctions.zahir.ftm.entity.StatementEntity;
import org.opensanctions.zahir.ftm.model.Model;
import org.opensanctions.zahir.ftm.resolver.Linker;
import org.rocksdb.RocksDBException;

import com.fasterxml.jackson.databind.JsonNode;

public class App {

    public static void main(String[] args) {
        try {
            Model model = Model.loadDefault();
            Store store = new Store(model, "/Users/pudo/Code/zahir/data/exp1");
            Linker linker = Linker.fromJsonPath("/Users/pudo/Code/operations/etl/data/resolve.ijson");

            System.out.println("Linker loaded: " + linker.size());
            StatementLoader.loadStatementsFromCSVPath(model, store, "/Users/pudo/Data/statements.csv");
            StoreView view = store.getView(linker, List.of("test"));
            Iterator<StatementEntity> entities = view.entities();
            long count = 0;
            while (entities.hasNext()) {
                StatementEntity entity = entities.next();
                JsonNode node = entity.toValueJson();
                count++;
                if (count % 100000 == 0) {
                    System.err.println("Generated JSON: " + count);
                }
                // System.out.println(node.toPrettyString());
            }
            // Optional<StatementEntity> entity = view.getEntity("Q7747");
            // if (entity.isPresent()) {
            //     System.out.println(entity.get().getCaption());
            // }
        } catch (RocksDBException | IOException re) {
            re.printStackTrace();
        }
    }
}
