package org.opensanctions.zahir;

import java.io.IOException;

import org.opensanctions.zahir.db.Store;
import org.opensanctions.zahir.ftm.StatementHelper;
import org.opensanctions.zahir.ftm.model.Model;
import org.rocksdb.RocksDBException;

public class App {

    public static void main(String[] args) {
        try {
            Model model = Model.loadDefault();
            Store store = new Store(model, "/Users/pudo/Code/zahir/data/exp1");
            System.out.println("DB initialized.");
            StatementHelper.loadStatementsFromCSVPath(model, store, "/Users/pudo/Data/statements.csv");
        } catch (RocksDBException | IOException re) {
            re.printStackTrace();
        }
    }
}
