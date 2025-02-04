package org.opensanctions.zahir;

import java.io.IOException;

import org.opensanctions.zahir.db.RocksDBBackend;
import org.opensanctions.zahir.ftm.model.Model;
import org.rocksdb.RocksDBException;

public class App {

    public static void main(String[] args) {
        try {
            Model model = Model.loadDefault();
            RocksDBBackend db = new RocksDBBackend("/Users/pudo/Code/zahir/data/exp1");
            db.initDB();
            System.out.println("DB initialized.");
            // StatementHelper.loadStatementsFromCSVPath(model, "/Users/pudo/Data/statements.csv");
        } catch (RocksDBException re) {
            re.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
