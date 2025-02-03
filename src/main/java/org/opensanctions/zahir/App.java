package org.opensanctions.zahir;

import java.io.IOException;

import org.opensanctions.zahir.ftm.StatementHelper;
import org.opensanctions.zahir.ftm.model.Model;

public class App {

    public static void main(String[] args) {
        System.out.println("Hello World!");
        try {
            Model model = Model.loadDefault();
            StatementHelper.loadStatementsFromCSVPath(model, "/Users/pudo/Data/statements.csv");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
