package org.opensanctions.zahir;

import java.io.IOException;

import org.opensanctions.zahir.ftm.model.Model;

public class App {

    public static void main(String[] args) {
        System.out.println("Hello World!");
        try {
            Model model = Model.loadDefault();
            for (String key : model.getTypes().keySet()) {
                System.out.println(key);
            }
            for (String schemaName : model.getSchemata().keySet()) {
                System.out.println(schemaName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
