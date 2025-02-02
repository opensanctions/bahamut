package org.opensanctions.zahir;

import java.io.IOException;
import java.io.InputStream;

import org.opensanctions.zahir.ftm.Model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class App {

    private static Model loadModel() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        InputStream stream = App.class.getResourceAsStream("/model.json");
        JsonNode root = mapper.readTree(stream);
        Model model = Model.fromJson(mapper, root.get("model"));
        return model;
    }

    public static void main(String[] args) {
        System.out.println("Hello World!");
        try {
            Model model = loadModel();
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
