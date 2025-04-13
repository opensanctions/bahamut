package org.opensanctions.zahir;

import java.io.IOException;

import tech.followthemoney.model.Model;

public class ModelFixtures {
    private static Model model;

    public static Model getModel() {
        if (model == null) {
            try {
                model = Model.loadDefault();
            } catch (IOException e) {
                throw new RuntimeException("Failed to load model", e);
            }
        }
        return model;
    }    
}
