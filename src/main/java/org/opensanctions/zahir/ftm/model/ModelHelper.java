package org.opensanctions.zahir.ftm.model;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

public class ModelHelper {
    
    public static List<String> getJsonStringArray(JsonNode node, String key) {
        List<String> strings = new ArrayList<>();
        if (node == null || !node.has(key)) {
            return strings;
        }
        JsonNode value = node.get(key);
        if (value.isArray()) {
            for (JsonNode element : value) {
                strings.add(element.asText());
            }
        }
        return strings;
    }
    
}
