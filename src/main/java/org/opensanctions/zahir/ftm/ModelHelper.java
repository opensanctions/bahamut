package org.opensanctions.zahir.ftm;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

public class ModelHelper {
    
    public static List<String> getJsonStringArray(JsonNode node, String key) {
        List<String> strings = new ArrayList<>();
        if (node == null || !node.has(key)) {
            return strings;
        }
        if (node.isArray()) {
            for (JsonNode element : node.get(key)) {
                strings.add(element.asText());
            }
        }
        return strings;
    }
    
}
