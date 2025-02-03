package org.opensanctions.zahir.ftm.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

    public static Map<String, String> getJsonStringMap(JsonNode node, String key) {
        Map<String, String> strings = new HashMap<>();
        if (node == null || !node.has(key)) {
            return strings;
        }
        JsonNode value = node.get(key);
        if (value.isObject()) {
            Iterator<String> it = value.fieldNames();
            while (it.hasNext()) {
                String k = it.next();
                strings.put(k, value.get(k).asText());
            }
        }
        return strings;
    }
    
}
