package org.opensanctions.zahir.resolver;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


public class Linker {
    private final Map<Identifier, Set<Identifier>> entities;

    public Linker() {
        this.entities = new HashMap<>();
    }

    public Set<Identifier> getConnected(Identifier node) {
        Set<Identifier> result = entities.get(node);
        return result != null ? result : Set.of(node);
    }

    public Set<Identifier> getConnected(String entityId) {
        return getConnected(new Identifier(entityId));
    }

    public Identifier getCanonicalIdentifier(String entityId) {
        Identifier node = new Identifier(entityId);
        Identifier best = Collections.max(getConnected(node));
        return best.isCanonical() ? best : node;
    }

    public Identifier getCanonical(String entityId) {
        return getCanonicalIdentifier(entityId);
    }

    // public Stream<Identifier> canonicals() {
    //     return entities.keySet().stream()
    //         .filter(Identifier::isCanonical)
    //         .filter(node -> getCanonical(node.id).equals(node.id));
    // }

    public Set<String> getReferents(String canonicalId, boolean canonicals) {
        Identifier node = new Identifier(canonicalId);
        Set<String> referents = new HashSet<>();
        for (Identifier connected : getConnected(node)) {
            if (!canonicals && connected.isCanonical()) {
                continue;
            }
            if (connected.equals(node)) {
                continue;
            }
            referents.add(connected.id);
        }
        return referents;
    }

    public int size() {
        return entities.size();
    }

    public static Linker fromJsonPath(String path) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Linker linker = new Linker();
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Skip empty lines
                if (line.trim().isEmpty()) {
                    continue;
                }
                JsonNode edge = mapper.readTree(line);
                Iterator<JsonNode> parts = edge.elements();
                Identifier target = new Identifier(parts.next().asText());
                Identifier source = new Identifier(parts.next().asText());
                String judgement = parts.next().asText();
                if (judgement.equals("positive")) {
                    Set<Identifier> combined = new HashSet<>();
                    if (linker.entities.containsKey(target)) {
                        combined.addAll(linker.entities.get(target));   
                    } else {
                        combined.add(target);
                    }
                    if (linker.entities.containsKey(source)) {
                        combined.addAll(linker.entities.get(source));
                    } else {
                        combined.add(source);
                    }
                    for (Identifier node : combined) {
                        linker.entities.put(node, combined);
                    }
                }
            }
        }
        return linker;
    }
}