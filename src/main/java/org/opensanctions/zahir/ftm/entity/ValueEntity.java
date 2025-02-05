package org.opensanctions.zahir.ftm.entity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.opensanctions.zahir.ftm.exceptions.SchemaException;
import org.opensanctions.zahir.ftm.model.Model;
import org.opensanctions.zahir.ftm.model.ModelHelper;
import org.opensanctions.zahir.ftm.model.Property;
import org.opensanctions.zahir.ftm.model.Schema;

import com.fasterxml.jackson.databind.JsonNode;

public class ValueEntity extends Entity {
    
    private Set<String> datasets;
    private Set<String> referents;
    private String firstSeen;
    private String lastSeen;
    private final Map<Property, List<String>> properties;

    public ValueEntity(String id, Schema schema, Map<Property, List<String>> properties) {
        super(id, schema);
        this.properties = properties;
    }

    public ValueEntity(String id, Schema schema) {
        this(id, schema, new HashMap<>());
    }

    @Override
    protected String pickCaption() {
        for (Property prop : schema.getCaptionProperties()) {
            if (properties.containsKey(prop)) {
                // Put in the logic to pick the display name
                return properties.get(prop).get(0);
            }
        }
        return schema.getLabel();
    }

    @Override
    public Set<String> getDatasets() {
        return datasets;
    }

    public void setDatasets(Set<String> datasets) {
        this.datasets = datasets;
    }

    @Override
    public Set<String> getReferents() {
        return referents;
    }

    public void setReferents(Set<String> referents) {
        this.referents = referents;
    }

    @Override
    public String getFirstSeen() {
        return firstSeen;
    }

    public void setFirstSeen(String firstSeen) {
        this.firstSeen = firstSeen;
    }

    @Override
    public String getLastSeen() {
        return lastSeen;
    }

    public void setLastSeen(String lastSeen) {
        this.lastSeen = lastSeen;
    }

    public boolean has(Property property) {
        return properties.containsKey(property);
    }

    @Override
    public List<String> getValues(Property property) {
        if (!properties.containsKey(property)) {
            return new ArrayList<>();
        }
        return properties.get(property);
    }

    public void addValue(String propertyName, String value) throws SchemaException {
        Property property = schema.getProperty(propertyName);
        if (property == null) {
            throw new SchemaException("Invalid property: " + propertyName);
        }
        addValue(property, value);
    }

    public void addValue(Property property, String value) {
        if (property.isEnum()) {
            value = value.intern();
        }
        List<String> values = properties.getOrDefault(property, new ArrayList<>());
        if (!values.contains(value)) {
            values.add(value);
        }
        properties.put(property, values);
    }

    // public JsonNode toValueJson() {
    //     JsonNode node = super.toJson();
    //     node.put("type", "value");
    //     node.put("schema", schema.getName());
    //     if (caption != null) {
    //         node.put("caption", caption);
    //     }
    //     if (!properties.isEmpty()) {
    //         JsonNode propsNode = node.putObject("properties");
    //         for (Map.Entry<Property, List<String>> entry : properties.entrySet()) {
    //             Property property = entry.getKey();
    //             List<String> values = entry.getValue();
    //             if (property.isEnum()) {
    //                 for (int i = 0; i < values.size(); i++) {
    //                     values.set(i, values.get(i).intern());
    //                 }
    //             }
    //             ModelHelper.setJsonStringArray(propsNode, property.getName(), values);
    //         }
    //     }
    //     if (datasets != null) {
    //         ModelHelper.setJsonStringSet(node, "datasets", datasets);
    //     }
    //     if (referents != null) {
    //         ModelHelper.setJsonStringSet(node, "referents", referents);
    //     }
    //     if (firstSeen != null) {
    //         node.put("first_seen", firstSeen);
    //     }
    //     if (lastSeen != null) {
    //         node.put("last_seen", lastSeen);
    //     }
    //     return node;
    // }

    public static ValueEntity fromJson(Model model, JsonNode node) throws SchemaException {
        String entityId = node.get("id").asText();
        String schemaName = node.get("schema").asText();
        Schema schema = model.getSchema(schemaName);
        if (schema == null) {
            throw new SchemaException("Invalid schema: " + schemaName);
        }
        Map<Property, List<String>> properties = new HashMap<>();
        if (node.has("properties")) {
            JsonNode propsNode = node.get("properties");
            Iterator<String> it = propsNode.fieldNames();
            while (it.hasNext()) {
                String propertyName = it.next();
                Property property = schema.getProperty(propertyName);
                if (property == null) {
                    continue;
                }
                List<String> values = ModelHelper.getJsonStringArray(propsNode, propertyName);
                if (property.isEnum()) {
                    for (int i = 0; i < values.size(); i++) {
                        values.set(i, values.get(i).intern());
                    }
                }
                properties.put(property, values);
            }
        }
        ValueEntity entity = new ValueEntity(entityId, schema, properties);
        if (node.has("caption")) {
            entity.setCaption(node.get("caption").asText());
        }
        entity.setDatasets(ModelHelper.getJsonStringSet(node, "datasets"));
        entity.setReferents(ModelHelper.getJsonStringSet(node, "referents"));
        if (node.has("first_seen")) {
            entity.setFirstSeen(node.get("first_seen").asText());
        }
        if (node.has("last_seen")) {
            entity.setLastSeen(node.get("last_seen").asText());
        }
        return entity;
    }
}
