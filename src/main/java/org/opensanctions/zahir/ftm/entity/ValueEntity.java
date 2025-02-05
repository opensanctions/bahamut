package org.opensanctions.zahir.ftm.entity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.opensanctions.zahir.ftm.model.Model;
import org.opensanctions.zahir.ftm.model.ModelHelper;
import org.opensanctions.zahir.ftm.model.Property;
import org.opensanctions.zahir.ftm.model.Schema;

import com.fasterxml.jackson.databind.JsonNode;

public class ValueEntity {
    private String id;
    private Schema schema;
    private String caption;
    private Set<String> datasets;
    private Set<String> referents;
    private String firstSeen;
    private String lastSeen;
    private final Map<Property, List<String>> properties;

    public ValueEntity(String id, Schema schema, Map<Property, List<String>> properties) {
        this.id = id;
        this.schema = schema;
        this.properties = properties;
    }

    public ValueEntity(String id, Schema schema) {
        this(id, schema, new HashMap<>());
    }

    public String getId() {
        return id;
    }

    public Schema getSchema() {
        return schema;
    }

    public String pickCaption() {
        for (Property prop : schema.getCaptionProperties()) {
            if (properties.containsKey(prop)) {
                // Put in the logic to pick the display name
                return properties.get(prop).get(0);
            }
        }
        return schema.getLabel();
    }

    public String getCaption() {
        if (caption == null) {
            caption = pickCaption();
        }
        return caption;
    }

    public void setCaption(String caption) {
        this.caption = caption;
    }

    public Set<String> getDatasets() {
        return datasets;
    }

    public void setDatasets(Set<String> datasets) {
        this.datasets = datasets;
    }

    public Set<String> getReferents() {
        return referents;
    }

    public void setReferents(Set<String> referents) {
        this.referents = referents;
    }

    public String getFirstSeen() {
        return firstSeen;
    }

    public void setFirstSeen(String firstSeen) {
        this.firstSeen = firstSeen;
    }

    public String getLastSeen() {
        return lastSeen;
    }

    public void setLastSeen(String lastSeen) {
        this.lastSeen = lastSeen;
    }

    public boolean has(Property property) {
        return properties.containsKey(property);
    }

    public List<String> getValues(Property property) {
        if (!properties.containsKey(property)) {
            return new ArrayList<>();
        }
        return properties.get(property);
    }

    public void addValue(String propertyName, String value) {
        Property property = schema.getProperty(propertyName);
        if (property == null) {
            throw new IllegalArgumentException("Invalid property: " + propertyName);
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
    //     JsonNode node = new JsonNode();
    // }

    public static ValueEntity fromJson(Model model, JsonNode node) {
        String entityId = node.get("id").asText();
        String schemaName = node.get("schema").asText();
        Schema schema = model.getSchema(schemaName);
        if (schema == null) {
            throw new IllegalArgumentException("Invalid schema: " + schemaName);
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
