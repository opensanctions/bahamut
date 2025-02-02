package org.opensanctions.zahir.ftm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;

public class Schema {
    private final Model model;
    private final String name;
    private final List<String> extendsNames;
    private final String label;
    private final String plural;
    private final List<String> featuredNames;
    private final List<String> requiredNames;
    private final List<String> captionNames;
    private final Set<Schema> schemata;
    private final Map<String, Property> properties;

    public Schema(Model model, String name, List<String> extendsSchemata, String label, String plural, List<String> featured, List<String> required, List<String> caption) {
        this.model = model;
        this.name = name;
        this.extendsNames = extendsSchemata;
        this.label = label.length() == 0 ? this.name : label;
        this.plural = plural.length() == 0 ? this.label : plural;
        this.featuredNames = featured;
        this.requiredNames = required;
        this.captionNames = caption;
        this.schemata = new HashSet<>();
        this.properties = new HashMap<>();
    }

    /**
     * Get a list of Schema objects that this schema extends from.
     * 
     * @return A list of Schema objects representing the schemas that this schema extends.
     *         The list is constructed based on the schema names stored in extendsNames.
     *         Returns an empty list if this schema doesn't extend any other schemas.
     */
    public List<Schema> getExtends() {
        List<Schema> extendsSchemata = new ArrayList<>();
        for (String schemaName : extendsNames) {
            extendsSchemata.add(model.getSchema(schemaName));
        }
        return extendsSchemata;
    }

    public Set<Schema> getSchemata() {
        if (schemata.isEmpty()) {
            schemata.add(this);
            for (Schema schema : getExtends()) {
                schemata.addAll(schema.getSchemata());
            }
        }
        return schemata;
    }

    public Model getModel() {
        return model;
    }

    public String getName() {
        return name;
    }

    public String getLabel() {
        return label;
    }

    public String getPlural() {
        return plural;
    }

    public void addProperty(Property property) {
        properties.put(property.getName(), property);
    }

    public Property getProperty(String name) {
        return properties.get(name);
    }

    public List<Property> getProperties() {
        return new ArrayList<>(properties.values());
    }

    public List<Property> getFeaturedProperties() {
        List<Property> featuredProperties = new ArrayList<>();
        for (String propertyName : featuredNames) {
            featuredProperties.add(properties.get(propertyName));
        }
        return featuredProperties;
    }

    public List<Property> getRequiredProperties() {
        List<Property> requiredProperties = new ArrayList<>();
        for (String propertyName : requiredNames) {
            requiredProperties.add(properties.get(propertyName));
        }
        return requiredProperties;
    }

    public List<Property> getCaptionProperties() {
        List<Property> captionProperties = new ArrayList<>();
        for (String propertyName : captionNames) {
            captionProperties.add(properties.get(propertyName));
        }
        return captionProperties;
    }

    @Override
    public String toString() {
        return name;
    }

    public static Schema fromJson(Model model, String name, JsonNode node) {
        Schema schema = new Schema(model, name,
            ModelHelper.getJsonStringArray(node, "extends"),
            node.get("label").asText(),
            node.get("plural").asText(),
            ModelHelper.getJsonStringArray(node, "featured"),
            ModelHelper.getJsonStringArray(node, "required"),
            ModelHelper.getJsonStringArray(node, "caption"));

        JsonNode propertiesNode = node.get("properties");
        Iterator<String> it = propertiesNode.fieldNames();
        while (it.hasNext()) {
            String propertyName = it.next();
            JsonNode propertyNode = propertiesNode.get(propertyName);
            Property property = Property.fromJson(schema, propertyName, propertyNode);
            schema.addProperty(property);
        }
        return schema;
    }
}
