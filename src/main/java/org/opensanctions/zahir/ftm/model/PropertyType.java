package org.opensanctions.zahir.ftm.model;

import com.fasterxml.jackson.databind.JsonNode;

public class PropertyType {
    private final String name;
    private final String label;
    private final String plural;
    private final String description;
    private final int maxLength;
    private final boolean matchable;
    private final boolean pivot;

    public static String ENTITY = "entity".intern();
    public static String NAME = "name".intern();
    public static String IDENTIFIER = "identifier".intern();

    public PropertyType(String name, String label, String plural, String description, int maxLength, boolean matchable, boolean pivot) {
        if (name == null) {
            throw new IllegalArgumentException("Property type name cannot be null");
        }
        this.name = name.intern();
        this.label = label.length() == 0 ? this.name : label;
        this.plural = plural.length() == 0 ? this.label : plural;
        this.description = description;
        this.maxLength = maxLength;
        this.matchable = matchable;
        this.pivot = pivot;
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

    public String getDescription() {
        return description;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public boolean isMatchable() {
        return matchable;
    }

    public boolean isPivot() {
        return pivot;
    }

    public boolean isEntity() {
        return name.equals(ENTITY);
    }

    public static PropertyType fromJson(String name, JsonNode node) {
        return new PropertyType(name,
            node.get("label").asText(),
            node.get("plural").asText(),
            node.get("description").asText(),
            node.get("maxLength").asInt(),
            node.has("matchable") ? node.get("matchable").asBoolean() : false,
            node.has("pivot") ? node.get("pivot").asBoolean() : false);
    }
}
