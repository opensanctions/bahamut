package org.opensanctions.zahir.ftm.model;

import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;

public class Property {
    private final Schema schema;
    private final String name;
    private final String qname;
    private final String label;
    private final String plural;
    private final Optional<String> group;
    private final PropertyType type;
    private final int maxLength;
    private final boolean matchable;
    private final boolean stub;
    private final Optional<String> reverseName;
    private final Optional<String> rangeName;

    public Property(Schema schema, String name, PropertyType type, String label, String plural, int maxLength, Optional<String> group, boolean matchable, boolean stub, Optional<String> reverse, Optional<String> range) {
        this.schema = schema;
        this.name = name.intern();
        this.qname = schema.getName() + ":" + name;
        this.label = label.length() == 0 ? this.name : label;
        this.plural = plural.length() == 0 ? this.label : plural;
        this.type = type;
        this.maxLength = maxLength;
        this.group = group;
        this.matchable = matchable;
        if (!type.isEntity() && stub) {
            throw new IllegalArgumentException("Only entity properties can be stubs: " + type.getName());
        }
        if (type.isEntity()) {
            if (reverse.isEmpty()) {
                throw new IllegalArgumentException("Entity properties must have a reverse property");
            }
            if (range.isEmpty()) {
                throw new IllegalArgumentException("Entity properties must have a range");
            }
        }
        this.stub = stub;
        this.reverseName = reverse;
        this.rangeName = range;
    }

    public PropertyType getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String getQName() {
        return qname;
    }

    public String getLabel() {
        return label;
    }

    public String getPlural() {
        return plural;
    }

    public Optional<String> getGroup() {
        return group;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public boolean isMatchable() {
        return matchable;
    }

    public boolean isStub() {
        return stub;
    }

    public Optional<Schema> getRange() {
        if (!rangeName.isPresent()) {
            return Optional.empty();
        }
        return Optional.of(schema.getModel().getSchema(rangeName.get()));
    }

    public Optional<Property> getReverse() {
        Schema range = getRange().orElse(null);
        if (!reverseName.isPresent() || range == null) {
            return Optional.empty();
        }
        return Optional.of(range.getProperty(reverseName.get()));
    }

    public static Property fromJson(Schema schema, String name, JsonNode node) {
        PropertyType type = schema.getModel().getType(node.get("type").asText());
        String label = node.get("label").asText();
        String plural = node.has("plural") ? node.get("plural").asText() : label;
        int maxLength = node.has("maxLength") ? node.get("maxLength").asInt() : type.getMaxLength();
        boolean matchable = node.has("matchable") ? node.get("matchable").asBoolean() : type.isMatchable();
        boolean stub = node.has("stub") ? node.get("stub").asBoolean() : false;
        Optional<String> group = node.has("group") ? Optional.of(node.get("group").asText()) : Optional.empty();
        Optional<String> reverse = node.has("reverse") ? Optional.of(node.get("reverse").asText()) : Optional.empty();
        Optional<String> range = node.has("range") ? Optional.of(node.get("range").asText()) : Optional.empty();
        return new Property(schema, name, type, label, plural, maxLength, group, matchable, stub, reverse, range);
    }
}
