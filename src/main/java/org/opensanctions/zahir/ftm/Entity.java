package org.opensanctions.zahir.ftm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.opensanctions.zahir.ftm.model.Property;
import org.opensanctions.zahir.ftm.model.Schema;

public class Entity {
    private String id;
    private Schema schema;
    private final Map<Property, List<Statement>> properties;
    private final List<Statement> idStatements;

    public Entity(String id, Schema schema, Map<Property, List<Statement>> properties, List<Statement> idStatements) {
        this.id = id;
        this.schema = schema;
        this.properties = properties;
        this.idStatements = idStatements;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public void addStatement(Statement statement) {
        if (!statement.getCanonicalId().equals(id)) {
            throw new IllegalArgumentException("Statement does not belong to this entity.");
        }
        String propName = statement.getPropertyName();
        if (propName == null || !schema.hasProperty(propName)) {
            throw new IllegalArgumentException("Statement property does not exist in schema.");
        }
        Property prop = schema.getProperty(propName);
        properties.computeIfAbsent(prop, k -> new ArrayList<>()).add(statement);
    }

    public boolean has(Property property) {
        return properties.containsKey(property);
    }

    public List<Statement> popStatements(Property property) {
        return properties.remove(property);
    }

    public List<Statement> getStatements(Property property) {
        return properties.get(property);
    }

    public Set<String> getValues(Property property) {
        Set<String> values = new HashSet<>();
        if (!properties.containsKey(property)) {
            return values;
        }
        for (Statement statement : properties.get(property)) {
            values.add(statement.getValue());
        }
        return values;
    }

    public Map<Property, List<Statement>> getProperties() {
        return properties;
    }

    public Iterable<Statement> getAllStatements() {
        List<Statement> allStatements = new ArrayList<>(idStatements);
        for (List<Statement> statements : properties.values()) {
            allStatements.addAll(statements);
        }
        return allStatements;
    }

    public boolean hasStatements() {
        // FIXME: Check idProperties?
        return !properties.isEmpty();
    }

    public static Entity fromStatements(String canonicalId, List<Statement> statements) {
        if (statements.isEmpty()) {
            throw new IllegalArgumentException("Cannot create entity from empty list of statements.");
        }
        Schema schema = statements.get(0).getSchema();
        Map<Property, List<Statement>> properties = new HashMap<>();
        List<Statement> idStatements = new ArrayList<>();
        for (Statement statement : statements) {
            statement = statement.withCanonicalId(canonicalId);
            schema = schema.commonWith(statement.getSchema());
            if (statement.getPropertyName().equals(Statement.ID_PROP)) {
                idStatements.add(statement);
            } else {
                Property prop = schema.getProperty(statement.getPropertyName());
                properties.computeIfAbsent(prop, k -> new ArrayList<>()).add(statement);
            }
        }
        return new Entity(canonicalId, schema, properties, idStatements);
    }

    public static Entity fromStatements(List<Statement> statements) {
        if (statements.isEmpty()) {
            throw new IllegalArgumentException("Cannot create entity from empty list of statements.");
        }
        String canonicalId = statements.get(0).getCanonicalId();
        return fromStatements(canonicalId, statements);
    }
}
