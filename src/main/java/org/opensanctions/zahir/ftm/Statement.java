package org.opensanctions.zahir.ftm;

import java.math.BigInteger;
import java.util.Objects;

import org.opensanctions.zahir.ftm.model.Property;
import org.opensanctions.zahir.ftm.model.Schema;

public class Statement {
    // private static final String DEFAULT_LANG = "und";
    private static final String EMPTY = "";

    private final BigInteger id;
    private final String entityId;
    private final String canonicalId;
    private final Schema schema;
    private final Property property;
    private final String dataset;
    private final String value;
    private final String lang;
    private final String originalValue;
    private final boolean external;
    private final long firstSeen;
    private final long lastSeen;

    public Statement(BigInteger id, String entityId, String canonicalId, Schema schema, Property property, String dataset, String value, String lang, String originalValue, boolean external, long firstSeen, long lastSeen) {
        this.id = id;
        this.entityId = entityId;
        this.canonicalId = canonicalId.equals(entityId) ? EMPTY : canonicalId;
        this.schema = schema;
        this.property = property;
        this.dataset = dataset.intern();
        this.value = value;
        this.lang = lang.length() == 0 ? EMPTY : lang;
        this.originalValue = originalValue.length() == 0 ? EMPTY : originalValue;
        this.external = external;
        this.firstSeen = firstSeen;
        this.lastSeen = lastSeen;
    }

    public BigInteger getId() {
        return id;
    }

    public String getIdString() {
        return id.toString(16);
    }

    public String getEntityId() {
        return entityId;
    }

    @SuppressWarnings("StringEquality")
    public String getCanonicalId() {
        return canonicalId == EMPTY ? entityId : canonicalId;
    }

    public Schema getSchema() {
        return schema;
    }

    public Property getProperty() {
        return property;
    }

    public String getDatasetName() {
        return dataset;
    }

    public String getValue() {
        return value;
    }

    public String getLang() {
        return lang;
    }

    public String getOriginalValue() {
        return originalValue;
    }

    public boolean isExternal() {
        return external;
    }

    public long getFirstSeen() {
        return firstSeen;
    }

    public long getLastSeen() {
        return lastSeen;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof Statement)) {
            return false;
        }
        Statement other = (Statement) obj;
        return id.equals(other.id);
    }
}
