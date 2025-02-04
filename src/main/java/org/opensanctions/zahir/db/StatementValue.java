package org.opensanctions.zahir.db;

import org.opensanctions.zahir.ftm.Statement;
import org.opensanctions.zahir.ftm.model.Schema;

public class StatementValue {
    private final String value;
    private final String lang;
    private final String originalValue;
    private final long firstSeen;
    private final long lastSeen;

    public StatementValue(String value, String lang, String originalValue, long firstSeen, long lastSeen) {
        this.value = value;
        this.lang = lang;
        this.originalValue = originalValue;
        this.firstSeen = firstSeen;
        this.lastSeen = lastSeen;
    }

    public static StatementValue fromStatement(Statement statement) {
        return new StatementValue(statement.getValue(), statement.getLang(), statement.getOriginalValue(), statement.getFirstSeen(), statement.getLastSeen());
    }

    public Statement toStatement(String id, String entityId, String canonicalId, Schema schema, String dataset, String propertyName, boolean external, StatementValue value) {
        return new Statement(id, entityId, canonicalId, schema, propertyName, dataset, value.value, value.lang, value.originalValue, external, value.firstSeen, value.lastSeen);
    }
}
