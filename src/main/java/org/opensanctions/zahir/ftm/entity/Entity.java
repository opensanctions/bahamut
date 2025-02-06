package org.opensanctions.zahir.ftm.entity;

import java.util.List;
import java.util.Set;

import org.opensanctions.zahir.ftm.model.Property;
import org.opensanctions.zahir.ftm.model.Schema;

public abstract class Entity {
    protected String id;
    protected Schema schema;
    protected String caption;

    public Entity(String id, Schema schema) {
        this.id = id;
        this.schema = schema;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Schema getSchema() {
        return schema;
    }

    protected abstract String pickCaption();

    public String getCaption() {
        if (caption == null) {
            caption = pickCaption();
        }
        return caption;
    }
    
    public void setCaption(String caption) {
        this.caption = caption;
    }

    public abstract Set<String> getDatasets();
    public abstract Set<String> getReferents();
    public abstract String getFirstSeen();
    public abstract String getLastSeen();
    public abstract String getLastChange();

    public abstract List<String> getValues(Property property);
    // FIXME: implemnent getLastChange()
}
