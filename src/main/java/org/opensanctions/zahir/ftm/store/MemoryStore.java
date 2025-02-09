package org.opensanctions.zahir.ftm.store;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.opensanctions.zahir.ftm.entity.Entity;
import org.opensanctions.zahir.ftm.model.PropertyType;

public class MemoryStore<E extends Entity> {
    private final Map<String, E> entityMap;
    private final Map<String, Set<String>> inverted;

    public MemoryStore() {
        this.entityMap = new HashMap<>();
        this.inverted = new HashMap<>();
    }

    public void put(E entity) {
        PropertyType entityType = entity.getSchema().getModel().getType(PropertyType.ENTITY);
        for (String value : entity.getTypeValues(entityType, false)) {
            Set<String> entities = inverted.computeIfAbsent(value, k -> new HashSet<>());
            entities.add(entity.getId());
        }
        entityMap.put(entity.getId(), entity);
    }

    public Optional<E> getEntity(String entityId) {
        E entity = entityMap.get(entityId);
        if (entity == null) {
            return Optional.empty();
        }
        return Optional.of(entity);
    }

    public Iterator<E> entities() {
        return entityMap.values().iterator();
    }

    public void delete(String entityId) {
        E entity = entityMap.remove(entityId);
        if (entity != null) {
            PropertyType entityType = entity.getSchema().getModel().getType(PropertyType.ENTITY);
            for (String value : entity.getTypeValues(entityType, false)) {
                if (inverted.containsKey(value)) {
                    Set<String> entities = inverted.get(value);
                    entities.remove(entityId);
                    if (entities.isEmpty()) {
                        inverted.remove(value);
                    }
                }
            }
        }
    }

    public void clear() {
        entityMap.clear();
        inverted.clear();
    }

    public void close() {
        clear();
    }
}
