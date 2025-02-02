package org.opensanctions.zahir.ftm.model;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

public class ModelTest {
    @Test
    public void testLoadDefault() throws IOException {
        Model model = Model.loadDefault();
        assertNotNull(model);
        assertFalse(model.getSchemata().isEmpty());
        assertFalse(model.getTypes().isEmpty());
        
        // Test getting a known schema type
        Schema legalEntity = model.getSchema("LegalEntity");
        Schema person = model.getSchema("Person");
        assertNotNull(person);
        assertEquals("Person", person.getName());
        assertTrue(person.getExtends().size() == 1);
        assertTrue(person.isA(legalEntity));
        assertTrue(person.isA(person));
        
        // Test getting a known property type
        PropertyType type = model.getType("name");
        assertNotNull(type);
        assertEquals("name", type.getName());
        assertEquals(true, type.isName());

        // Test getting a known property
        Property property = person.getProperty("name");
        assertNotNull(property);
        assertEquals("name", property.getName());
        assertEquals("Thing:name", property.getQName());
        assertEquals("Name", property.getLabel());
        assertEquals(type, property.getType());

        Schema ownership = model.getSchema("Ownership");
        Property owner = ownership.getProperty("owner");
        assertTrue(owner.getType().isEntity());
        assertTrue(owner.getRange().isPresent());
        assertTrue(owner.getRange().get() == legalEntity);
    }
}
