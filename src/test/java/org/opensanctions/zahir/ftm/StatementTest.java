package org.opensanctions.zahir.ftm;

import java.io.IOException;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opensanctions.zahir.ftm.model.Model;
import org.opensanctions.zahir.ftm.model.Schema;


public class StatementTest {
    private static Model model;
    private static final BigInteger ID = BigInteger.valueOf(123);
    private static final String ENTITY_ID = "entity1";
    private static final String CANONICAL_ID = "canon1";
    private static final String PROP_NAME = "name";
    private static final String DATASET = "test";
    private static final String VALUE = "Harry Test";
    private static final String LANG = "eng";
    private static final String ORIG_VALUE = "Mr. Harry (Baron) Test";

    @BeforeAll
    public static void setUp() throws IOException {
        model = Model.loadDefault();
    }
    
    @Test
    public void testBasicConstructionAndGetters() {
        Schema schema = model.getSchema("Person");
        Statement stmt = new Statement(ID, ENTITY_ID, CANONICAL_ID, schema, 
            PROP_NAME, DATASET, VALUE, LANG, ORIG_VALUE, false, 100L, 200L);
            
        assertEquals(ID, stmt.getId());
        assertEquals(ENTITY_ID, stmt.getEntityId());
        assertEquals(CANONICAL_ID, stmt.getCanonicalId());
        assertEquals(schema, stmt.getSchema());
        assertEquals(PROP_NAME, stmt.getPropertyName());
        assertEquals(DATASET, stmt.getDatasetName());
        assertEquals(VALUE, stmt.getValue());
        assertEquals(LANG, stmt.getLang());
        assertEquals(ORIG_VALUE, stmt.getOriginalValue());
        assertFalse(stmt.isExternal());
        assertEquals(100L, stmt.getFirstSeen());
        assertEquals(200L, stmt.getLastSeen());
    }

    @Test
    public void testEmptyOptionals() {
        Schema schema = model.getSchema("Person");
        Statement stmt = new Statement(ID, ENTITY_ID, CANONICAL_ID, schema,
            PROP_NAME, DATASET, VALUE, "", "", false, 100L, 200L);
            
        assertEquals("", stmt.getLang());
        assertEquals("", stmt.getOriginalValue());
    }

    @Test
    public void testEquality() {
        Schema schema = model.getSchema("Person");
        Statement stmt1 = new Statement(ID, ENTITY_ID, CANONICAL_ID, schema,
            PROP_NAME, DATASET, VALUE, LANG, ORIG_VALUE, false, 100L, 200L);
        Statement stmt2 = new Statement(ID, "different", "different", schema,
            "different", "different", "different", "xx", "different", true, 300L, 400L);
            
        assertEquals(stmt1, stmt2); // Should be equal as they have same ID
        assertEquals(stmt1.hashCode(), stmt2.hashCode());
    }

    @Test
    public void testCanonicalIdHandling() {
        Schema schema = model.getSchema("Person");
        Statement stmt1 = new Statement(ID, ENTITY_ID, ENTITY_ID, schema,
            PROP_NAME, DATASET, VALUE, LANG, ORIG_VALUE, false, 100L, 200L);
            
        assertEquals(ENTITY_ID, stmt1.getCanonicalId());
        
        Statement stmt2 = stmt1.withCanonicalId(CANONICAL_ID);
        assertEquals(CANONICAL_ID, stmt2.getCanonicalId());
    }
}

