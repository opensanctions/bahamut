package org.opensanctions.zahir.db;

import org.junit.jupiter.api.Test;

public class KeyTest {
    
    @Test
    public void testKeyCreation() {
        byte[] key = Key.makeKey("hello", "world");
        assert key != null : "Key should not be null";
        assert key.length > 0 : "Key should not be empty";
        
        String[] parts = Key.splitKey(key);
        assert parts != null : "Parts should not be null";
        assert parts.length == 2 : "Key should split into two parts";
        assert parts[0].equals("hello") : "First part of the key should be 'hello'";
        assert parts[1].equals("world") : "Second part of the key should be 'world'";
    }

    @Test
    public void testKeyPrefix() {
        byte[] prefix = Key.makePrefix("hello", "world");
        assert prefix != null : "Prefix should not be null";
        assert prefix.length > 0 : "Prefix should not be empty";
        
        byte[] key = Key.makeKey("hello", "world", "1234");
        assert Key.hasPrefix(key, prefix);

        byte[] end = Key.makePrefixRangeEnd("hello", "world");
        assert end != null : "End should not be null";
        assert end.length > 0 : "End should not be empty";
        assert Key.hasPrefix(end, prefix) : "Key should have prefix";
    }

}
