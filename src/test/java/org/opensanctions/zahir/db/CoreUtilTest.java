package org.opensanctions.zahir.db;

import org.junit.jupiter.api.Test;

public class CoreUtilTest {
    
    @Test
    public void testGetTimestampValue() {
        long timestamp = CoreUtil.getTimestamp();
        assert timestamp > 0 : "Timestamp should be greater than 0";
        byte[] timestampValue = CoreUtil.getTimestampValue();
        assert timestampValue != null : "Timestamp value should not be null";
        assert timestampValue.length > 0 : "Timestamp value should not be empty";
        assert Long.parseLong(new String(timestampValue)) > 0 : "Timestamp value should match the current timestamp";
    }
    
    @Test
    public void testMakeRandomId() {
        String randomId = CoreUtil.makeRandomId();
        assert randomId != null : "Random ID should not be null";
        assert randomId.length() == 32 : "Random ID should be 32 characters long";
        assert randomId.matches("[a-f0-9]+") : "Random ID should contain only hexadecimal characters";
    }
}
