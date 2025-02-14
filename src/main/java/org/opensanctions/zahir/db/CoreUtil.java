package org.opensanctions.zahir.db;

import java.util.UUID;

public class CoreUtil {

    public static long getTimestamp() {
        return System.currentTimeMillis();
    }

    public static byte[] getTimestampValue() {
        return Long.toString(getTimestamp()).getBytes();
    }

    public static String makeRandomId() {
        return UUID.randomUUID().toString().replace("-", "");
    }
    
}
