package org.opensanctions.zahir.db;

import java.io.UnsupportedEncodingException;

import com.fasterxml.jackson.core.util.ByteArrayBuilder;

public class Key {
    private static final byte SEPARATOR = ':';
    private static final String ENCODING = "UTF-8";

    public static byte[] makeKey(String... parts) {
        try(ByteArrayBuilder builder = new ByteArrayBuilder()) {
            for (int i = 0; i < parts.length; i++) {
                if (i > 0) {
                    builder.append(SEPARATOR);
                }
                builder.write(parts[i].getBytes());
            }
            return builder.toByteArray();
        }
    }

    public static String[] splitKey(byte[] key) {
        try {
            return new String(key, ENCODING).split(":");    
        } catch (UnsupportedEncodingException e) {
            return new String[0];
        }
    }
}
