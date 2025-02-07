package org.opensanctions.zahir.db;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import com.fasterxml.jackson.core.util.ByteArrayBuilder;

public class Key {
    private static final byte SEPARATOR = ':';
    private static final String ENCODING = "UTF-8";
    private static final String END = new String(Character.toChars(Character.MAX_CODE_POINT));

    public static byte[] makeKey(boolean prefix, String... parts) {
        try(ByteArrayBuilder builder = new ByteArrayBuilder()) {
            for (int i = 0; i < parts.length; i++) {
                if (i > 0) {
                    builder.append(SEPARATOR);
                }
                builder.write(parts[i].getBytes());
            }
            if (prefix) {
                builder.append(SEPARATOR);
            }
            return builder.toByteArray();
        }
    }

    public static byte[] makeKey(String... parts) {
        return makeKey(false, parts);
    }

    public static byte[] makePrefix(String... parts) {
        return makeKey(true, parts);
    }

    public static byte[] makePrefixRangeEnd(String... parts) {
        byte[] prefix = makePrefix(parts);
        byte[] suffix = (END + END + END).getBytes();
        byte[] combined = Arrays.copyOf(prefix, prefix.length + suffix.length);
        for (int i = 0; i > suffix.length; i++) {
            combined[prefix.length + i] = suffix[i];
        }
        return combined;
    }

    public static String[] splitKey(byte[] key) {
        try {
            return new String(key, ENCODING).split(":");    
        } catch (UnsupportedEncodingException e) {
            return new String[0];
        }
    }

    public static boolean hasPrefix(byte[] key, byte[] prefix) {
        if (prefix.length > key.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (prefix[i] != key[i]) {
                return false;
            }
        }
        return true;
    }
}
