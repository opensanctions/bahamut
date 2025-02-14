package org.opensanctions.zahir;

public class Config {
    public static final String APP_NAME = "Zahir";
    public static final int PORT = System.getenv("PORT") != null ? Integer.parseInt(System.getenv("ZAHIR_PORT")) : 6674;

    // Can this be shorter?
    public static final long LOCK_TIMEOUT = 84600 * 7 * 1000;
}
