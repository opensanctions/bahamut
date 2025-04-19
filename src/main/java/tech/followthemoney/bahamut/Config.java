package tech.followthemoney.bahamut;

public class Config {
    public static final String APP_NAME = "Bahamut";
    public static final int PORT = System.getenv("BAHAMUT_PORT") != null ? Integer.parseInt(System.getenv("BAHAMUT_PORT")) : 6674;
    public static final String DATA_PATH = System.getenv("BAHAMUT_DATA_PATH") != null ? System.getenv("BAHAMUT_DATA_PATH") : "data/db";

    // Auto-clean old dataset versions
    public static final int KEEP_VERSIONS = 2;

    // Can this be shorter?
    public static final long LOCK_TIMEOUT = 84600 * 7 * 1000;
}
