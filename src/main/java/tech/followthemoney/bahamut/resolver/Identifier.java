package tech.followthemoney.bahamut.resolver;

import java.math.BigInteger;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

public class Identifier implements Comparable<Identifier> {
    private static final String PREFIX = "NK-";
    private static final Pattern QID = Pattern.compile("^Q\\d+$");
    private static final String ALPHABET = "23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    private static final BigInteger BASE57 = BigInteger.valueOf(57);
    protected final String id;
    private final int weight;
    private final boolean qid;
    private final boolean canonical;

    public Identifier(String id) {
        this.id = id;
        this.qid = QID.matcher(id).matches();
        if (qid) {
            this.weight = 3;
        } else if (id.startsWith(PREFIX)) {
            this.weight = 2;
        }else {
            this.weight = 1;
        }
        this.canonical = this.weight > 1;
    }

    public boolean isQid() {
        return qid;
    }

    public boolean isCanonical() {
        return canonical;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof Identifier)) return false;
        return id.equals(o.toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return id;
    }

    public int length() {
        return id.length();
    }

    @Override
    public int compareTo(Identifier other) {
        int weightCompare = Integer.compare(this.weight, other.weight);
        return weightCompare != 0 ? weightCompare : this.id.compareTo(other.id);
    }

    public static Identifier generate() {
        UUID uuid = UUID.randomUUID();
        BigInteger number = new BigInteger(1, uuidToBytes(uuid));
        
        // Convert to Base57
        StringBuilder identifier = new StringBuilder();
        while (number.compareTo(BigInteger.ZERO) > 0) {
            BigInteger[] divmod = number.divideAndRemainder(BASE57);
            identifier.insert(0, ALPHABET.charAt(divmod[1].intValue()));
            number = divmod[0];
        }
        return new Identifier(PREFIX + identifier.toString());
    }

    private static byte[] uuidToBytes(UUID uuid) {
        byte[] bytes = new byte[16];
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) ((msb >> ((7 - i) * 8)) & 0xff);
            bytes[i + 8] = (byte) ((lsb >> ((7 - i) * 8)) & 0xff);
        }
        return bytes;
    }
}