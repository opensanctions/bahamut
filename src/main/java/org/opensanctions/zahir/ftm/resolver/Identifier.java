package org.opensanctions.zahir.ftm.resolver;

import java.util.Objects;
import java.util.regex.Pattern;

public class Identifier implements Comparable<Identifier> {
    private static final String PREFIX = "NK-";
    private static final Pattern QID = Pattern.compile("^Q\\d+$");
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
}