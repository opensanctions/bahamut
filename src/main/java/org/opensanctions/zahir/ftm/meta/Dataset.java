package org.opensanctions.zahir.ftm.meta;

public class Dataset {
    private final Catalog catalog;
    private final String name;
    private String label;

    public Dataset(Catalog catalog, String name, String label) {
        this.catalog = catalog;
        this.name = name.intern();
        this.label = label;
    }

    public String getName() {
        return name;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }
}
