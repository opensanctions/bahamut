package org.opensanctions.zahir.ftm.meta;

import java.util.HashMap;
import java.util.Map;

public class Catalog {
    private final Map<String, Dataset> datasets;

    public Catalog() {
        this.datasets = new HashMap<>();        
    }

    public Dataset getDataset(String name) {
        return datasets.get(name);
    }

    public void addDataset(Dataset dataset) {
        datasets.put(dataset.getName(), dataset);
    }

    public Dataset toDataset(String name, String label) {
        return datasets.computeIfAbsent(name, k -> new Dataset(this, name, label));
    }

    public Dataset toDataset(String name) {
        return toDataset(name, name);
    }
}