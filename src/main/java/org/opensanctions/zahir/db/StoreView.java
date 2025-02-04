package org.opensanctions.zahir.db;

import java.util.List;

public class StoreView {
    private final Store store;
    private final List<String> datasets;

    public StoreView(Store store, List<String> datasets) {
        this.store = store;
        this.datasets = datasets;
    }
}
