package org.opensanctions.zahir.server;

import java.util.Map;
import java.util.UUID;

import org.opensanctions.zahir.db.Store;
import org.opensanctions.zahir.db.StoreView;
import org.opensanctions.zahir.ftm.resolver.Linker;

public class ViewSession {
    private final ZahirManager manager;
    private final String id;
    private final Map<String, String> scope;

    public ViewSession(ZahirManager manager, Map<String, String> scope) {
        this.manager = manager;
        this.scope = scope;
        this.id = UUID.randomUUID().toString().replace("-", "");
    }

    public String getId() {
        return id;
    }

    public Linker getLinker() {
        return manager.linker;
    }

    public StoreView getStoreView() {
        Store store = manager.getStore();
        return store.getView(getLinker(), scope);
    }


}
