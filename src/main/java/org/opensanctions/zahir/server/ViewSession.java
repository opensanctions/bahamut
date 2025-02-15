package org.opensanctions.zahir.server;

import java.util.Map;

import org.opensanctions.zahir.db.Store;
import org.opensanctions.zahir.db.StoreView;
import org.opensanctions.zahir.resolver.Linker;
import org.rocksdb.RocksDBException;

import tech.followthemoney.exc.ViewException;

public class ViewSession {
    // private final static Logger log = LoggerFactory.getLogger(ViewSession.class);

    private final ZahirManager manager;
    private final String id;
    private final Map<String, String> scope;

    public ViewSession(ZahirManager manager, Map<String, String> scope) throws RocksDBException {
        this.manager = manager;
        this.scope = scope;
        
        Store store = manager.getStore();
        StoreView view = store.getView(manager.linker, scope);
        this.id = view.lockScope();
    }

    public String getId() {
        return id;
    }

    public Linker getLinker() {
        return manager.linker;
    }

    public StoreView getStoreView() {
        Store store = manager.getStore();
        StoreView view = store.getView(getLinker(), scope);
        view.resumeLock(id);
        return view;
    }

    public void close() throws ViewException {
        StoreView view = getStoreView();
        view.close();
    }

}
