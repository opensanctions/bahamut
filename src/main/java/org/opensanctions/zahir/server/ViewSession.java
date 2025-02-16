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
    private final Linker linker;
    private final String id;
    private final Map<String, String> scope;
    private final boolean withExternal;

    public ViewSession(ZahirManager manager, Map<String, String> scope, boolean unResolved, boolean withExternal) throws RocksDBException {
        this.manager = manager;
        this.scope = scope;
        this.withExternal = withExternal;
        this.linker = unResolved ? new Linker() : manager.linker;
        
        Store store = manager.getStore();
        StoreView view = store.getView(manager.linker, scope, withExternal);
        this.id = view.lockScope();
    }

    public String getId() {
        return id;
    }

    public Linker getLinker() {
        return linker;
    }

    public StoreView getStoreView() {
        Store store = manager.getStore();
        StoreView view = store.getView(getLinker(), scope, withExternal);
        view.resumeLock(id);
        return view;
    }

    public void close() throws ViewException {
        StoreView view = getStoreView();
        view.close();
    }

}
