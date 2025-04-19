package tech.followthemoney.bahamut.server;

import java.util.Map;

import org.rocksdb.RocksDBException;

import tech.followthemoney.bahamut.db.Store;
import tech.followthemoney.bahamut.db.StoreView;
import tech.followthemoney.bahamut.resolver.Linker;
import tech.followthemoney.exc.ViewException;

public class ViewSession {
    // private final static Logger log = LoggerFactory.getLogger(ViewSession.class);

    private final BahamutManager manager;
    private final Linker linker;
    private final String id;
    private final Map<String, String> scope;
    private final boolean withExternal;

    public ViewSession(BahamutManager manager, Map<String, String> scope, boolean unResolved, boolean withExternal) throws RocksDBException {
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
