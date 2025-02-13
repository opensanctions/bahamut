package org.opensanctions.zahir.server;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.opensanctions.zahir.db.Store;
import org.opensanctions.zahir.ftm.model.Model;
import org.opensanctions.zahir.ftm.resolver.Linker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZahirManager {
    private final static Logger log = LoggerFactory.getLogger(ZahirManager.class);
    private final Model model;
    private final Store store;

    private Map<String, ViewSession> sessions = new HashMap<>();

    // TODO: linker instances should be view session-scoped and managed by the server
    protected final Linker linker;

    public ZahirManager() throws IOException{
        this.model = Model.loadDefault();
        Path cwd = Paths.get("").toAbsolutePath();
        this.store = new Store(model, cwd.resolve("data/db").toString());
        log.info("Store initialized at: {}", store.getPath());
        this.linker = Linker.fromJsonPath("/Users/pudo/Code/operations/etl/data/resolve.ijson");
        this.sessions = new HashMap<>();
    }

    public Store getStore() {
        return store;
    }

    public Model getModel() {
        return model;
    }

    public ViewSession createSession(Map<String, String> scope) {
        ViewSession session = new ViewSession(this, scope);
        sessions.put(session.getId(), session);
        return session;
    }

    public ViewSession getSession(String id) {
        return sessions.get(id);
    }

    public ViewSession closeSession(String id) {
        return sessions.remove(id);
    }
}
