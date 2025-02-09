package org.opensanctions.zahir.server;

import java.io.IOException;
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

    private Map<String, Session> sessions = new HashMap<>();

    // TODO: linker instances should be session-scoped and managed by the server
    protected final Linker linker;

    public ZahirManager() throws IOException{
        this.model = Model.loadDefault();
        this.store = new Store(model, "/Users/pudo/Code/zahir/data/exp1");
        this.linker = Linker.fromJsonPath("/Users/pudo/Code/operations/etl/data/resolve.ijson");
        this.sessions = new HashMap<>();
    }

    public Store getStore() {
        return store;
    }

    public Model getModel() {
        return model;
    }

    public Session createSession() {
        Session session = new Session(this);
        sessions.put(session.getId(), session);
        return session;
    }

    public Session closeSession(String id) {
        return sessions.remove(id);
    }
}
