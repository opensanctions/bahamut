package org.opensanctions.zahir.server;

import java.util.UUID;

import org.opensanctions.zahir.ftm.resolver.Linker;

public class Session {
    private final ZahirManager manager;
    private final String id;

    public Session(ZahirManager manager) {
        this.manager = manager;
        this.id = UUID.randomUUID().toString().replace("-", "");
    }

    public String getId() {
        return id;
    }

    public Linker getLinker() {
        return manager.linker;
    }


}
