package org.opensanctions.zahir;

import java.io.IOException;

import org.opensanctions.zahir.server.ZahirServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private final static Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        try{
            ZahirServer server = new ZahirServer();
            server.start();
            try {
                server.blockUntilShutdown();    
            } catch (InterruptedException e) {
                log.error("Server interrupted: " + e.getMessage());
            }
        } catch (IOException e) {
            log.error("Server caused IO error", e);
        }
    }
}
