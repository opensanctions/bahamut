package tech.followthemoney.bahamut;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.followthemoney.bahamut.server.BahamutServer;

public class App {
    private final static Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        try{
            BahamutServer server = new BahamutServer();
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
