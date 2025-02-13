package org.opensanctions.zahir.server;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public class ZahirServer {
    private final static Logger log = LoggerFactory.getLogger(ZahirServer.class);

    private final int port;
    private final Server server;
    private final ZahirManager manager;

    public ZahirServer(int port) throws IOException {
        this.port = port;
        this.manager = new ZahirManager();
        this.server = ServerBuilder.forPort(port)
                .addService(new ViewServiceImpl(manager))
                .build();
    }

    public void start() throws IOException {
        server.start();
        log.info("Server started, listening on port " + port);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Shutting down gRPC server due to JVM shutdown");
                try {
                    ZahirServer.this.stop();
                } catch (InterruptedException e) {
                    log.error("Error shutting down: " + e.getMessage());
                }
            }
        });
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}
