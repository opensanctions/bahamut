package org.opensanctions.zahir.server;

import org.opensanctions.zahir.db.Store;
import org.opensanctions.zahir.db.StoreWriter;
import org.opensanctions.zahir.ftm.model.Model;
import org.opensanctions.zahir.ftm.model.Schema;
import org.opensanctions.zahir.ftm.statement.Statement;
import org.opensanctions.zahir.server.proto.v1.WriteDatasetResponse;
import org.opensanctions.zahir.server.proto.v1.WriteStatement;
import org.opensanctions.zahir.server.proto.v1.WriterServiceGrpc;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;

public class WriterServiceImpl extends WriterServiceGrpc.WriterServiceImplBase {
    private final static Logger log = LoggerFactory.getLogger(WriterServiceImpl.class);
    private final ZahirManager manager;

    public WriterServiceImpl(ZahirManager manager) {
        super();
        this.manager = manager;
    }

    @Override
    public StreamObserver<WriteStatement> writeDataset(StreamObserver<WriteDatasetResponse> responseObserver) {

        return new StreamObserver<WriteStatement>() {
            private final Model model = manager.getModel();
            private StoreWriter writer;
            private String dataset;
            private int writtenCount = 0;

            @Override
            public void onNext(WriteStatement msg) {
                if (writer == null) {
                    if (!msg.hasDataset()) {
                        log.error("No dataset spec provided.");
                        responseObserver.onError(new IllegalArgumentException("No dataset spec provided."));
                        return;
                    }
                    Store store = manager.getStore();
                    dataset = msg.getDataset();
                    writer = store.getWriter(dataset, msg.getVersion());
                } else if (msg.hasDataset() || !msg.getDataset().equals(dataset)) {
                    log.error("Dataset spec changes after dataset writer was initialized.");
                    responseObserver.onError(
                            new IllegalArgumentException("Dataset spec provided after dataset was initialized."));
                    return;
                }
                try {
                    Schema schema = model.getSchema(msg.getSchema());
                    String entityId = msg.getEntityId();
                    String propertyName = msg.getProperty();
                    boolean external = msg.hasExternal() ? msg.getExternal() : false;
                    String value = msg.getValue();
                    String stmtId = msg.hasId() ? msg.getId() : Statement.makeId(dataset, entityId, propertyName, value, external);
                    Statement stmt = new Statement(stmtId, entityId, entityId, schema,
                            propertyName, dataset, value, msg.getLang(), msg.getOriginalValue(),
                            external, msg.getFirstSeen(), msg.getLastSeen());
                    writer.writeStatement(stmt);
                    writtenCount++;
                } catch (RocksDBException re) {
                    log.error("Error processing write command:", re);
                    responseObserver.onError(re);
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("Error in write stream:", t);
                responseObserver.onError(t);
            }

            @Override
            public void onCompleted() {
                try {
                    if (writer != null) {
                        writer.release();
                        writer.close();
                    }
                } catch (RocksDBException e) {
                    log.error("Error closing writer:", e);
                    responseObserver.onError(e);
                }

                WriteDatasetResponse response = WriteDatasetResponse.newBuilder()
                        .setEntitiesWritten(writtenCount)
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };
    }
}
