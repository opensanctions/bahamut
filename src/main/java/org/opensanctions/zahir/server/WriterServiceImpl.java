package org.opensanctions.zahir.server;

import org.opensanctions.zahir.db.Store;
import org.opensanctions.zahir.db.StoreWriter;
import org.opensanctions.zahir.ftm.model.Model;
import org.opensanctions.zahir.ftm.model.Schema;
import org.opensanctions.zahir.ftm.statement.Statement;
import org.opensanctions.zahir.server.proto.v1.DatasetSpec;
import org.opensanctions.zahir.server.proto.v1.StatementMessage;
import org.opensanctions.zahir.server.proto.v1.WriteCommand;
import org.opensanctions.zahir.server.proto.v1.WriteDatasetResponse;
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
    public StreamObserver<WriteCommand> writeDataset(StreamObserver<WriteDatasetResponse> responseObserver) {
        
        return new StreamObserver<WriteCommand>() {
            private final Model model = manager.getModel();
            private StoreWriter writer;
            private String dataset;

            @Override
            public void onNext(WriteCommand command) {
                DatasetSpec spec = command.getDataset();
                if (writer == null) {
                    if (spec == null) {
                        log.error("No dataset spec provided.");
                        responseObserver.onError(new IllegalArgumentException("No dataset spec provided."));
                        return;
                    }
                    Store store = manager.getStore();
                    dataset = spec.getName();
                    writer = store.getWriter(dataset, spec.getVersion());
                } else if (spec != null && !spec.getName().equals(dataset)) {
                    log.error("Dataset spec changes after dataset writer was initialized.");
                    responseObserver.onError(new IllegalArgumentException("Dataset spec provided after dataset was initialized."));
                    return;
                }
                try {
                    StatementMessage msg = command.getStatement();
                    Schema schema = model.getSchema(msg.getSchema());
                    Statement stmt = new Statement(msg.getId(), msg.getEntityId(), msg.getEntityId(), schema, msg.getProperty(), dataset, msg.getValue(), msg.getLang(), msg.getOriginalValue(), msg.getExternal(), msg.getFirstSeen(), msg.getLastSeen());
                    writer.writeStatement(stmt);
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
                
                WriteDatasetResponse response = WriteDatasetResponse
                    .newBuilder()
                    .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };
    }
}
