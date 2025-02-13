package org.opensanctions.zahir.server;

import java.util.HashMap;
import java.util.Map;

import org.opensanctions.zahir.server.proto.v1.CloseViewRequest;
import org.opensanctions.zahir.server.proto.v1.CloseViewResponse;
import org.opensanctions.zahir.server.proto.v1.CreateViewRequest;
import org.opensanctions.zahir.server.proto.v1.CreateViewResponse;
import org.opensanctions.zahir.server.proto.v1.DatasetSpec;
import org.opensanctions.zahir.server.proto.v1.GetDatasetsRequest;
import org.opensanctions.zahir.server.proto.v1.GetDatasetsResponse;
import org.opensanctions.zahir.server.proto.v1.ViewServiceGrpc;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;

public class ViewServiceImpl extends ViewServiceGrpc.ViewServiceImplBase {
    private final static Logger log = LoggerFactory.getLogger(ViewServiceImpl.class);
    private final ZahirManager manager;

    public ViewServiceImpl(ZahirManager manager) {
        super();
        this.manager = manager;
    }

    @Override
    public void getDatasets(GetDatasetsRequest request, StreamObserver<GetDatasetsResponse> responseObserver) {
        try {
            Map<String, String> datasets = manager.getStore().getDatasets();
            // GetDatasetsResponse response = GetDatasetsResponse.newBuilder()
            //     .
            //     .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RocksDBException e) {
            log.error("Cannot retrieve datasets from store", e);
            responseObserver.onError(e);
            responseObserver.onCompleted();
        }
    }
    
    @Override
    public void createView(CreateViewRequest request, StreamObserver<CreateViewResponse> responseObserver) {
        Map<String, String> scope = new HashMap<>();
        for (DatasetSpec spec : request.getScopeList()) {
            scope.put(spec.getName(), spec.getVersion());
        }
        if (scope.isEmpty()) {
            try {
                scope.putAll(manager.getStore().getDatasets());
            } catch (RocksDBException e) {
                log.error("Cannot retrieve currently loaded datasets from store", e);
                responseObserver.onError(e);
                responseObserver.onCompleted();
                return;
            }
        }
        ViewSession session = manager.createSession(scope);
        log.info("New client session: {}", session.getId());
        CreateViewResponse response = CreateViewResponse.newBuilder()
            .setViewId(session.getId())
            .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void closeView(CloseViewRequest request, StreamObserver<CloseViewResponse> responseObserver) {
        CloseViewResponse response = CloseViewResponse.newBuilder()
            .setSuccess(true)
            .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
