package org.opensanctions.zahir.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.opensanctions.zahir.db.StoreView;
import org.opensanctions.zahir.server.proto.v1.AdjacencyRequest;
import org.opensanctions.zahir.server.proto.v1.AdjacencyResponse;
import org.opensanctions.zahir.server.proto.v1.CloseViewRequest;
import org.opensanctions.zahir.server.proto.v1.CloseViewResponse;
import org.opensanctions.zahir.server.proto.v1.CreateViewRequest;
import org.opensanctions.zahir.server.proto.v1.CreateViewResponse;
import org.opensanctions.zahir.server.proto.v1.DatasetSpec;
import org.opensanctions.zahir.server.proto.v1.EntityRequest;
import org.opensanctions.zahir.server.proto.v1.EntityResponse;
import org.opensanctions.zahir.server.proto.v1.EntityStreamRequest;
import org.opensanctions.zahir.server.proto.v1.GetDatasetVersionsRequest;
import org.opensanctions.zahir.server.proto.v1.GetDatasetVersionsResponse;
import org.opensanctions.zahir.server.proto.v1.GetDatasetsRequest;
import org.opensanctions.zahir.server.proto.v1.GetDatasetsResponse;
import org.opensanctions.zahir.server.proto.v1.ViewEntity;
import org.opensanctions.zahir.server.proto.v1.ViewServiceGrpc;
import org.opensanctions.zahir.server.proto.v1.ViewStatement;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;
import tech.followthemoney.entity.StatementEntity;
import tech.followthemoney.exc.ViewException;
import tech.followthemoney.statement.Statement;
import tech.followthemoney.store.Adjacency;

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
            List<DatasetSpec> specs = new ArrayList<>();
            for (Map.Entry<String, String> entry : datasets.entrySet()) {
                DatasetSpec spec = DatasetSpec.newBuilder()
                    .setName(entry.getKey())
                    .setVersion(entry.getValue())
                    .build();
                specs.add(spec);
            }
            GetDatasetsResponse response = GetDatasetsResponse.newBuilder()
                .addAllDatasets(specs)
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RocksDBException e) {
            log.error("Cannot retrieve datasets from store", e);
            responseObserver.onError(e);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getDatasetVersions(GetDatasetVersionsRequest request, StreamObserver<GetDatasetVersionsResponse> responseObserver) {
        try {
            List<String> versions = manager.getStore().getDatasetVersions(request.getDataset());
            GetDatasetVersionsResponse response = GetDatasetVersionsResponse.newBuilder()
                .addAllVersions(versions)
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RocksDBException e) {
            log.error("Cannot retrieve dataset versions from store", e);
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
        try {
            boolean unResolved = request.getUnresolved();
            boolean withExternal = request.getWithExternal();
            ViewSession session = manager.createSession(scope, unResolved, withExternal);
            log.info("New client session: {}", session.getId());
            CreateViewResponse response = CreateViewResponse.newBuilder()
                .setViewId(session.getId())
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RocksDBException e) {
            log.error("Cannot create new session", e);
            responseObserver.onError(e);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void closeView(CloseViewRequest request, StreamObserver<CloseViewResponse> responseObserver) {
        try {
            log.info("Closing client session: {}", request.getViewId());
            manager.closeSession(request.getViewId());
            CloseViewResponse response = CloseViewResponse.newBuilder()
                .setSuccess(true)
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (ViewException e) {   
            log.error("Cannot close session", e);
            responseObserver.onError(e);
            responseObserver.onCompleted();
        }
    }

    private ViewEntity buildViewEntity(StatementEntity entity) {
        List<ViewStatement> stmts = new ArrayList<>();
        for (Statement stmt : entity.getAllStatements()) {
            ViewStatement msg = ViewStatement.newBuilder()
                .setId(stmt.getId())
                .setEntityId(stmt.getId())
                .setSchema(stmt.getSchema().getName())
                .setProperty(stmt.getPropertyName())
                .setDataset(stmt.getDatasetName())
                .setValue(stmt.getValue())
                .setExternal(stmt.isExternal())
                .setLang(stmt.getLang())
                .setOriginalValue(stmt.getOriginalValue())
                .setFirstSeen(stmt.getFirstSeen())
                .setLastSeen(stmt.getLastSeen())
                .build();
            stmts.add(msg);
        }
        return ViewEntity.newBuilder()
            .setId(entity.getId())
            .setCaption(entity.getCaption())
            .addAllStatements(stmts)
            .build();
    }

    @Override
    public void getEntity(EntityRequest request, StreamObserver<EntityResponse> responseObserver) {
        ViewSession session = manager.getSession(request.getViewId());
        if (session == null) {
            responseObserver.onError(new IllegalArgumentException("No such session"));
            responseObserver.onCompleted();
            return;
        }
        try {
            StoreView view = session.getStoreView();
            Optional<StatementEntity> entity = view.getEntity(request.getEntityId());
            if (entity.isEmpty()) {
                EntityResponse response = EntityResponse.newBuilder().build();
                responseObserver.onNext(response);
            } else {
                EntityResponse response = EntityResponse.newBuilder()
                    .setEntity(buildViewEntity(entity.get()))
                    .build();
                responseObserver.onNext(response);
                
            }
            responseObserver.onCompleted();
        } catch (ViewException e) {
            log.error("Failed to retrieve entity", e);
            responseObserver.onError(e);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getEntities(EntityStreamRequest request, StreamObserver<ViewEntity> responseObserver) {
        ViewSession session = manager.getSession(request.getViewId());
        if (session == null) {
            responseObserver.onError(new IllegalArgumentException("No such session"));
            responseObserver.onCompleted();
            return;
        }
        try {
            StoreView view = session.getStoreView();
            Iterator<StatementEntity> entities = view.allEntities().iterator();
            while (entities.hasNext()) {
                responseObserver.onNext(buildViewEntity(entities.next()));
            }
            responseObserver.onCompleted();
        } catch (ViewException e) {
            log.error("Failed to retrieve entities", e);
            responseObserver.onError(e);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getAdjacent(AdjacencyRequest request, StreamObserver<AdjacencyResponse> responseObserver) {
        ViewSession session = manager.getSession(request.getViewId());
        if (session == null) {
            responseObserver.onError(new IllegalArgumentException("No such session"));
            responseObserver.onCompleted();
            return;
        }
        try {
            StoreView view = session.getStoreView();
            boolean inverted = request.getInverted();
            Optional<StatementEntity> fEntity = view.getEntity(request.getEntityId());
            if (fEntity.isEmpty()) {
                responseObserver.onError(new IllegalArgumentException("No such entity"));
                responseObserver.onCompleted();
                return;
            }
            StatementEntity entity = fEntity.get();
            Stream<Adjacency<StatementEntity>> adjacencies = inverted ? view.getAdjacent(entity) : view.getOutbound(entity);
            Iterator<Adjacency<StatementEntity>> iterator = adjacencies.iterator();
            while (iterator.hasNext()) {
                Adjacency<StatementEntity> adjacency = iterator.next();
                ViewEntity viewEntity = buildViewEntity(adjacency.getEntity());
                AdjacencyResponse resp = AdjacencyResponse.newBuilder()
                    .setProperty(adjacency.getProperty().getName())
                    .setEntity(viewEntity)
                    .build();
                responseObserver.onNext(resp);
            }
            responseObserver.onCompleted();
        } catch (ViewException e) {
            log.error("Failed to retrieve adjacent entities", e);
            responseObserver.onError(e);
            responseObserver.onCompleted();
        }
    }
}
