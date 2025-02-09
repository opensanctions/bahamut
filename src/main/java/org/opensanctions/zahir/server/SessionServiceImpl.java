package org.opensanctions.zahir.server;

import org.opensanctions.zahir.server.proto.v1.CloseSessionRequest;
import org.opensanctions.zahir.server.proto.v1.CloseSessionResponse;
import org.opensanctions.zahir.server.proto.v1.CreateSessionRequest;
import org.opensanctions.zahir.server.proto.v1.CreateSessionResponse;
import org.opensanctions.zahir.server.proto.v1.SessionServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;

public class SessionServiceImpl extends SessionServiceGrpc.SessionServiceImplBase {
    private final static Logger log = LoggerFactory.getLogger(SessionServiceImpl.class);
    private final ZahirManager manager;

    public SessionServiceImpl(ZahirManager manager) {
        super();
        this.manager = manager;
    }

    
    @Override
    public void createSession(CreateSessionRequest request, StreamObserver<CreateSessionResponse> responseObserver) {
        Session session = manager.createSession();
        log.info("New client session: {}", session.getId());
        CreateSessionResponse response = CreateSessionResponse.newBuilder()
            .setSessionId(session.getId())
            .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void closeSession(CloseSessionRequest request, StreamObserver<CloseSessionResponse> responseObserver) {
        CloseSessionResponse response = CloseSessionResponse.newBuilder()
            .setSuccess(true)
            .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
