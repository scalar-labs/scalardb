package com.scalar.db.server;

import com.google.inject.Inject;
import com.google.protobuf.Empty;
import com.scalar.admin.rpc.AdminGrpc;
import com.scalar.admin.rpc.PauseRequest;
import com.scalar.admin.rpc.StatsResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class AdminService extends AdminGrpc.AdminImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(AdminService.class);

  private static final long MAX_PAUSE_WAIT_TIME_MILLIS = 10000; // 10 seconds

  private final Pauser pauser;

  @Inject
  public AdminService(Pauser pauser) {
    this.pauser = pauser;
  }

  @Override
  public void pause(PauseRequest request, StreamObserver<Empty> responseObserver) {
    pauser.pause();

    if (request.getWaitOutstanding()) {
      LOGGER.warn("Pausing... waiting until outstanding requests are all finished");
      if (!pauser.await(MAX_PAUSE_WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS)) {
        pauser.unpause();

        String message = "Failed to pause";
        LOGGER.warn(message);
        responseObserver.onError(Status.INTERNAL.withDescription(message).asRuntimeException());
        return;
      }
    }

    LOGGER.warn("Paused");

    responseObserver.onNext(Empty.getDefaultInstance());
    responseObserver.onCompleted();
  }

  @Override
  public void unpause(Empty request, StreamObserver<Empty> responseObserver) {
    pauser.unpause();
    LOGGER.warn("Unpaused");
    responseObserver.onNext(Empty.getDefaultInstance());
    responseObserver.onCompleted();
  }

  @Override
  public void stats(Empty request, StreamObserver<StatsResponse> responseObserver) {
    // returns empty for now
    responseObserver.onNext(StatsResponse.newBuilder().setStats("{}").build());
    responseObserver.onCompleted();
  }
}
