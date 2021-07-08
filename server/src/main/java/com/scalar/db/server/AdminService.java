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
import javax.json.Json;
import javax.json.JsonObject;
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
      long start = System.currentTimeMillis();
      while (pauser.outstandingRequestsExist()) {
        try {
          TimeUnit.MILLISECONDS.sleep(100);
        } catch (InterruptedException ignored) {
        }

        if (System.currentTimeMillis() - start >= MAX_PAUSE_WAIT_TIME_MILLIS) {
          String message = "Failed to pause";
          LOGGER.warn(message);
          responseObserver.onError(Status.INTERNAL.withDescription(message).asRuntimeException());
          return;
        }
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
    JsonObject json =
        Json.createObjectBuilder().add("outstanding", pauser.getOutstandingRequestCount()).build();
    responseObserver.onNext(StatsResponse.newBuilder().setStats(json.toString()).build());
    responseObserver.onCompleted();
  }
}
