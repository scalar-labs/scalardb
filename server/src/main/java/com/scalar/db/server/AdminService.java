package com.scalar.db.server;

import com.google.protobuf.Empty;
import com.scalar.admin.rpc.AdminGrpc;
import com.scalar.admin.rpc.CheckPausedResponse;
import com.scalar.admin.rpc.PauseRequest;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class AdminService extends AdminGrpc.AdminImplBase {
  private static final Logger logger = LoggerFactory.getLogger(AdminService.class);

  private static final long DEFAULT_MAX_PAUSE_WAIT_TIME_MILLIS = 30000; // 30 seconds

  private final GateKeeper gateKeeper;

  public AdminService(GateKeeper gateKeeper) {
    this.gateKeeper = gateKeeper;
  }

  @Override
  public void pause(PauseRequest request, StreamObserver<Empty> responseObserver) {
    gateKeeper.close();

    if (request.getWaitOutstanding()) {
      logger.warn("Pausing... waiting until outstanding requests are all finished");
      boolean drained = false;
      long maxPauseWaitTime =
          request.getMaxPauseWaitTime() != 0
              ? request.getMaxPauseWaitTime()
              : DEFAULT_MAX_PAUSE_WAIT_TIME_MILLIS;

      try {
        drained = gateKeeper.awaitDrained(maxPauseWaitTime, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        // ignored
      }
      if (!drained) {
        gateKeeper.open();
        String message = "failed to finish processing outstanding requests within the time limit.";
        logger.warn(message);
        responseObserver.onError(
            Status.FAILED_PRECONDITION.withDescription(message).asRuntimeException());
        return;
      }
    }
    logger.warn("Paused");

    responseObserver.onNext(Empty.getDefaultInstance());
    responseObserver.onCompleted();
  }

  @Override
  public void unpause(Empty request, StreamObserver<Empty> responseObserver) {
    gateKeeper.open();
    logger.warn("Unpaused");
    responseObserver.onNext(Empty.getDefaultInstance());
    responseObserver.onCompleted();
  }

  @Override
  public void checkPaused(Empty request, StreamObserver<CheckPausedResponse> responseObserver) {
    responseObserver.onNext(
        CheckPausedResponse.newBuilder().setPaused(!gateKeeper.isOpen()).build());
    responseObserver.onCompleted();
  }
}
