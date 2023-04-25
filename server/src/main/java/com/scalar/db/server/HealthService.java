package com.scalar.db.server;

import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class HealthService extends HealthGrpc.HealthImplBase {

  private final AtomicReference<HealthCheckResponse.ServingStatus> servingStatus =
      new AtomicReference<>(HealthCheckResponse.ServingStatus.SERVING);

  @Override
  public void check(
      HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
    HealthCheckResponse.Builder builder = HealthCheckResponse.newBuilder();
    builder.setStatus(servingStatus.get());
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  /** Decommissions the cluster node from the cluster */
  public void decommission() {
    servingStatus.set(HealthCheckResponse.ServingStatus.NOT_SERVING);
  }
}
