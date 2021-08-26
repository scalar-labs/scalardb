package com.scalar.db.server;

import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.stub.StreamObserver;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class HealthService extends HealthGrpc.HealthImplBase {

  @Override
  public void check(
      HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
    HealthCheckResponse.Builder builder = HealthCheckResponse.newBuilder();
    builder.setStatus(HealthCheckResponse.ServingStatus.SERVING);
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }
}
