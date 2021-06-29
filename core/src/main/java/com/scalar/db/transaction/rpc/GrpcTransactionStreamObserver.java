package com.scalar.db.transaction.rpc;

import com.scalar.db.rpc.TransactionResponse;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;

public class GrpcTransactionStreamObserver implements StreamObserver<TransactionResponse> {

  private TransactionResponse response;
  private Throwable error;
  private CountDownLatch latch;

  @Override
  public void onNext(TransactionResponse response) {
    this.response = response;
    latch.countDown();
  }

  @Override
  public void onError(Throwable t) {
    this.error = t;
    latch.countDown();
  }

  @Override
  public void onCompleted() {}

  /** Need to call this method before calling StreamObserver.onNext() */
  public void init() {
    latch = new CountDownLatch(1);
    response = null;
    error = null;
  }

  public void await() throws InterruptedException {
    latch.await();
  }

  public boolean hasError() {
    return error != null;
  }

  public TransactionResponse getResponse() {
    return response;
  }

  public Throwable getError() {
    return error;
  }
}
