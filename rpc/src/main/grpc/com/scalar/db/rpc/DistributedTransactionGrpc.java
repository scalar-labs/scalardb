package com.scalar.db.rpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.60.0)",
    comments = "Source: scalardb.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class DistributedTransactionGrpc {

  private DistributedTransactionGrpc() {}

  public static final java.lang.String SERVICE_NAME = "rpc.DistributedTransaction";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.TransactionRequest,
      com.scalar.db.rpc.TransactionResponse> getTransactionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Transaction",
      requestType = com.scalar.db.rpc.TransactionRequest.class,
      responseType = com.scalar.db.rpc.TransactionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.TransactionRequest,
      com.scalar.db.rpc.TransactionResponse> getTransactionMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.TransactionRequest, com.scalar.db.rpc.TransactionResponse> getTransactionMethod;
    if ((getTransactionMethod = DistributedTransactionGrpc.getTransactionMethod) == null) {
      synchronized (DistributedTransactionGrpc.class) {
        if ((getTransactionMethod = DistributedTransactionGrpc.getTransactionMethod) == null) {
          DistributedTransactionGrpc.getTransactionMethod = getTransactionMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.TransactionRequest, com.scalar.db.rpc.TransactionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Transaction"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.TransactionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.TransactionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionMethodDescriptorSupplier("Transaction"))
              .build();
        }
      }
    }
    return getTransactionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.GetTransactionStateRequest,
      com.scalar.db.rpc.GetTransactionStateResponse> getGetStateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetState",
      requestType = com.scalar.db.rpc.GetTransactionStateRequest.class,
      responseType = com.scalar.db.rpc.GetTransactionStateResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.GetTransactionStateRequest,
      com.scalar.db.rpc.GetTransactionStateResponse> getGetStateMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.GetTransactionStateRequest, com.scalar.db.rpc.GetTransactionStateResponse> getGetStateMethod;
    if ((getGetStateMethod = DistributedTransactionGrpc.getGetStateMethod) == null) {
      synchronized (DistributedTransactionGrpc.class) {
        if ((getGetStateMethod = DistributedTransactionGrpc.getGetStateMethod) == null) {
          DistributedTransactionGrpc.getGetStateMethod = getGetStateMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.GetTransactionStateRequest, com.scalar.db.rpc.GetTransactionStateResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetState"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.GetTransactionStateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.GetTransactionStateResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionMethodDescriptorSupplier("GetState"))
              .build();
        }
      }
    }
    return getGetStateMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.RollbackRequest,
      com.scalar.db.rpc.RollbackResponse> getRollbackMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Rollback",
      requestType = com.scalar.db.rpc.RollbackRequest.class,
      responseType = com.scalar.db.rpc.RollbackResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.RollbackRequest,
      com.scalar.db.rpc.RollbackResponse> getRollbackMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.RollbackRequest, com.scalar.db.rpc.RollbackResponse> getRollbackMethod;
    if ((getRollbackMethod = DistributedTransactionGrpc.getRollbackMethod) == null) {
      synchronized (DistributedTransactionGrpc.class) {
        if ((getRollbackMethod = DistributedTransactionGrpc.getRollbackMethod) == null) {
          DistributedTransactionGrpc.getRollbackMethod = getRollbackMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.RollbackRequest, com.scalar.db.rpc.RollbackResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Rollback"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.RollbackRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.RollbackResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionMethodDescriptorSupplier("Rollback"))
              .build();
        }
      }
    }
    return getRollbackMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.AbortRequest,
      com.scalar.db.rpc.AbortResponse> getAbortMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Abort",
      requestType = com.scalar.db.rpc.AbortRequest.class,
      responseType = com.scalar.db.rpc.AbortResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.AbortRequest,
      com.scalar.db.rpc.AbortResponse> getAbortMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.AbortRequest, com.scalar.db.rpc.AbortResponse> getAbortMethod;
    if ((getAbortMethod = DistributedTransactionGrpc.getAbortMethod) == null) {
      synchronized (DistributedTransactionGrpc.class) {
        if ((getAbortMethod = DistributedTransactionGrpc.getAbortMethod) == null) {
          DistributedTransactionGrpc.getAbortMethod = getAbortMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.AbortRequest, com.scalar.db.rpc.AbortResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Abort"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.AbortRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.AbortResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionMethodDescriptorSupplier("Abort"))
              .build();
        }
      }
    }
    return getAbortMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DistributedTransactionStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DistributedTransactionStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DistributedTransactionStub>() {
        @java.lang.Override
        public DistributedTransactionStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DistributedTransactionStub(channel, callOptions);
        }
      };
    return DistributedTransactionStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DistributedTransactionBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DistributedTransactionBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DistributedTransactionBlockingStub>() {
        @java.lang.Override
        public DistributedTransactionBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DistributedTransactionBlockingStub(channel, callOptions);
        }
      };
    return DistributedTransactionBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static DistributedTransactionFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DistributedTransactionFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DistributedTransactionFutureStub>() {
        @java.lang.Override
        public DistributedTransactionFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DistributedTransactionFutureStub(channel, callOptions);
        }
      };
    return DistributedTransactionFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default io.grpc.stub.StreamObserver<com.scalar.db.rpc.TransactionRequest> transaction(
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.TransactionResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getTransactionMethod(), responseObserver);
    }

    /**
     */
    default void getState(com.scalar.db.rpc.GetTransactionStateRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.GetTransactionStateResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetStateMethod(), responseObserver);
    }

    /**
     */
    default void rollback(com.scalar.db.rpc.RollbackRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.RollbackResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRollbackMethod(), responseObserver);
    }

    /**
     */
    default void abort(com.scalar.db.rpc.AbortRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.AbortResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAbortMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service DistributedTransaction.
   */
  public static abstract class DistributedTransactionImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return DistributedTransactionGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service DistributedTransaction.
   */
  public static final class DistributedTransactionStub
      extends io.grpc.stub.AbstractAsyncStub<DistributedTransactionStub> {
    private DistributedTransactionStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DistributedTransactionStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DistributedTransactionStub(channel, callOptions);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<com.scalar.db.rpc.TransactionRequest> transaction(
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.TransactionResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getTransactionMethod(), getCallOptions()), responseObserver);
    }

    /**
     */
    public void getState(com.scalar.db.rpc.GetTransactionStateRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.GetTransactionStateResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetStateMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void rollback(com.scalar.db.rpc.RollbackRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.RollbackResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRollbackMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void abort(com.scalar.db.rpc.AbortRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.AbortResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAbortMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service DistributedTransaction.
   */
  public static final class DistributedTransactionBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<DistributedTransactionBlockingStub> {
    private DistributedTransactionBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DistributedTransactionBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DistributedTransactionBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.scalar.db.rpc.GetTransactionStateResponse getState(com.scalar.db.rpc.GetTransactionStateRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetStateMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.scalar.db.rpc.RollbackResponse rollback(com.scalar.db.rpc.RollbackRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRollbackMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.scalar.db.rpc.AbortResponse abort(com.scalar.db.rpc.AbortRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAbortMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service DistributedTransaction.
   */
  public static final class DistributedTransactionFutureStub
      extends io.grpc.stub.AbstractFutureStub<DistributedTransactionFutureStub> {
    private DistributedTransactionFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DistributedTransactionFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DistributedTransactionFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.scalar.db.rpc.GetTransactionStateResponse> getState(
        com.scalar.db.rpc.GetTransactionStateRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetStateMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.scalar.db.rpc.RollbackResponse> rollback(
        com.scalar.db.rpc.RollbackRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRollbackMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.scalar.db.rpc.AbortResponse> abort(
        com.scalar.db.rpc.AbortRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAbortMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_STATE = 0;
  private static final int METHODID_ROLLBACK = 1;
  private static final int METHODID_ABORT = 2;
  private static final int METHODID_TRANSACTION = 3;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_STATE:
          serviceImpl.getState((com.scalar.db.rpc.GetTransactionStateRequest) request,
              (io.grpc.stub.StreamObserver<com.scalar.db.rpc.GetTransactionStateResponse>) responseObserver);
          break;
        case METHODID_ROLLBACK:
          serviceImpl.rollback((com.scalar.db.rpc.RollbackRequest) request,
              (io.grpc.stub.StreamObserver<com.scalar.db.rpc.RollbackResponse>) responseObserver);
          break;
        case METHODID_ABORT:
          serviceImpl.abort((com.scalar.db.rpc.AbortRequest) request,
              (io.grpc.stub.StreamObserver<com.scalar.db.rpc.AbortResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_TRANSACTION:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.transaction(
              (io.grpc.stub.StreamObserver<com.scalar.db.rpc.TransactionResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getTransactionMethod(),
          io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
            new MethodHandlers<
              com.scalar.db.rpc.TransactionRequest,
              com.scalar.db.rpc.TransactionResponse>(
                service, METHODID_TRANSACTION)))
        .addMethod(
          getGetStateMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.scalar.db.rpc.GetTransactionStateRequest,
              com.scalar.db.rpc.GetTransactionStateResponse>(
                service, METHODID_GET_STATE)))
        .addMethod(
          getRollbackMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.scalar.db.rpc.RollbackRequest,
              com.scalar.db.rpc.RollbackResponse>(
                service, METHODID_ROLLBACK)))
        .addMethod(
          getAbortMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.scalar.db.rpc.AbortRequest,
              com.scalar.db.rpc.AbortResponse>(
                service, METHODID_ABORT)))
        .build();
  }

  private static abstract class DistributedTransactionBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    DistributedTransactionBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.scalar.db.rpc.ScalarDbProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("DistributedTransaction");
    }
  }

  private static final class DistributedTransactionFileDescriptorSupplier
      extends DistributedTransactionBaseDescriptorSupplier {
    DistributedTransactionFileDescriptorSupplier() {}
  }

  private static final class DistributedTransactionMethodDescriptorSupplier
      extends DistributedTransactionBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    DistributedTransactionMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (DistributedTransactionGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new DistributedTransactionFileDescriptorSupplier())
              .addMethod(getTransactionMethod())
              .addMethod(getGetStateMethod())
              .addMethod(getRollbackMethod())
              .addMethod(getAbortMethod())
              .build();
        }
      }
    }
    return result;
  }
}
