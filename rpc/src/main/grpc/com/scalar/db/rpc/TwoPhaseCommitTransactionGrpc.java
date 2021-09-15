package com.scalar.db.rpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.38.0)",
    comments = "Source: scalardb.proto")
public final class TwoPhaseCommitTransactionGrpc {

  private TwoPhaseCommitTransactionGrpc() {}

  public static final String SERVICE_NAME = "rpc.TwoPhaseCommitTransaction";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.TwoPhaseCommitTransactionRequest,
      com.scalar.db.rpc.TwoPhaseCommitTransactionResponse> getTwoPhaseCommitTransactionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "TwoPhaseCommitTransaction",
      requestType = com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.class,
      responseType = com.scalar.db.rpc.TwoPhaseCommitTransactionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.TwoPhaseCommitTransactionRequest,
      com.scalar.db.rpc.TwoPhaseCommitTransactionResponse> getTwoPhaseCommitTransactionMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.TwoPhaseCommitTransactionRequest, com.scalar.db.rpc.TwoPhaseCommitTransactionResponse> getTwoPhaseCommitTransactionMethod;
    if ((getTwoPhaseCommitTransactionMethod = TwoPhaseCommitTransactionGrpc.getTwoPhaseCommitTransactionMethod) == null) {
      synchronized (TwoPhaseCommitTransactionGrpc.class) {
        if ((getTwoPhaseCommitTransactionMethod = TwoPhaseCommitTransactionGrpc.getTwoPhaseCommitTransactionMethod) == null) {
          TwoPhaseCommitTransactionGrpc.getTwoPhaseCommitTransactionMethod = getTwoPhaseCommitTransactionMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.TwoPhaseCommitTransactionRequest, com.scalar.db.rpc.TwoPhaseCommitTransactionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "TwoPhaseCommitTransaction"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.TwoPhaseCommitTransactionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TwoPhaseCommitTransactionMethodDescriptorSupplier("TwoPhaseCommitTransaction"))
              .build();
        }
      }
    }
    return getTwoPhaseCommitTransactionMethod;
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
    if ((getGetStateMethod = TwoPhaseCommitTransactionGrpc.getGetStateMethod) == null) {
      synchronized (TwoPhaseCommitTransactionGrpc.class) {
        if ((getGetStateMethod = TwoPhaseCommitTransactionGrpc.getGetStateMethod) == null) {
          TwoPhaseCommitTransactionGrpc.getGetStateMethod = getGetStateMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.GetTransactionStateRequest, com.scalar.db.rpc.GetTransactionStateResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetState"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.GetTransactionStateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.GetTransactionStateResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TwoPhaseCommitTransactionMethodDescriptorSupplier("GetState"))
              .build();
        }
      }
    }
    return getGetStateMethod;
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
    if ((getAbortMethod = TwoPhaseCommitTransactionGrpc.getAbortMethod) == null) {
      synchronized (TwoPhaseCommitTransactionGrpc.class) {
        if ((getAbortMethod = TwoPhaseCommitTransactionGrpc.getAbortMethod) == null) {
          TwoPhaseCommitTransactionGrpc.getAbortMethod = getAbortMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.AbortRequest, com.scalar.db.rpc.AbortResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Abort"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.AbortRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.AbortResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TwoPhaseCommitTransactionMethodDescriptorSupplier("Abort"))
              .build();
        }
      }
    }
    return getAbortMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static TwoPhaseCommitTransactionStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TwoPhaseCommitTransactionStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TwoPhaseCommitTransactionStub>() {
        @java.lang.Override
        public TwoPhaseCommitTransactionStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TwoPhaseCommitTransactionStub(channel, callOptions);
        }
      };
    return TwoPhaseCommitTransactionStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static TwoPhaseCommitTransactionBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TwoPhaseCommitTransactionBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TwoPhaseCommitTransactionBlockingStub>() {
        @java.lang.Override
        public TwoPhaseCommitTransactionBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TwoPhaseCommitTransactionBlockingStub(channel, callOptions);
        }
      };
    return TwoPhaseCommitTransactionBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static TwoPhaseCommitTransactionFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TwoPhaseCommitTransactionFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TwoPhaseCommitTransactionFutureStub>() {
        @java.lang.Override
        public TwoPhaseCommitTransactionFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TwoPhaseCommitTransactionFutureStub(channel, callOptions);
        }
      };
    return TwoPhaseCommitTransactionFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class TwoPhaseCommitTransactionImplBase implements io.grpc.BindableService {

    /**
     */
    public io.grpc.stub.StreamObserver<com.scalar.db.rpc.TwoPhaseCommitTransactionRequest> twoPhaseCommitTransaction(
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.TwoPhaseCommitTransactionResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getTwoPhaseCommitTransactionMethod(), responseObserver);
    }

    /**
     */
    public void getState(com.scalar.db.rpc.GetTransactionStateRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.GetTransactionStateResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetStateMethod(), responseObserver);
    }

    /**
     */
    public void abort(com.scalar.db.rpc.AbortRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.AbortResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAbortMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getTwoPhaseCommitTransactionMethod(),
            io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
              new MethodHandlers<
                com.scalar.db.rpc.TwoPhaseCommitTransactionRequest,
                com.scalar.db.rpc.TwoPhaseCommitTransactionResponse>(
                  this, METHODID_TWO_PHASE_COMMIT_TRANSACTION)))
          .addMethod(
            getGetStateMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.GetTransactionStateRequest,
                com.scalar.db.rpc.GetTransactionStateResponse>(
                  this, METHODID_GET_STATE)))
          .addMethod(
            getAbortMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.AbortRequest,
                com.scalar.db.rpc.AbortResponse>(
                  this, METHODID_ABORT)))
          .build();
    }
  }

  /**
   */
  public static final class TwoPhaseCommitTransactionStub extends io.grpc.stub.AbstractAsyncStub<TwoPhaseCommitTransactionStub> {
    private TwoPhaseCommitTransactionStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TwoPhaseCommitTransactionStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TwoPhaseCommitTransactionStub(channel, callOptions);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<com.scalar.db.rpc.TwoPhaseCommitTransactionRequest> twoPhaseCommitTransaction(
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.TwoPhaseCommitTransactionResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getTwoPhaseCommitTransactionMethod(), getCallOptions()), responseObserver);
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
    public void abort(com.scalar.db.rpc.AbortRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.AbortResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAbortMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class TwoPhaseCommitTransactionBlockingStub extends io.grpc.stub.AbstractBlockingStub<TwoPhaseCommitTransactionBlockingStub> {
    private TwoPhaseCommitTransactionBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TwoPhaseCommitTransactionBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TwoPhaseCommitTransactionBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.scalar.db.rpc.GetTransactionStateResponse getState(com.scalar.db.rpc.GetTransactionStateRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetStateMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.scalar.db.rpc.AbortResponse abort(com.scalar.db.rpc.AbortRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAbortMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class TwoPhaseCommitTransactionFutureStub extends io.grpc.stub.AbstractFutureStub<TwoPhaseCommitTransactionFutureStub> {
    private TwoPhaseCommitTransactionFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TwoPhaseCommitTransactionFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TwoPhaseCommitTransactionFutureStub(channel, callOptions);
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
    public com.google.common.util.concurrent.ListenableFuture<com.scalar.db.rpc.AbortResponse> abort(
        com.scalar.db.rpc.AbortRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAbortMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_STATE = 0;
  private static final int METHODID_ABORT = 1;
  private static final int METHODID_TWO_PHASE_COMMIT_TRANSACTION = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final TwoPhaseCommitTransactionImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(TwoPhaseCommitTransactionImplBase serviceImpl, int methodId) {
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
        case METHODID_TWO_PHASE_COMMIT_TRANSACTION:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.twoPhaseCommitTransaction(
              (io.grpc.stub.StreamObserver<com.scalar.db.rpc.TwoPhaseCommitTransactionResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class TwoPhaseCommitTransactionBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    TwoPhaseCommitTransactionBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.scalar.db.rpc.ScalarDbProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("TwoPhaseCommitTransaction");
    }
  }

  private static final class TwoPhaseCommitTransactionFileDescriptorSupplier
      extends TwoPhaseCommitTransactionBaseDescriptorSupplier {
    TwoPhaseCommitTransactionFileDescriptorSupplier() {}
  }

  private static final class TwoPhaseCommitTransactionMethodDescriptorSupplier
      extends TwoPhaseCommitTransactionBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    TwoPhaseCommitTransactionMethodDescriptorSupplier(String methodName) {
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
      synchronized (TwoPhaseCommitTransactionGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new TwoPhaseCommitTransactionFileDescriptorSupplier())
              .addMethod(getTwoPhaseCommitTransactionMethod())
              .addMethod(getGetStateMethod())
              .addMethod(getAbortMethod())
              .build();
        }
      }
    }
    return result;
  }
}
