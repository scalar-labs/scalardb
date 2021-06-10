package com.scalar.db.rpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.38.0)",
    comments = "Source: scalardb.proto")
public final class DistributedTransactionGrpc {

  private DistributedTransactionGrpc() {}

  public static final String SERVICE_NAME = "rpc.DistributedTransaction";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.StartTransactionRequest,
      com.scalar.db.rpc.StartTransactionResponse> getStartMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Start",
      requestType = com.scalar.db.rpc.StartTransactionRequest.class,
      responseType = com.scalar.db.rpc.StartTransactionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.StartTransactionRequest,
      com.scalar.db.rpc.StartTransactionResponse> getStartMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.StartTransactionRequest, com.scalar.db.rpc.StartTransactionResponse> getStartMethod;
    if ((getStartMethod = DistributedTransactionGrpc.getStartMethod) == null) {
      synchronized (DistributedTransactionGrpc.class) {
        if ((getStartMethod = DistributedTransactionGrpc.getStartMethod) == null) {
          DistributedTransactionGrpc.getStartMethod = getStartMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.StartTransactionRequest, com.scalar.db.rpc.StartTransactionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Start"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.StartTransactionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.StartTransactionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionMethodDescriptorSupplier("Start"))
              .build();
        }
      }
    }
    return getStartMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.TransactionalGetRequest,
      com.scalar.db.rpc.TransactionalGetResponse> getGetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Get",
      requestType = com.scalar.db.rpc.TransactionalGetRequest.class,
      responseType = com.scalar.db.rpc.TransactionalGetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.TransactionalGetRequest,
      com.scalar.db.rpc.TransactionalGetResponse> getGetMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.TransactionalGetRequest, com.scalar.db.rpc.TransactionalGetResponse> getGetMethod;
    if ((getGetMethod = DistributedTransactionGrpc.getGetMethod) == null) {
      synchronized (DistributedTransactionGrpc.class) {
        if ((getGetMethod = DistributedTransactionGrpc.getGetMethod) == null) {
          DistributedTransactionGrpc.getGetMethod = getGetMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.TransactionalGetRequest, com.scalar.db.rpc.TransactionalGetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Get"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.TransactionalGetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.TransactionalGetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionMethodDescriptorSupplier("Get"))
              .build();
        }
      }
    }
    return getGetMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.TransactionalScanRequest,
      com.scalar.db.rpc.TransactionalScanResponse> getScanMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Scan",
      requestType = com.scalar.db.rpc.TransactionalScanRequest.class,
      responseType = com.scalar.db.rpc.TransactionalScanResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.TransactionalScanRequest,
      com.scalar.db.rpc.TransactionalScanResponse> getScanMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.TransactionalScanRequest, com.scalar.db.rpc.TransactionalScanResponse> getScanMethod;
    if ((getScanMethod = DistributedTransactionGrpc.getScanMethod) == null) {
      synchronized (DistributedTransactionGrpc.class) {
        if ((getScanMethod = DistributedTransactionGrpc.getScanMethod) == null) {
          DistributedTransactionGrpc.getScanMethod = getScanMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.TransactionalScanRequest, com.scalar.db.rpc.TransactionalScanResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Scan"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.TransactionalScanRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.TransactionalScanResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionMethodDescriptorSupplier("Scan"))
              .build();
        }
      }
    }
    return getScanMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.TransactionalMutateRequest,
      com.google.protobuf.Empty> getMutateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Mutate",
      requestType = com.scalar.db.rpc.TransactionalMutateRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.TransactionalMutateRequest,
      com.google.protobuf.Empty> getMutateMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.TransactionalMutateRequest, com.google.protobuf.Empty> getMutateMethod;
    if ((getMutateMethod = DistributedTransactionGrpc.getMutateMethod) == null) {
      synchronized (DistributedTransactionGrpc.class) {
        if ((getMutateMethod = DistributedTransactionGrpc.getMutateMethod) == null) {
          DistributedTransactionGrpc.getMutateMethod = getMutateMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.TransactionalMutateRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Mutate"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.TransactionalMutateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionMethodDescriptorSupplier("Mutate"))
              .build();
        }
      }
    }
    return getMutateMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.CommitRequest,
      com.google.protobuf.Empty> getCommitMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Commit",
      requestType = com.scalar.db.rpc.CommitRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.CommitRequest,
      com.google.protobuf.Empty> getCommitMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.CommitRequest, com.google.protobuf.Empty> getCommitMethod;
    if ((getCommitMethod = DistributedTransactionGrpc.getCommitMethod) == null) {
      synchronized (DistributedTransactionGrpc.class) {
        if ((getCommitMethod = DistributedTransactionGrpc.getCommitMethod) == null) {
          DistributedTransactionGrpc.getCommitMethod = getCommitMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.CommitRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Commit"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.CommitRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionMethodDescriptorSupplier("Commit"))
              .build();
        }
      }
    }
    return getCommitMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.AbortRequest,
      com.google.protobuf.Empty> getAbortMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Abort",
      requestType = com.scalar.db.rpc.AbortRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.AbortRequest,
      com.google.protobuf.Empty> getAbortMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.AbortRequest, com.google.protobuf.Empty> getAbortMethod;
    if ((getAbortMethod = DistributedTransactionGrpc.getAbortMethod) == null) {
      synchronized (DistributedTransactionGrpc.class) {
        if ((getAbortMethod = DistributedTransactionGrpc.getAbortMethod) == null) {
          DistributedTransactionGrpc.getAbortMethod = getAbortMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.AbortRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Abort"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.AbortRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionMethodDescriptorSupplier("Abort"))
              .build();
        }
      }
    }
    return getAbortMethod;
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
  public static abstract class DistributedTransactionImplBase implements io.grpc.BindableService {

    /**
     */
    public void start(com.scalar.db.rpc.StartTransactionRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.StartTransactionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStartMethod(), responseObserver);
    }

    /**
     */
    public void get(com.scalar.db.rpc.TransactionalGetRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.TransactionalGetResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetMethod(), responseObserver);
    }

    /**
     */
    public void scan(com.scalar.db.rpc.TransactionalScanRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.TransactionalScanResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getScanMethod(), responseObserver);
    }

    /**
     */
    public void mutate(com.scalar.db.rpc.TransactionalMutateRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMutateMethod(), responseObserver);
    }

    /**
     */
    public void commit(com.scalar.db.rpc.CommitRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCommitMethod(), responseObserver);
    }

    /**
     */
    public void abort(com.scalar.db.rpc.AbortRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAbortMethod(), responseObserver);
    }

    /**
     */
    public void getState(com.scalar.db.rpc.GetTransactionStateRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.GetTransactionStateResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetStateMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getStartMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.StartTransactionRequest,
                com.scalar.db.rpc.StartTransactionResponse>(
                  this, METHODID_START)))
          .addMethod(
            getGetMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.TransactionalGetRequest,
                com.scalar.db.rpc.TransactionalGetResponse>(
                  this, METHODID_GET)))
          .addMethod(
            getScanMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.TransactionalScanRequest,
                com.scalar.db.rpc.TransactionalScanResponse>(
                  this, METHODID_SCAN)))
          .addMethod(
            getMutateMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.TransactionalMutateRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_MUTATE)))
          .addMethod(
            getCommitMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.CommitRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_COMMIT)))
          .addMethod(
            getAbortMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.AbortRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_ABORT)))
          .addMethod(
            getGetStateMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.GetTransactionStateRequest,
                com.scalar.db.rpc.GetTransactionStateResponse>(
                  this, METHODID_GET_STATE)))
          .build();
    }
  }

  /**
   */
  public static final class DistributedTransactionStub extends io.grpc.stub.AbstractAsyncStub<DistributedTransactionStub> {
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
    public void start(com.scalar.db.rpc.StartTransactionRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.StartTransactionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getStartMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void get(com.scalar.db.rpc.TransactionalGetRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.TransactionalGetResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void scan(com.scalar.db.rpc.TransactionalScanRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.TransactionalScanResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getScanMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void mutate(com.scalar.db.rpc.TransactionalMutateRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getMutateMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void commit(com.scalar.db.rpc.CommitRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCommitMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void abort(com.scalar.db.rpc.AbortRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAbortMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getState(com.scalar.db.rpc.GetTransactionStateRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.GetTransactionStateResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetStateMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class DistributedTransactionBlockingStub extends io.grpc.stub.AbstractBlockingStub<DistributedTransactionBlockingStub> {
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
    public com.scalar.db.rpc.StartTransactionResponse start(com.scalar.db.rpc.StartTransactionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getStartMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.scalar.db.rpc.TransactionalGetResponse get(com.scalar.db.rpc.TransactionalGetRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.scalar.db.rpc.TransactionalScanResponse scan(com.scalar.db.rpc.TransactionalScanRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getScanMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty mutate(com.scalar.db.rpc.TransactionalMutateRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getMutateMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty commit(com.scalar.db.rpc.CommitRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCommitMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty abort(com.scalar.db.rpc.AbortRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAbortMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.scalar.db.rpc.GetTransactionStateResponse getState(com.scalar.db.rpc.GetTransactionStateRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetStateMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class DistributedTransactionFutureStub extends io.grpc.stub.AbstractFutureStub<DistributedTransactionFutureStub> {
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
    public com.google.common.util.concurrent.ListenableFuture<com.scalar.db.rpc.StartTransactionResponse> start(
        com.scalar.db.rpc.StartTransactionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getStartMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.scalar.db.rpc.TransactionalGetResponse> get(
        com.scalar.db.rpc.TransactionalGetRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.scalar.db.rpc.TransactionalScanResponse> scan(
        com.scalar.db.rpc.TransactionalScanRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getScanMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> mutate(
        com.scalar.db.rpc.TransactionalMutateRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getMutateMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> commit(
        com.scalar.db.rpc.CommitRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCommitMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> abort(
        com.scalar.db.rpc.AbortRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAbortMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.scalar.db.rpc.GetTransactionStateResponse> getState(
        com.scalar.db.rpc.GetTransactionStateRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetStateMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_START = 0;
  private static final int METHODID_GET = 1;
  private static final int METHODID_SCAN = 2;
  private static final int METHODID_MUTATE = 3;
  private static final int METHODID_COMMIT = 4;
  private static final int METHODID_ABORT = 5;
  private static final int METHODID_GET_STATE = 6;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final DistributedTransactionImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(DistributedTransactionImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_START:
          serviceImpl.start((com.scalar.db.rpc.StartTransactionRequest) request,
              (io.grpc.stub.StreamObserver<com.scalar.db.rpc.StartTransactionResponse>) responseObserver);
          break;
        case METHODID_GET:
          serviceImpl.get((com.scalar.db.rpc.TransactionalGetRequest) request,
              (io.grpc.stub.StreamObserver<com.scalar.db.rpc.TransactionalGetResponse>) responseObserver);
          break;
        case METHODID_SCAN:
          serviceImpl.scan((com.scalar.db.rpc.TransactionalScanRequest) request,
              (io.grpc.stub.StreamObserver<com.scalar.db.rpc.TransactionalScanResponse>) responseObserver);
          break;
        case METHODID_MUTATE:
          serviceImpl.mutate((com.scalar.db.rpc.TransactionalMutateRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_COMMIT:
          serviceImpl.commit((com.scalar.db.rpc.CommitRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_ABORT:
          serviceImpl.abort((com.scalar.db.rpc.AbortRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_GET_STATE:
          serviceImpl.getState((com.scalar.db.rpc.GetTransactionStateRequest) request,
              (io.grpc.stub.StreamObserver<com.scalar.db.rpc.GetTransactionStateResponse>) responseObserver);
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
        default:
          throw new AssertionError();
      }
    }
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
    private final String methodName;

    DistributedTransactionMethodDescriptorSupplier(String methodName) {
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
              .addMethod(getStartMethod())
              .addMethod(getGetMethod())
              .addMethod(getScanMethod())
              .addMethod(getMutateMethod())
              .addMethod(getCommitMethod())
              .addMethod(getAbortMethod())
              .addMethod(getGetStateMethod())
              .build();
        }
      }
    }
    return result;
  }
}
