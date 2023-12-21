package com.scalar.db.rpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.60.0)",
    comments = "Source: scalardb.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class DistributedStorageGrpc {

  private DistributedStorageGrpc() {}

  public static final java.lang.String SERVICE_NAME = "rpc.DistributedStorage";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.GetRequest,
      com.scalar.db.rpc.GetResponse> getGetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Get",
      requestType = com.scalar.db.rpc.GetRequest.class,
      responseType = com.scalar.db.rpc.GetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.GetRequest,
      com.scalar.db.rpc.GetResponse> getGetMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.GetRequest, com.scalar.db.rpc.GetResponse> getGetMethod;
    if ((getGetMethod = DistributedStorageGrpc.getGetMethod) == null) {
      synchronized (DistributedStorageGrpc.class) {
        if ((getGetMethod = DistributedStorageGrpc.getGetMethod) == null) {
          DistributedStorageGrpc.getGetMethod = getGetMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.GetRequest, com.scalar.db.rpc.GetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Get"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.GetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.GetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedStorageMethodDescriptorSupplier("Get"))
              .build();
        }
      }
    }
    return getGetMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.ScanRequest,
      com.scalar.db.rpc.ScanResponse> getScanMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Scan",
      requestType = com.scalar.db.rpc.ScanRequest.class,
      responseType = com.scalar.db.rpc.ScanResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.ScanRequest,
      com.scalar.db.rpc.ScanResponse> getScanMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.ScanRequest, com.scalar.db.rpc.ScanResponse> getScanMethod;
    if ((getScanMethod = DistributedStorageGrpc.getScanMethod) == null) {
      synchronized (DistributedStorageGrpc.class) {
        if ((getScanMethod = DistributedStorageGrpc.getScanMethod) == null) {
          DistributedStorageGrpc.getScanMethod = getScanMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.ScanRequest, com.scalar.db.rpc.ScanResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Scan"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.ScanRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.ScanResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedStorageMethodDescriptorSupplier("Scan"))
              .build();
        }
      }
    }
    return getScanMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.MutateRequest,
      com.google.protobuf.Empty> getMutateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Mutate",
      requestType = com.scalar.db.rpc.MutateRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.MutateRequest,
      com.google.protobuf.Empty> getMutateMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.MutateRequest, com.google.protobuf.Empty> getMutateMethod;
    if ((getMutateMethod = DistributedStorageGrpc.getMutateMethod) == null) {
      synchronized (DistributedStorageGrpc.class) {
        if ((getMutateMethod = DistributedStorageGrpc.getMutateMethod) == null) {
          DistributedStorageGrpc.getMutateMethod = getMutateMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.MutateRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Mutate"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.MutateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedStorageMethodDescriptorSupplier("Mutate"))
              .build();
        }
      }
    }
    return getMutateMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DistributedStorageStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DistributedStorageStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DistributedStorageStub>() {
        @java.lang.Override
        public DistributedStorageStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DistributedStorageStub(channel, callOptions);
        }
      };
    return DistributedStorageStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DistributedStorageBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DistributedStorageBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DistributedStorageBlockingStub>() {
        @java.lang.Override
        public DistributedStorageBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DistributedStorageBlockingStub(channel, callOptions);
        }
      };
    return DistributedStorageBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static DistributedStorageFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DistributedStorageFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DistributedStorageFutureStub>() {
        @java.lang.Override
        public DistributedStorageFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DistributedStorageFutureStub(channel, callOptions);
        }
      };
    return DistributedStorageFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void get(com.scalar.db.rpc.GetRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.GetResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetMethod(), responseObserver);
    }

    /**
     */
    default io.grpc.stub.StreamObserver<com.scalar.db.rpc.ScanRequest> scan(
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.ScanResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getScanMethod(), responseObserver);
    }

    /**
     */
    default void mutate(com.scalar.db.rpc.MutateRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMutateMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service DistributedStorage.
   */
  public static abstract class DistributedStorageImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return DistributedStorageGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service DistributedStorage.
   */
  public static final class DistributedStorageStub
      extends io.grpc.stub.AbstractAsyncStub<DistributedStorageStub> {
    private DistributedStorageStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DistributedStorageStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DistributedStorageStub(channel, callOptions);
    }

    /**
     */
    public void get(com.scalar.db.rpc.GetRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.GetResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<com.scalar.db.rpc.ScanRequest> scan(
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.ScanResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getScanMethod(), getCallOptions()), responseObserver);
    }

    /**
     */
    public void mutate(com.scalar.db.rpc.MutateRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getMutateMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service DistributedStorage.
   */
  public static final class DistributedStorageBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<DistributedStorageBlockingStub> {
    private DistributedStorageBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DistributedStorageBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DistributedStorageBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.scalar.db.rpc.GetResponse get(com.scalar.db.rpc.GetRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty mutate(com.scalar.db.rpc.MutateRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getMutateMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service DistributedStorage.
   */
  public static final class DistributedStorageFutureStub
      extends io.grpc.stub.AbstractFutureStub<DistributedStorageFutureStub> {
    private DistributedStorageFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DistributedStorageFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DistributedStorageFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.scalar.db.rpc.GetResponse> get(
        com.scalar.db.rpc.GetRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> mutate(
        com.scalar.db.rpc.MutateRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getMutateMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET = 0;
  private static final int METHODID_MUTATE = 1;
  private static final int METHODID_SCAN = 2;

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
        case METHODID_GET:
          serviceImpl.get((com.scalar.db.rpc.GetRequest) request,
              (io.grpc.stub.StreamObserver<com.scalar.db.rpc.GetResponse>) responseObserver);
          break;
        case METHODID_MUTATE:
          serviceImpl.mutate((com.scalar.db.rpc.MutateRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
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
        case METHODID_SCAN:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.scan(
              (io.grpc.stub.StreamObserver<com.scalar.db.rpc.ScanResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getGetMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.scalar.db.rpc.GetRequest,
              com.scalar.db.rpc.GetResponse>(
                service, METHODID_GET)))
        .addMethod(
          getScanMethod(),
          io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
            new MethodHandlers<
              com.scalar.db.rpc.ScanRequest,
              com.scalar.db.rpc.ScanResponse>(
                service, METHODID_SCAN)))
        .addMethod(
          getMutateMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.scalar.db.rpc.MutateRequest,
              com.google.protobuf.Empty>(
                service, METHODID_MUTATE)))
        .build();
  }

  private static abstract class DistributedStorageBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    DistributedStorageBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.scalar.db.rpc.ScalarDbProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("DistributedStorage");
    }
  }

  private static final class DistributedStorageFileDescriptorSupplier
      extends DistributedStorageBaseDescriptorSupplier {
    DistributedStorageFileDescriptorSupplier() {}
  }

  private static final class DistributedStorageMethodDescriptorSupplier
      extends DistributedStorageBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    DistributedStorageMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (DistributedStorageGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new DistributedStorageFileDescriptorSupplier())
              .addMethod(getGetMethod())
              .addMethod(getScanMethod())
              .addMethod(getMutateMethod())
              .build();
        }
      }
    }
    return result;
  }
}
