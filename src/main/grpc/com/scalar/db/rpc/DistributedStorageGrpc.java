package com.scalar.db.rpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.38.0)",
    comments = "Source: scalardb.proto")
public final class DistributedStorageGrpc {

  private DistributedStorageGrpc() {}

  public static final String SERVICE_NAME = "rpc.DistributedStorage";

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

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.OpenScannerRequest,
      com.scalar.db.rpc.OpenScannerResponse> getOpenScannerMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "OpenScanner",
      requestType = com.scalar.db.rpc.OpenScannerRequest.class,
      responseType = com.scalar.db.rpc.OpenScannerResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.OpenScannerRequest,
      com.scalar.db.rpc.OpenScannerResponse> getOpenScannerMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.OpenScannerRequest, com.scalar.db.rpc.OpenScannerResponse> getOpenScannerMethod;
    if ((getOpenScannerMethod = DistributedStorageGrpc.getOpenScannerMethod) == null) {
      synchronized (DistributedStorageGrpc.class) {
        if ((getOpenScannerMethod = DistributedStorageGrpc.getOpenScannerMethod) == null) {
          DistributedStorageGrpc.getOpenScannerMethod = getOpenScannerMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.OpenScannerRequest, com.scalar.db.rpc.OpenScannerResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "OpenScanner"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.OpenScannerRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.OpenScannerResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedStorageMethodDescriptorSupplier("OpenScanner"))
              .build();
        }
      }
    }
    return getOpenScannerMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.ScanNextRequest,
      com.scalar.db.rpc.ScanNextResponse> getScanNextMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ScanNext",
      requestType = com.scalar.db.rpc.ScanNextRequest.class,
      responseType = com.scalar.db.rpc.ScanNextResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.ScanNextRequest,
      com.scalar.db.rpc.ScanNextResponse> getScanNextMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.ScanNextRequest, com.scalar.db.rpc.ScanNextResponse> getScanNextMethod;
    if ((getScanNextMethod = DistributedStorageGrpc.getScanNextMethod) == null) {
      synchronized (DistributedStorageGrpc.class) {
        if ((getScanNextMethod = DistributedStorageGrpc.getScanNextMethod) == null) {
          DistributedStorageGrpc.getScanNextMethod = getScanNextMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.ScanNextRequest, com.scalar.db.rpc.ScanNextResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ScanNext"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.ScanNextRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.ScanNextResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedStorageMethodDescriptorSupplier("ScanNext"))
              .build();
        }
      }
    }
    return getScanNextMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.CloseScannerRequest,
      com.google.protobuf.Empty> getCloseScannerMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CloseScanner",
      requestType = com.scalar.db.rpc.CloseScannerRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.CloseScannerRequest,
      com.google.protobuf.Empty> getCloseScannerMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.CloseScannerRequest, com.google.protobuf.Empty> getCloseScannerMethod;
    if ((getCloseScannerMethod = DistributedStorageGrpc.getCloseScannerMethod) == null) {
      synchronized (DistributedStorageGrpc.class) {
        if ((getCloseScannerMethod = DistributedStorageGrpc.getCloseScannerMethod) == null) {
          DistributedStorageGrpc.getCloseScannerMethod = getCloseScannerMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.CloseScannerRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CloseScanner"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.CloseScannerRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedStorageMethodDescriptorSupplier("CloseScanner"))
              .build();
        }
      }
    }
    return getCloseScannerMethod;
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
  public static abstract class DistributedStorageImplBase implements io.grpc.BindableService {

    /**
     */
    public void get(com.scalar.db.rpc.GetRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.GetResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetMethod(), responseObserver);
    }

    /**
     */
    public void openScanner(com.scalar.db.rpc.OpenScannerRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.OpenScannerResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getOpenScannerMethod(), responseObserver);
    }

    /**
     */
    public void scanNext(com.scalar.db.rpc.ScanNextRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.ScanNextResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getScanNextMethod(), responseObserver);
    }

    /**
     */
    public void closeScanner(com.scalar.db.rpc.CloseScannerRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCloseScannerMethod(), responseObserver);
    }

    /**
     */
    public void mutate(com.scalar.db.rpc.MutateRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMutateMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.GetRequest,
                com.scalar.db.rpc.GetResponse>(
                  this, METHODID_GET)))
          .addMethod(
            getOpenScannerMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.OpenScannerRequest,
                com.scalar.db.rpc.OpenScannerResponse>(
                  this, METHODID_OPEN_SCANNER)))
          .addMethod(
            getScanNextMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.ScanNextRequest,
                com.scalar.db.rpc.ScanNextResponse>(
                  this, METHODID_SCAN_NEXT)))
          .addMethod(
            getCloseScannerMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.CloseScannerRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_CLOSE_SCANNER)))
          .addMethod(
            getMutateMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.MutateRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_MUTATE)))
          .build();
    }
  }

  /**
   */
  public static final class DistributedStorageStub extends io.grpc.stub.AbstractAsyncStub<DistributedStorageStub> {
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
    public void openScanner(com.scalar.db.rpc.OpenScannerRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.OpenScannerResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getOpenScannerMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void scanNext(com.scalar.db.rpc.ScanNextRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.ScanNextResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getScanNextMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void closeScanner(com.scalar.db.rpc.CloseScannerRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCloseScannerMethod(), getCallOptions()), request, responseObserver);
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
   */
  public static final class DistributedStorageBlockingStub extends io.grpc.stub.AbstractBlockingStub<DistributedStorageBlockingStub> {
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
    public com.scalar.db.rpc.OpenScannerResponse openScanner(com.scalar.db.rpc.OpenScannerRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getOpenScannerMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.scalar.db.rpc.ScanNextResponse scanNext(com.scalar.db.rpc.ScanNextRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getScanNextMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty closeScanner(com.scalar.db.rpc.CloseScannerRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCloseScannerMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty mutate(com.scalar.db.rpc.MutateRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getMutateMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class DistributedStorageFutureStub extends io.grpc.stub.AbstractFutureStub<DistributedStorageFutureStub> {
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
    public com.google.common.util.concurrent.ListenableFuture<com.scalar.db.rpc.OpenScannerResponse> openScanner(
        com.scalar.db.rpc.OpenScannerRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getOpenScannerMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.scalar.db.rpc.ScanNextResponse> scanNext(
        com.scalar.db.rpc.ScanNextRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getScanNextMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> closeScanner(
        com.scalar.db.rpc.CloseScannerRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCloseScannerMethod(), getCallOptions()), request);
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
  private static final int METHODID_OPEN_SCANNER = 1;
  private static final int METHODID_SCAN_NEXT = 2;
  private static final int METHODID_CLOSE_SCANNER = 3;
  private static final int METHODID_MUTATE = 4;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final DistributedStorageImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(DistributedStorageImplBase serviceImpl, int methodId) {
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
        case METHODID_OPEN_SCANNER:
          serviceImpl.openScanner((com.scalar.db.rpc.OpenScannerRequest) request,
              (io.grpc.stub.StreamObserver<com.scalar.db.rpc.OpenScannerResponse>) responseObserver);
          break;
        case METHODID_SCAN_NEXT:
          serviceImpl.scanNext((com.scalar.db.rpc.ScanNextRequest) request,
              (io.grpc.stub.StreamObserver<com.scalar.db.rpc.ScanNextResponse>) responseObserver);
          break;
        case METHODID_CLOSE_SCANNER:
          serviceImpl.closeScanner((com.scalar.db.rpc.CloseScannerRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
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
        default:
          throw new AssertionError();
      }
    }
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
    private final String methodName;

    DistributedStorageMethodDescriptorSupplier(String methodName) {
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
              .addMethod(getOpenScannerMethod())
              .addMethod(getScanNextMethod())
              .addMethod(getCloseScannerMethod())
              .addMethod(getMutateMethod())
              .build();
        }
      }
    }
    return result;
  }
}
