package com.scalar.db.rpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.38.0)",
    comments = "Source: scalardb.proto")
public final class DistributedStorageAdminGrpc {

  private DistributedStorageAdminGrpc() {}

  public static final String SERVICE_NAME = "rpc.DistributedStorageAdmin";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.CreateNamespaceRequest,
      com.google.protobuf.Empty> getCreateNamespaceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateNamespace",
      requestType = com.scalar.db.rpc.CreateNamespaceRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.CreateNamespaceRequest,
      com.google.protobuf.Empty> getCreateNamespaceMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.CreateNamespaceRequest, com.google.protobuf.Empty> getCreateNamespaceMethod;
    if ((getCreateNamespaceMethod = DistributedStorageAdminGrpc.getCreateNamespaceMethod) == null) {
      synchronized (DistributedStorageAdminGrpc.class) {
        if ((getCreateNamespaceMethod = DistributedStorageAdminGrpc.getCreateNamespaceMethod) == null) {
          DistributedStorageAdminGrpc.getCreateNamespaceMethod = getCreateNamespaceMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.CreateNamespaceRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateNamespace"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.CreateNamespaceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedStorageAdminMethodDescriptorSupplier("CreateNamespace"))
              .build();
        }
      }
    }
    return getCreateNamespaceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.DropNamespaceRequest,
      com.google.protobuf.Empty> getDropNamespaceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DropNamespace",
      requestType = com.scalar.db.rpc.DropNamespaceRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.DropNamespaceRequest,
      com.google.protobuf.Empty> getDropNamespaceMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.DropNamespaceRequest, com.google.protobuf.Empty> getDropNamespaceMethod;
    if ((getDropNamespaceMethod = DistributedStorageAdminGrpc.getDropNamespaceMethod) == null) {
      synchronized (DistributedStorageAdminGrpc.class) {
        if ((getDropNamespaceMethod = DistributedStorageAdminGrpc.getDropNamespaceMethod) == null) {
          DistributedStorageAdminGrpc.getDropNamespaceMethod = getDropNamespaceMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.DropNamespaceRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DropNamespace"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.DropNamespaceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedStorageAdminMethodDescriptorSupplier("DropNamespace"))
              .build();
        }
      }
    }
    return getDropNamespaceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.CreateTableRequest,
      com.google.protobuf.Empty> getCreateTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateTable",
      requestType = com.scalar.db.rpc.CreateTableRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.CreateTableRequest,
      com.google.protobuf.Empty> getCreateTableMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.CreateTableRequest, com.google.protobuf.Empty> getCreateTableMethod;
    if ((getCreateTableMethod = DistributedStorageAdminGrpc.getCreateTableMethod) == null) {
      synchronized (DistributedStorageAdminGrpc.class) {
        if ((getCreateTableMethod = DistributedStorageAdminGrpc.getCreateTableMethod) == null) {
          DistributedStorageAdminGrpc.getCreateTableMethod = getCreateTableMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.CreateTableRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.CreateTableRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedStorageAdminMethodDescriptorSupplier("CreateTable"))
              .build();
        }
      }
    }
    return getCreateTableMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.DropTableRequest,
      com.google.protobuf.Empty> getDropTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DropTable",
      requestType = com.scalar.db.rpc.DropTableRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.DropTableRequest,
      com.google.protobuf.Empty> getDropTableMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.DropTableRequest, com.google.protobuf.Empty> getDropTableMethod;
    if ((getDropTableMethod = DistributedStorageAdminGrpc.getDropTableMethod) == null) {
      synchronized (DistributedStorageAdminGrpc.class) {
        if ((getDropTableMethod = DistributedStorageAdminGrpc.getDropTableMethod) == null) {
          DistributedStorageAdminGrpc.getDropTableMethod = getDropTableMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.DropTableRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DropTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.DropTableRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedStorageAdminMethodDescriptorSupplier("DropTable"))
              .build();
        }
      }
    }
    return getDropTableMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.TruncateTableRequest,
      com.google.protobuf.Empty> getTruncateTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "TruncateTable",
      requestType = com.scalar.db.rpc.TruncateTableRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.TruncateTableRequest,
      com.google.protobuf.Empty> getTruncateTableMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.TruncateTableRequest, com.google.protobuf.Empty> getTruncateTableMethod;
    if ((getTruncateTableMethod = DistributedStorageAdminGrpc.getTruncateTableMethod) == null) {
      synchronized (DistributedStorageAdminGrpc.class) {
        if ((getTruncateTableMethod = DistributedStorageAdminGrpc.getTruncateTableMethod) == null) {
          DistributedStorageAdminGrpc.getTruncateTableMethod = getTruncateTableMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.TruncateTableRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "TruncateTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.TruncateTableRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedStorageAdminMethodDescriptorSupplier("TruncateTable"))
              .build();
        }
      }
    }
    return getTruncateTableMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.GetTableMetadataRequest,
      com.scalar.db.rpc.GetTableMetadataResponse> getGetTableMetadataMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetTableMetadata",
      requestType = com.scalar.db.rpc.GetTableMetadataRequest.class,
      responseType = com.scalar.db.rpc.GetTableMetadataResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.GetTableMetadataRequest,
      com.scalar.db.rpc.GetTableMetadataResponse> getGetTableMetadataMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.GetTableMetadataRequest, com.scalar.db.rpc.GetTableMetadataResponse> getGetTableMetadataMethod;
    if ((getGetTableMetadataMethod = DistributedStorageAdminGrpc.getGetTableMetadataMethod) == null) {
      synchronized (DistributedStorageAdminGrpc.class) {
        if ((getGetTableMetadataMethod = DistributedStorageAdminGrpc.getGetTableMetadataMethod) == null) {
          DistributedStorageAdminGrpc.getGetTableMetadataMethod = getGetTableMetadataMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.GetTableMetadataRequest, com.scalar.db.rpc.GetTableMetadataResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetTableMetadata"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.GetTableMetadataRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.GetTableMetadataResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedStorageAdminMethodDescriptorSupplier("GetTableMetadata"))
              .build();
        }
      }
    }
    return getGetTableMetadataMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.GetNamespaceTableNamesRequest,
      com.scalar.db.rpc.GetNamespaceTableNamesResponse> getGetNamespaceTableNamesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetNamespaceTableNames",
      requestType = com.scalar.db.rpc.GetNamespaceTableNamesRequest.class,
      responseType = com.scalar.db.rpc.GetNamespaceTableNamesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.GetNamespaceTableNamesRequest,
      com.scalar.db.rpc.GetNamespaceTableNamesResponse> getGetNamespaceTableNamesMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.GetNamespaceTableNamesRequest, com.scalar.db.rpc.GetNamespaceTableNamesResponse> getGetNamespaceTableNamesMethod;
    if ((getGetNamespaceTableNamesMethod = DistributedStorageAdminGrpc.getGetNamespaceTableNamesMethod) == null) {
      synchronized (DistributedStorageAdminGrpc.class) {
        if ((getGetNamespaceTableNamesMethod = DistributedStorageAdminGrpc.getGetNamespaceTableNamesMethod) == null) {
          DistributedStorageAdminGrpc.getGetNamespaceTableNamesMethod = getGetNamespaceTableNamesMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.GetNamespaceTableNamesRequest, com.scalar.db.rpc.GetNamespaceTableNamesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetNamespaceTableNames"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.GetNamespaceTableNamesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.GetNamespaceTableNamesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedStorageAdminMethodDescriptorSupplier("GetNamespaceTableNames"))
              .build();
        }
      }
    }
    return getGetNamespaceTableNamesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.NamespaceExistsRequest,
      com.scalar.db.rpc.NamespaceExistsResponse> getNamespaceExistsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "NamespaceExists",
      requestType = com.scalar.db.rpc.NamespaceExistsRequest.class,
      responseType = com.scalar.db.rpc.NamespaceExistsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.NamespaceExistsRequest,
      com.scalar.db.rpc.NamespaceExistsResponse> getNamespaceExistsMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.NamespaceExistsRequest, com.scalar.db.rpc.NamespaceExistsResponse> getNamespaceExistsMethod;
    if ((getNamespaceExistsMethod = DistributedStorageAdminGrpc.getNamespaceExistsMethod) == null) {
      synchronized (DistributedStorageAdminGrpc.class) {
        if ((getNamespaceExistsMethod = DistributedStorageAdminGrpc.getNamespaceExistsMethod) == null) {
          DistributedStorageAdminGrpc.getNamespaceExistsMethod = getNamespaceExistsMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.NamespaceExistsRequest, com.scalar.db.rpc.NamespaceExistsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "NamespaceExists"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.NamespaceExistsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.NamespaceExistsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedStorageAdminMethodDescriptorSupplier("NamespaceExists"))
              .build();
        }
      }
    }
    return getNamespaceExistsMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DistributedStorageAdminStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DistributedStorageAdminStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DistributedStorageAdminStub>() {
        @java.lang.Override
        public DistributedStorageAdminStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DistributedStorageAdminStub(channel, callOptions);
        }
      };
    return DistributedStorageAdminStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DistributedStorageAdminBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DistributedStorageAdminBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DistributedStorageAdminBlockingStub>() {
        @java.lang.Override
        public DistributedStorageAdminBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DistributedStorageAdminBlockingStub(channel, callOptions);
        }
      };
    return DistributedStorageAdminBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static DistributedStorageAdminFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DistributedStorageAdminFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DistributedStorageAdminFutureStub>() {
        @java.lang.Override
        public DistributedStorageAdminFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DistributedStorageAdminFutureStub(channel, callOptions);
        }
      };
    return DistributedStorageAdminFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class DistributedStorageAdminImplBase implements io.grpc.BindableService {

    /**
     */
    public void createNamespace(com.scalar.db.rpc.CreateNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateNamespaceMethod(), responseObserver);
    }

    /**
     */
    public void dropNamespace(com.scalar.db.rpc.DropNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDropNamespaceMethod(), responseObserver);
    }

    /**
     */
    public void createTable(com.scalar.db.rpc.CreateTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateTableMethod(), responseObserver);
    }

    /**
     */
    public void dropTable(com.scalar.db.rpc.DropTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDropTableMethod(), responseObserver);
    }

    /**
     */
    public void truncateTable(com.scalar.db.rpc.TruncateTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getTruncateTableMethod(), responseObserver);
    }

    /**
     */
    public void getTableMetadata(com.scalar.db.rpc.GetTableMetadataRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.GetTableMetadataResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetTableMetadataMethod(), responseObserver);
    }

    /**
     */
    public void getNamespaceTableNames(com.scalar.db.rpc.GetNamespaceTableNamesRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.GetNamespaceTableNamesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetNamespaceTableNamesMethod(), responseObserver);
    }

    /**
     */
    public void namespaceExists(com.scalar.db.rpc.NamespaceExistsRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.NamespaceExistsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getNamespaceExistsMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getCreateNamespaceMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.CreateNamespaceRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_CREATE_NAMESPACE)))
          .addMethod(
            getDropNamespaceMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.DropNamespaceRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_DROP_NAMESPACE)))
          .addMethod(
            getCreateTableMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.CreateTableRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_CREATE_TABLE)))
          .addMethod(
            getDropTableMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.DropTableRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_DROP_TABLE)))
          .addMethod(
            getTruncateTableMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.TruncateTableRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_TRUNCATE_TABLE)))
          .addMethod(
            getGetTableMetadataMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.GetTableMetadataRequest,
                com.scalar.db.rpc.GetTableMetadataResponse>(
                  this, METHODID_GET_TABLE_METADATA)))
          .addMethod(
            getGetNamespaceTableNamesMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.GetNamespaceTableNamesRequest,
                com.scalar.db.rpc.GetNamespaceTableNamesResponse>(
                  this, METHODID_GET_NAMESPACE_TABLE_NAMES)))
          .addMethod(
            getNamespaceExistsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.scalar.db.rpc.NamespaceExistsRequest,
                com.scalar.db.rpc.NamespaceExistsResponse>(
                  this, METHODID_NAMESPACE_EXISTS)))
          .build();
    }
  }

  /**
   */
  public static final class DistributedStorageAdminStub extends io.grpc.stub.AbstractAsyncStub<DistributedStorageAdminStub> {
    private DistributedStorageAdminStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DistributedStorageAdminStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DistributedStorageAdminStub(channel, callOptions);
    }

    /**
     */
    public void createNamespace(com.scalar.db.rpc.CreateNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateNamespaceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void dropNamespace(com.scalar.db.rpc.DropNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDropNamespaceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createTable(com.scalar.db.rpc.CreateTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateTableMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void dropTable(com.scalar.db.rpc.DropTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDropTableMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void truncateTable(com.scalar.db.rpc.TruncateTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getTruncateTableMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getTableMetadata(com.scalar.db.rpc.GetTableMetadataRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.GetTableMetadataResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetTableMetadataMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getNamespaceTableNames(com.scalar.db.rpc.GetNamespaceTableNamesRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.GetNamespaceTableNamesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetNamespaceTableNamesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void namespaceExists(com.scalar.db.rpc.NamespaceExistsRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.NamespaceExistsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getNamespaceExistsMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class DistributedStorageAdminBlockingStub extends io.grpc.stub.AbstractBlockingStub<DistributedStorageAdminBlockingStub> {
    private DistributedStorageAdminBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DistributedStorageAdminBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DistributedStorageAdminBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.google.protobuf.Empty createNamespace(com.scalar.db.rpc.CreateNamespaceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateNamespaceMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty dropNamespace(com.scalar.db.rpc.DropNamespaceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDropNamespaceMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty createTable(com.scalar.db.rpc.CreateTableRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateTableMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty dropTable(com.scalar.db.rpc.DropTableRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDropTableMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty truncateTable(com.scalar.db.rpc.TruncateTableRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getTruncateTableMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.scalar.db.rpc.GetTableMetadataResponse getTableMetadata(com.scalar.db.rpc.GetTableMetadataRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetTableMetadataMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.scalar.db.rpc.GetNamespaceTableNamesResponse getNamespaceTableNames(com.scalar.db.rpc.GetNamespaceTableNamesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetNamespaceTableNamesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.scalar.db.rpc.NamespaceExistsResponse namespaceExists(com.scalar.db.rpc.NamespaceExistsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getNamespaceExistsMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class DistributedStorageAdminFutureStub extends io.grpc.stub.AbstractFutureStub<DistributedStorageAdminFutureStub> {
    private DistributedStorageAdminFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DistributedStorageAdminFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DistributedStorageAdminFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> createNamespace(
        com.scalar.db.rpc.CreateNamespaceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateNamespaceMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> dropNamespace(
        com.scalar.db.rpc.DropNamespaceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDropNamespaceMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> createTable(
        com.scalar.db.rpc.CreateTableRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateTableMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> dropTable(
        com.scalar.db.rpc.DropTableRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDropTableMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> truncateTable(
        com.scalar.db.rpc.TruncateTableRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getTruncateTableMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.scalar.db.rpc.GetTableMetadataResponse> getTableMetadata(
        com.scalar.db.rpc.GetTableMetadataRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetTableMetadataMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.scalar.db.rpc.GetNamespaceTableNamesResponse> getNamespaceTableNames(
        com.scalar.db.rpc.GetNamespaceTableNamesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetNamespaceTableNamesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.scalar.db.rpc.NamespaceExistsResponse> namespaceExists(
        com.scalar.db.rpc.NamespaceExistsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getNamespaceExistsMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_NAMESPACE = 0;
  private static final int METHODID_DROP_NAMESPACE = 1;
  private static final int METHODID_CREATE_TABLE = 2;
  private static final int METHODID_DROP_TABLE = 3;
  private static final int METHODID_TRUNCATE_TABLE = 4;
  private static final int METHODID_GET_TABLE_METADATA = 5;
  private static final int METHODID_GET_NAMESPACE_TABLE_NAMES = 6;
  private static final int METHODID_NAMESPACE_EXISTS = 7;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final DistributedStorageAdminImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(DistributedStorageAdminImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CREATE_NAMESPACE:
          serviceImpl.createNamespace((com.scalar.db.rpc.CreateNamespaceRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_DROP_NAMESPACE:
          serviceImpl.dropNamespace((com.scalar.db.rpc.DropNamespaceRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_CREATE_TABLE:
          serviceImpl.createTable((com.scalar.db.rpc.CreateTableRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_DROP_TABLE:
          serviceImpl.dropTable((com.scalar.db.rpc.DropTableRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_TRUNCATE_TABLE:
          serviceImpl.truncateTable((com.scalar.db.rpc.TruncateTableRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_GET_TABLE_METADATA:
          serviceImpl.getTableMetadata((com.scalar.db.rpc.GetTableMetadataRequest) request,
              (io.grpc.stub.StreamObserver<com.scalar.db.rpc.GetTableMetadataResponse>) responseObserver);
          break;
        case METHODID_GET_NAMESPACE_TABLE_NAMES:
          serviceImpl.getNamespaceTableNames((com.scalar.db.rpc.GetNamespaceTableNamesRequest) request,
              (io.grpc.stub.StreamObserver<com.scalar.db.rpc.GetNamespaceTableNamesResponse>) responseObserver);
          break;
        case METHODID_NAMESPACE_EXISTS:
          serviceImpl.namespaceExists((com.scalar.db.rpc.NamespaceExistsRequest) request,
              (io.grpc.stub.StreamObserver<com.scalar.db.rpc.NamespaceExistsResponse>) responseObserver);
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

  private static abstract class DistributedStorageAdminBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    DistributedStorageAdminBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.scalar.db.rpc.ScalarDbProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("DistributedStorageAdmin");
    }
  }

  private static final class DistributedStorageAdminFileDescriptorSupplier
      extends DistributedStorageAdminBaseDescriptorSupplier {
    DistributedStorageAdminFileDescriptorSupplier() {}
  }

  private static final class DistributedStorageAdminMethodDescriptorSupplier
      extends DistributedStorageAdminBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    DistributedStorageAdminMethodDescriptorSupplier(String methodName) {
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
      synchronized (DistributedStorageAdminGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new DistributedStorageAdminFileDescriptorSupplier())
              .addMethod(getCreateNamespaceMethod())
              .addMethod(getDropNamespaceMethod())
              .addMethod(getCreateTableMethod())
              .addMethod(getDropTableMethod())
              .addMethod(getTruncateTableMethod())
              .addMethod(getGetTableMetadataMethod())
              .addMethod(getGetNamespaceTableNamesMethod())
              .addMethod(getNamespaceExistsMethod())
              .build();
        }
      }
    }
    return result;
  }
}
