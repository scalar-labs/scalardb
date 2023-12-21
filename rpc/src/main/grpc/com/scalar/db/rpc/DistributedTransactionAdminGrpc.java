package com.scalar.db.rpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.60.0)",
    comments = "Source: scalardb.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class DistributedTransactionAdminGrpc {

  private DistributedTransactionAdminGrpc() {}

  public static final java.lang.String SERVICE_NAME = "rpc.DistributedTransactionAdmin";

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
    if ((getCreateNamespaceMethod = DistributedTransactionAdminGrpc.getCreateNamespaceMethod) == null) {
      synchronized (DistributedTransactionAdminGrpc.class) {
        if ((getCreateNamespaceMethod = DistributedTransactionAdminGrpc.getCreateNamespaceMethod) == null) {
          DistributedTransactionAdminGrpc.getCreateNamespaceMethod = getCreateNamespaceMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.CreateNamespaceRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateNamespace"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.CreateNamespaceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionAdminMethodDescriptorSupplier("CreateNamespace"))
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
    if ((getDropNamespaceMethod = DistributedTransactionAdminGrpc.getDropNamespaceMethod) == null) {
      synchronized (DistributedTransactionAdminGrpc.class) {
        if ((getDropNamespaceMethod = DistributedTransactionAdminGrpc.getDropNamespaceMethod) == null) {
          DistributedTransactionAdminGrpc.getDropNamespaceMethod = getDropNamespaceMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.DropNamespaceRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DropNamespace"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.DropNamespaceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionAdminMethodDescriptorSupplier("DropNamespace"))
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
    if ((getCreateTableMethod = DistributedTransactionAdminGrpc.getCreateTableMethod) == null) {
      synchronized (DistributedTransactionAdminGrpc.class) {
        if ((getCreateTableMethod = DistributedTransactionAdminGrpc.getCreateTableMethod) == null) {
          DistributedTransactionAdminGrpc.getCreateTableMethod = getCreateTableMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.CreateTableRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.CreateTableRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionAdminMethodDescriptorSupplier("CreateTable"))
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
    if ((getDropTableMethod = DistributedTransactionAdminGrpc.getDropTableMethod) == null) {
      synchronized (DistributedTransactionAdminGrpc.class) {
        if ((getDropTableMethod = DistributedTransactionAdminGrpc.getDropTableMethod) == null) {
          DistributedTransactionAdminGrpc.getDropTableMethod = getDropTableMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.DropTableRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DropTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.DropTableRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionAdminMethodDescriptorSupplier("DropTable"))
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
    if ((getTruncateTableMethod = DistributedTransactionAdminGrpc.getTruncateTableMethod) == null) {
      synchronized (DistributedTransactionAdminGrpc.class) {
        if ((getTruncateTableMethod = DistributedTransactionAdminGrpc.getTruncateTableMethod) == null) {
          DistributedTransactionAdminGrpc.getTruncateTableMethod = getTruncateTableMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.TruncateTableRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "TruncateTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.TruncateTableRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionAdminMethodDescriptorSupplier("TruncateTable"))
              .build();
        }
      }
    }
    return getTruncateTableMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.CreateIndexRequest,
      com.google.protobuf.Empty> getCreateIndexMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateIndex",
      requestType = com.scalar.db.rpc.CreateIndexRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.CreateIndexRequest,
      com.google.protobuf.Empty> getCreateIndexMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.CreateIndexRequest, com.google.protobuf.Empty> getCreateIndexMethod;
    if ((getCreateIndexMethod = DistributedTransactionAdminGrpc.getCreateIndexMethod) == null) {
      synchronized (DistributedTransactionAdminGrpc.class) {
        if ((getCreateIndexMethod = DistributedTransactionAdminGrpc.getCreateIndexMethod) == null) {
          DistributedTransactionAdminGrpc.getCreateIndexMethod = getCreateIndexMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.CreateIndexRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateIndex"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.CreateIndexRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionAdminMethodDescriptorSupplier("CreateIndex"))
              .build();
        }
      }
    }
    return getCreateIndexMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.DropIndexRequest,
      com.google.protobuf.Empty> getDropIndexMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DropIndex",
      requestType = com.scalar.db.rpc.DropIndexRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.DropIndexRequest,
      com.google.protobuf.Empty> getDropIndexMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.DropIndexRequest, com.google.protobuf.Empty> getDropIndexMethod;
    if ((getDropIndexMethod = DistributedTransactionAdminGrpc.getDropIndexMethod) == null) {
      synchronized (DistributedTransactionAdminGrpc.class) {
        if ((getDropIndexMethod = DistributedTransactionAdminGrpc.getDropIndexMethod) == null) {
          DistributedTransactionAdminGrpc.getDropIndexMethod = getDropIndexMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.DropIndexRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DropIndex"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.DropIndexRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionAdminMethodDescriptorSupplier("DropIndex"))
              .build();
        }
      }
    }
    return getDropIndexMethod;
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
    if ((getGetTableMetadataMethod = DistributedTransactionAdminGrpc.getGetTableMetadataMethod) == null) {
      synchronized (DistributedTransactionAdminGrpc.class) {
        if ((getGetTableMetadataMethod = DistributedTransactionAdminGrpc.getGetTableMetadataMethod) == null) {
          DistributedTransactionAdminGrpc.getGetTableMetadataMethod = getGetTableMetadataMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.GetTableMetadataRequest, com.scalar.db.rpc.GetTableMetadataResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetTableMetadata"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.GetTableMetadataRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.GetTableMetadataResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionAdminMethodDescriptorSupplier("GetTableMetadata"))
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
    if ((getGetNamespaceTableNamesMethod = DistributedTransactionAdminGrpc.getGetNamespaceTableNamesMethod) == null) {
      synchronized (DistributedTransactionAdminGrpc.class) {
        if ((getGetNamespaceTableNamesMethod = DistributedTransactionAdminGrpc.getGetNamespaceTableNamesMethod) == null) {
          DistributedTransactionAdminGrpc.getGetNamespaceTableNamesMethod = getGetNamespaceTableNamesMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.GetNamespaceTableNamesRequest, com.scalar.db.rpc.GetNamespaceTableNamesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetNamespaceTableNames"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.GetNamespaceTableNamesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.GetNamespaceTableNamesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionAdminMethodDescriptorSupplier("GetNamespaceTableNames"))
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
    if ((getNamespaceExistsMethod = DistributedTransactionAdminGrpc.getNamespaceExistsMethod) == null) {
      synchronized (DistributedTransactionAdminGrpc.class) {
        if ((getNamespaceExistsMethod = DistributedTransactionAdminGrpc.getNamespaceExistsMethod) == null) {
          DistributedTransactionAdminGrpc.getNamespaceExistsMethod = getNamespaceExistsMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.NamespaceExistsRequest, com.scalar.db.rpc.NamespaceExistsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "NamespaceExists"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.NamespaceExistsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.NamespaceExistsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionAdminMethodDescriptorSupplier("NamespaceExists"))
              .build();
        }
      }
    }
    return getNamespaceExistsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.CreateCoordinatorTablesRequest,
      com.google.protobuf.Empty> getCreateCoordinatorTablesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateCoordinatorTables",
      requestType = com.scalar.db.rpc.CreateCoordinatorTablesRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.CreateCoordinatorTablesRequest,
      com.google.protobuf.Empty> getCreateCoordinatorTablesMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.CreateCoordinatorTablesRequest, com.google.protobuf.Empty> getCreateCoordinatorTablesMethod;
    if ((getCreateCoordinatorTablesMethod = DistributedTransactionAdminGrpc.getCreateCoordinatorTablesMethod) == null) {
      synchronized (DistributedTransactionAdminGrpc.class) {
        if ((getCreateCoordinatorTablesMethod = DistributedTransactionAdminGrpc.getCreateCoordinatorTablesMethod) == null) {
          DistributedTransactionAdminGrpc.getCreateCoordinatorTablesMethod = getCreateCoordinatorTablesMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.CreateCoordinatorTablesRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateCoordinatorTables"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.CreateCoordinatorTablesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionAdminMethodDescriptorSupplier("CreateCoordinatorTables"))
              .build();
        }
      }
    }
    return getCreateCoordinatorTablesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.DropCoordinatorTablesRequest,
      com.google.protobuf.Empty> getDropCoordinatorTablesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DropCoordinatorTables",
      requestType = com.scalar.db.rpc.DropCoordinatorTablesRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.DropCoordinatorTablesRequest,
      com.google.protobuf.Empty> getDropCoordinatorTablesMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.DropCoordinatorTablesRequest, com.google.protobuf.Empty> getDropCoordinatorTablesMethod;
    if ((getDropCoordinatorTablesMethod = DistributedTransactionAdminGrpc.getDropCoordinatorTablesMethod) == null) {
      synchronized (DistributedTransactionAdminGrpc.class) {
        if ((getDropCoordinatorTablesMethod = DistributedTransactionAdminGrpc.getDropCoordinatorTablesMethod) == null) {
          DistributedTransactionAdminGrpc.getDropCoordinatorTablesMethod = getDropCoordinatorTablesMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.DropCoordinatorTablesRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DropCoordinatorTables"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.DropCoordinatorTablesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionAdminMethodDescriptorSupplier("DropCoordinatorTables"))
              .build();
        }
      }
    }
    return getDropCoordinatorTablesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.TruncateCoordinatorTablesRequest,
      com.google.protobuf.Empty> getTruncateCoordinatorTablesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "TruncateCoordinatorTables",
      requestType = com.scalar.db.rpc.TruncateCoordinatorTablesRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.TruncateCoordinatorTablesRequest,
      com.google.protobuf.Empty> getTruncateCoordinatorTablesMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.TruncateCoordinatorTablesRequest, com.google.protobuf.Empty> getTruncateCoordinatorTablesMethod;
    if ((getTruncateCoordinatorTablesMethod = DistributedTransactionAdminGrpc.getTruncateCoordinatorTablesMethod) == null) {
      synchronized (DistributedTransactionAdminGrpc.class) {
        if ((getTruncateCoordinatorTablesMethod = DistributedTransactionAdminGrpc.getTruncateCoordinatorTablesMethod) == null) {
          DistributedTransactionAdminGrpc.getTruncateCoordinatorTablesMethod = getTruncateCoordinatorTablesMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.TruncateCoordinatorTablesRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "TruncateCoordinatorTables"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.TruncateCoordinatorTablesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionAdminMethodDescriptorSupplier("TruncateCoordinatorTables"))
              .build();
        }
      }
    }
    return getTruncateCoordinatorTablesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.CoordinatorTablesExistRequest,
      com.scalar.db.rpc.CoordinatorTablesExistResponse> getCoordinatorTablesExistMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CoordinatorTablesExist",
      requestType = com.scalar.db.rpc.CoordinatorTablesExistRequest.class,
      responseType = com.scalar.db.rpc.CoordinatorTablesExistResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.CoordinatorTablesExistRequest,
      com.scalar.db.rpc.CoordinatorTablesExistResponse> getCoordinatorTablesExistMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.CoordinatorTablesExistRequest, com.scalar.db.rpc.CoordinatorTablesExistResponse> getCoordinatorTablesExistMethod;
    if ((getCoordinatorTablesExistMethod = DistributedTransactionAdminGrpc.getCoordinatorTablesExistMethod) == null) {
      synchronized (DistributedTransactionAdminGrpc.class) {
        if ((getCoordinatorTablesExistMethod = DistributedTransactionAdminGrpc.getCoordinatorTablesExistMethod) == null) {
          DistributedTransactionAdminGrpc.getCoordinatorTablesExistMethod = getCoordinatorTablesExistMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.CoordinatorTablesExistRequest, com.scalar.db.rpc.CoordinatorTablesExistResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CoordinatorTablesExist"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.CoordinatorTablesExistRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.CoordinatorTablesExistResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionAdminMethodDescriptorSupplier("CoordinatorTablesExist"))
              .build();
        }
      }
    }
    return getCoordinatorTablesExistMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.RepairTableRequest,
      com.google.protobuf.Empty> getRepairTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RepairTable",
      requestType = com.scalar.db.rpc.RepairTableRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.RepairTableRequest,
      com.google.protobuf.Empty> getRepairTableMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.RepairTableRequest, com.google.protobuf.Empty> getRepairTableMethod;
    if ((getRepairTableMethod = DistributedTransactionAdminGrpc.getRepairTableMethod) == null) {
      synchronized (DistributedTransactionAdminGrpc.class) {
        if ((getRepairTableMethod = DistributedTransactionAdminGrpc.getRepairTableMethod) == null) {
          DistributedTransactionAdminGrpc.getRepairTableMethod = getRepairTableMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.RepairTableRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RepairTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.RepairTableRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionAdminMethodDescriptorSupplier("RepairTable"))
              .build();
        }
      }
    }
    return getRepairTableMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.scalar.db.rpc.RepairCoordinatorTablesRequest,
      com.google.protobuf.Empty> getRepairCoordinatorTablesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RepairCoordinatorTables",
      requestType = com.scalar.db.rpc.RepairCoordinatorTablesRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.scalar.db.rpc.RepairCoordinatorTablesRequest,
      com.google.protobuf.Empty> getRepairCoordinatorTablesMethod() {
    io.grpc.MethodDescriptor<com.scalar.db.rpc.RepairCoordinatorTablesRequest, com.google.protobuf.Empty> getRepairCoordinatorTablesMethod;
    if ((getRepairCoordinatorTablesMethod = DistributedTransactionAdminGrpc.getRepairCoordinatorTablesMethod) == null) {
      synchronized (DistributedTransactionAdminGrpc.class) {
        if ((getRepairCoordinatorTablesMethod = DistributedTransactionAdminGrpc.getRepairCoordinatorTablesMethod) == null) {
          DistributedTransactionAdminGrpc.getRepairCoordinatorTablesMethod = getRepairCoordinatorTablesMethod =
              io.grpc.MethodDescriptor.<com.scalar.db.rpc.RepairCoordinatorTablesRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RepairCoordinatorTables"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.scalar.db.rpc.RepairCoordinatorTablesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new DistributedTransactionAdminMethodDescriptorSupplier("RepairCoordinatorTables"))
              .build();
        }
      }
    }
    return getRepairCoordinatorTablesMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DistributedTransactionAdminStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DistributedTransactionAdminStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DistributedTransactionAdminStub>() {
        @java.lang.Override
        public DistributedTransactionAdminStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DistributedTransactionAdminStub(channel, callOptions);
        }
      };
    return DistributedTransactionAdminStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DistributedTransactionAdminBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DistributedTransactionAdminBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DistributedTransactionAdminBlockingStub>() {
        @java.lang.Override
        public DistributedTransactionAdminBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DistributedTransactionAdminBlockingStub(channel, callOptions);
        }
      };
    return DistributedTransactionAdminBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static DistributedTransactionAdminFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DistributedTransactionAdminFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DistributedTransactionAdminFutureStub>() {
        @java.lang.Override
        public DistributedTransactionAdminFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DistributedTransactionAdminFutureStub(channel, callOptions);
        }
      };
    return DistributedTransactionAdminFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void createNamespace(com.scalar.db.rpc.CreateNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateNamespaceMethod(), responseObserver);
    }

    /**
     */
    default void dropNamespace(com.scalar.db.rpc.DropNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDropNamespaceMethod(), responseObserver);
    }

    /**
     */
    default void createTable(com.scalar.db.rpc.CreateTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateTableMethod(), responseObserver);
    }

    /**
     */
    default void dropTable(com.scalar.db.rpc.DropTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDropTableMethod(), responseObserver);
    }

    /**
     */
    default void truncateTable(com.scalar.db.rpc.TruncateTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getTruncateTableMethod(), responseObserver);
    }

    /**
     */
    default void createIndex(com.scalar.db.rpc.CreateIndexRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateIndexMethod(), responseObserver);
    }

    /**
     */
    default void dropIndex(com.scalar.db.rpc.DropIndexRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDropIndexMethod(), responseObserver);
    }

    /**
     */
    default void getTableMetadata(com.scalar.db.rpc.GetTableMetadataRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.GetTableMetadataResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetTableMetadataMethod(), responseObserver);
    }

    /**
     */
    default void getNamespaceTableNames(com.scalar.db.rpc.GetNamespaceTableNamesRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.GetNamespaceTableNamesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetNamespaceTableNamesMethod(), responseObserver);
    }

    /**
     */
    default void namespaceExists(com.scalar.db.rpc.NamespaceExistsRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.NamespaceExistsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getNamespaceExistsMethod(), responseObserver);
    }

    /**
     */
    default void createCoordinatorTables(com.scalar.db.rpc.CreateCoordinatorTablesRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateCoordinatorTablesMethod(), responseObserver);
    }

    /**
     */
    default void dropCoordinatorTables(com.scalar.db.rpc.DropCoordinatorTablesRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDropCoordinatorTablesMethod(), responseObserver);
    }

    /**
     */
    default void truncateCoordinatorTables(com.scalar.db.rpc.TruncateCoordinatorTablesRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getTruncateCoordinatorTablesMethod(), responseObserver);
    }

    /**
     */
    default void coordinatorTablesExist(com.scalar.db.rpc.CoordinatorTablesExistRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.CoordinatorTablesExistResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCoordinatorTablesExistMethod(), responseObserver);
    }

    /**
     */
    default void repairTable(com.scalar.db.rpc.RepairTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRepairTableMethod(), responseObserver);
    }

    /**
     */
    default void repairCoordinatorTables(com.scalar.db.rpc.RepairCoordinatorTablesRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRepairCoordinatorTablesMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service DistributedTransactionAdmin.
   */
  public static abstract class DistributedTransactionAdminImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return DistributedTransactionAdminGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service DistributedTransactionAdmin.
   */
  public static final class DistributedTransactionAdminStub
      extends io.grpc.stub.AbstractAsyncStub<DistributedTransactionAdminStub> {
    private DistributedTransactionAdminStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DistributedTransactionAdminStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DistributedTransactionAdminStub(channel, callOptions);
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
    public void createIndex(com.scalar.db.rpc.CreateIndexRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateIndexMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void dropIndex(com.scalar.db.rpc.DropIndexRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDropIndexMethod(), getCallOptions()), request, responseObserver);
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

    /**
     */
    public void createCoordinatorTables(com.scalar.db.rpc.CreateCoordinatorTablesRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateCoordinatorTablesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void dropCoordinatorTables(com.scalar.db.rpc.DropCoordinatorTablesRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDropCoordinatorTablesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void truncateCoordinatorTables(com.scalar.db.rpc.TruncateCoordinatorTablesRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getTruncateCoordinatorTablesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void coordinatorTablesExist(com.scalar.db.rpc.CoordinatorTablesExistRequest request,
        io.grpc.stub.StreamObserver<com.scalar.db.rpc.CoordinatorTablesExistResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCoordinatorTablesExistMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void repairTable(com.scalar.db.rpc.RepairTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRepairTableMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void repairCoordinatorTables(com.scalar.db.rpc.RepairCoordinatorTablesRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRepairCoordinatorTablesMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service DistributedTransactionAdmin.
   */
  public static final class DistributedTransactionAdminBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<DistributedTransactionAdminBlockingStub> {
    private DistributedTransactionAdminBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DistributedTransactionAdminBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DistributedTransactionAdminBlockingStub(channel, callOptions);
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
    public com.google.protobuf.Empty createIndex(com.scalar.db.rpc.CreateIndexRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateIndexMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty dropIndex(com.scalar.db.rpc.DropIndexRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDropIndexMethod(), getCallOptions(), request);
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

    /**
     */
    public com.google.protobuf.Empty createCoordinatorTables(com.scalar.db.rpc.CreateCoordinatorTablesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateCoordinatorTablesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty dropCoordinatorTables(com.scalar.db.rpc.DropCoordinatorTablesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDropCoordinatorTablesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty truncateCoordinatorTables(com.scalar.db.rpc.TruncateCoordinatorTablesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getTruncateCoordinatorTablesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.scalar.db.rpc.CoordinatorTablesExistResponse coordinatorTablesExist(com.scalar.db.rpc.CoordinatorTablesExistRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCoordinatorTablesExistMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty repairTable(com.scalar.db.rpc.RepairTableRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRepairTableMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty repairCoordinatorTables(com.scalar.db.rpc.RepairCoordinatorTablesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRepairCoordinatorTablesMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service DistributedTransactionAdmin.
   */
  public static final class DistributedTransactionAdminFutureStub
      extends io.grpc.stub.AbstractFutureStub<DistributedTransactionAdminFutureStub> {
    private DistributedTransactionAdminFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DistributedTransactionAdminFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DistributedTransactionAdminFutureStub(channel, callOptions);
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
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> createIndex(
        com.scalar.db.rpc.CreateIndexRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateIndexMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> dropIndex(
        com.scalar.db.rpc.DropIndexRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDropIndexMethod(), getCallOptions()), request);
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

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> createCoordinatorTables(
        com.scalar.db.rpc.CreateCoordinatorTablesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateCoordinatorTablesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> dropCoordinatorTables(
        com.scalar.db.rpc.DropCoordinatorTablesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDropCoordinatorTablesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> truncateCoordinatorTables(
        com.scalar.db.rpc.TruncateCoordinatorTablesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getTruncateCoordinatorTablesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.scalar.db.rpc.CoordinatorTablesExistResponse> coordinatorTablesExist(
        com.scalar.db.rpc.CoordinatorTablesExistRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCoordinatorTablesExistMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> repairTable(
        com.scalar.db.rpc.RepairTableRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRepairTableMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> repairCoordinatorTables(
        com.scalar.db.rpc.RepairCoordinatorTablesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRepairCoordinatorTablesMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_NAMESPACE = 0;
  private static final int METHODID_DROP_NAMESPACE = 1;
  private static final int METHODID_CREATE_TABLE = 2;
  private static final int METHODID_DROP_TABLE = 3;
  private static final int METHODID_TRUNCATE_TABLE = 4;
  private static final int METHODID_CREATE_INDEX = 5;
  private static final int METHODID_DROP_INDEX = 6;
  private static final int METHODID_GET_TABLE_METADATA = 7;
  private static final int METHODID_GET_NAMESPACE_TABLE_NAMES = 8;
  private static final int METHODID_NAMESPACE_EXISTS = 9;
  private static final int METHODID_CREATE_COORDINATOR_TABLES = 10;
  private static final int METHODID_DROP_COORDINATOR_TABLES = 11;
  private static final int METHODID_TRUNCATE_COORDINATOR_TABLES = 12;
  private static final int METHODID_COORDINATOR_TABLES_EXIST = 13;
  private static final int METHODID_REPAIR_TABLE = 14;
  private static final int METHODID_REPAIR_COORDINATOR_TABLES = 15;

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
        case METHODID_CREATE_INDEX:
          serviceImpl.createIndex((com.scalar.db.rpc.CreateIndexRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_DROP_INDEX:
          serviceImpl.dropIndex((com.scalar.db.rpc.DropIndexRequest) request,
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
        case METHODID_CREATE_COORDINATOR_TABLES:
          serviceImpl.createCoordinatorTables((com.scalar.db.rpc.CreateCoordinatorTablesRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_DROP_COORDINATOR_TABLES:
          serviceImpl.dropCoordinatorTables((com.scalar.db.rpc.DropCoordinatorTablesRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_TRUNCATE_COORDINATOR_TABLES:
          serviceImpl.truncateCoordinatorTables((com.scalar.db.rpc.TruncateCoordinatorTablesRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_COORDINATOR_TABLES_EXIST:
          serviceImpl.coordinatorTablesExist((com.scalar.db.rpc.CoordinatorTablesExistRequest) request,
              (io.grpc.stub.StreamObserver<com.scalar.db.rpc.CoordinatorTablesExistResponse>) responseObserver);
          break;
        case METHODID_REPAIR_TABLE:
          serviceImpl.repairTable((com.scalar.db.rpc.RepairTableRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_REPAIR_COORDINATOR_TABLES:
          serviceImpl.repairCoordinatorTables((com.scalar.db.rpc.RepairCoordinatorTablesRequest) request,
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

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getCreateNamespaceMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.scalar.db.rpc.CreateNamespaceRequest,
              com.google.protobuf.Empty>(
                service, METHODID_CREATE_NAMESPACE)))
        .addMethod(
          getDropNamespaceMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.scalar.db.rpc.DropNamespaceRequest,
              com.google.protobuf.Empty>(
                service, METHODID_DROP_NAMESPACE)))
        .addMethod(
          getCreateTableMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.scalar.db.rpc.CreateTableRequest,
              com.google.protobuf.Empty>(
                service, METHODID_CREATE_TABLE)))
        .addMethod(
          getDropTableMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.scalar.db.rpc.DropTableRequest,
              com.google.protobuf.Empty>(
                service, METHODID_DROP_TABLE)))
        .addMethod(
          getTruncateTableMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.scalar.db.rpc.TruncateTableRequest,
              com.google.protobuf.Empty>(
                service, METHODID_TRUNCATE_TABLE)))
        .addMethod(
          getCreateIndexMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.scalar.db.rpc.CreateIndexRequest,
              com.google.protobuf.Empty>(
                service, METHODID_CREATE_INDEX)))
        .addMethod(
          getDropIndexMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.scalar.db.rpc.DropIndexRequest,
              com.google.protobuf.Empty>(
                service, METHODID_DROP_INDEX)))
        .addMethod(
          getGetTableMetadataMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.scalar.db.rpc.GetTableMetadataRequest,
              com.scalar.db.rpc.GetTableMetadataResponse>(
                service, METHODID_GET_TABLE_METADATA)))
        .addMethod(
          getGetNamespaceTableNamesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.scalar.db.rpc.GetNamespaceTableNamesRequest,
              com.scalar.db.rpc.GetNamespaceTableNamesResponse>(
                service, METHODID_GET_NAMESPACE_TABLE_NAMES)))
        .addMethod(
          getNamespaceExistsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.scalar.db.rpc.NamespaceExistsRequest,
              com.scalar.db.rpc.NamespaceExistsResponse>(
                service, METHODID_NAMESPACE_EXISTS)))
        .addMethod(
          getCreateCoordinatorTablesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.scalar.db.rpc.CreateCoordinatorTablesRequest,
              com.google.protobuf.Empty>(
                service, METHODID_CREATE_COORDINATOR_TABLES)))
        .addMethod(
          getDropCoordinatorTablesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.scalar.db.rpc.DropCoordinatorTablesRequest,
              com.google.protobuf.Empty>(
                service, METHODID_DROP_COORDINATOR_TABLES)))
        .addMethod(
          getTruncateCoordinatorTablesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.scalar.db.rpc.TruncateCoordinatorTablesRequest,
              com.google.protobuf.Empty>(
                service, METHODID_TRUNCATE_COORDINATOR_TABLES)))
        .addMethod(
          getCoordinatorTablesExistMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.scalar.db.rpc.CoordinatorTablesExistRequest,
              com.scalar.db.rpc.CoordinatorTablesExistResponse>(
                service, METHODID_COORDINATOR_TABLES_EXIST)))
        .addMethod(
          getRepairTableMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.scalar.db.rpc.RepairTableRequest,
              com.google.protobuf.Empty>(
                service, METHODID_REPAIR_TABLE)))
        .addMethod(
          getRepairCoordinatorTablesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.scalar.db.rpc.RepairCoordinatorTablesRequest,
              com.google.protobuf.Empty>(
                service, METHODID_REPAIR_COORDINATOR_TABLES)))
        .build();
  }

  private static abstract class DistributedTransactionAdminBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    DistributedTransactionAdminBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.scalar.db.rpc.ScalarDbProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("DistributedTransactionAdmin");
    }
  }

  private static final class DistributedTransactionAdminFileDescriptorSupplier
      extends DistributedTransactionAdminBaseDescriptorSupplier {
    DistributedTransactionAdminFileDescriptorSupplier() {}
  }

  private static final class DistributedTransactionAdminMethodDescriptorSupplier
      extends DistributedTransactionAdminBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    DistributedTransactionAdminMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (DistributedTransactionAdminGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new DistributedTransactionAdminFileDescriptorSupplier())
              .addMethod(getCreateNamespaceMethod())
              .addMethod(getDropNamespaceMethod())
              .addMethod(getCreateTableMethod())
              .addMethod(getDropTableMethod())
              .addMethod(getTruncateTableMethod())
              .addMethod(getCreateIndexMethod())
              .addMethod(getDropIndexMethod())
              .addMethod(getGetTableMetadataMethod())
              .addMethod(getGetNamespaceTableNamesMethod())
              .addMethod(getNamespaceExistsMethod())
              .addMethod(getCreateCoordinatorTablesMethod())
              .addMethod(getDropCoordinatorTablesMethod())
              .addMethod(getTruncateCoordinatorTablesMethod())
              .addMethod(getCoordinatorTablesExistMethod())
              .addMethod(getRepairTableMethod())
              .addMethod(getRepairCoordinatorTablesMethod())
              .build();
        }
      }
    }
    return result;
  }
}
