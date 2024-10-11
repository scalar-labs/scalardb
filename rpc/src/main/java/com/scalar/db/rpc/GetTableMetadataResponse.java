// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: scalardb.proto

// Protobuf Java Version: 3.25.5
package com.scalar.db.rpc;

/**
 * Protobuf type {@code rpc.GetTableMetadataResponse}
 */
public final class GetTableMetadataResponse extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:rpc.GetTableMetadataResponse)
    GetTableMetadataResponseOrBuilder {
private static final long serialVersionUID = 0L;
  // Use GetTableMetadataResponse.newBuilder() to construct.
  private GetTableMetadataResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private GetTableMetadataResponse() {
  }

  @java.lang.Override
  @SuppressWarnings({"unused"})
  protected java.lang.Object newInstance(
      UnusedPrivateParameter unused) {
    return new GetTableMetadataResponse();
  }

  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return com.scalar.db.rpc.ScalarDbProto.internal_static_rpc_GetTableMetadataResponse_descriptor;
  }

  @java.lang.Override
  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return com.scalar.db.rpc.ScalarDbProto.internal_static_rpc_GetTableMetadataResponse_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            com.scalar.db.rpc.GetTableMetadataResponse.class, com.scalar.db.rpc.GetTableMetadataResponse.Builder.class);
  }

  private int bitField0_;
  public static final int TABLE_METADATA_FIELD_NUMBER = 1;
  private com.scalar.db.rpc.TableMetadata tableMetadata_;
  /**
   * <code>.rpc.TableMetadata table_metadata = 1;</code>
   * @return Whether the tableMetadata field is set.
   */
  @java.lang.Override
  public boolean hasTableMetadata() {
    return ((bitField0_ & 0x00000001) != 0);
  }
  /**
   * <code>.rpc.TableMetadata table_metadata = 1;</code>
   * @return The tableMetadata.
   */
  @java.lang.Override
  public com.scalar.db.rpc.TableMetadata getTableMetadata() {
    return tableMetadata_ == null ? com.scalar.db.rpc.TableMetadata.getDefaultInstance() : tableMetadata_;
  }
  /**
   * <code>.rpc.TableMetadata table_metadata = 1;</code>
   */
  @java.lang.Override
  public com.scalar.db.rpc.TableMetadataOrBuilder getTableMetadataOrBuilder() {
    return tableMetadata_ == null ? com.scalar.db.rpc.TableMetadata.getDefaultInstance() : tableMetadata_;
  }

  private byte memoizedIsInitialized = -1;
  @java.lang.Override
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  @java.lang.Override
  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (((bitField0_ & 0x00000001) != 0)) {
      output.writeMessage(1, getTableMetadata());
    }
    getUnknownFields().writeTo(output);
  }

  @java.lang.Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) != 0)) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(1, getTableMetadata());
    }
    size += getUnknownFields().getSerializedSize();
    memoizedSize = size;
    return size;
  }

  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof com.scalar.db.rpc.GetTableMetadataResponse)) {
      return super.equals(obj);
    }
    com.scalar.db.rpc.GetTableMetadataResponse other = (com.scalar.db.rpc.GetTableMetadataResponse) obj;

    if (hasTableMetadata() != other.hasTableMetadata()) return false;
    if (hasTableMetadata()) {
      if (!getTableMetadata()
          .equals(other.getTableMetadata())) return false;
    }
    if (!getUnknownFields().equals(other.getUnknownFields())) return false;
    return true;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    if (hasTableMetadata()) {
      hash = (37 * hash) + TABLE_METADATA_FIELD_NUMBER;
      hash = (53 * hash) + getTableMetadata().hashCode();
    }
    hash = (29 * hash) + getUnknownFields().hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static com.scalar.db.rpc.GetTableMetadataResponse parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.scalar.db.rpc.GetTableMetadataResponse parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.scalar.db.rpc.GetTableMetadataResponse parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.scalar.db.rpc.GetTableMetadataResponse parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.scalar.db.rpc.GetTableMetadataResponse parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.scalar.db.rpc.GetTableMetadataResponse parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.scalar.db.rpc.GetTableMetadataResponse parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static com.scalar.db.rpc.GetTableMetadataResponse parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  public static com.scalar.db.rpc.GetTableMetadataResponse parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }

  public static com.scalar.db.rpc.GetTableMetadataResponse parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static com.scalar.db.rpc.GetTableMetadataResponse parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static com.scalar.db.rpc.GetTableMetadataResponse parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  @java.lang.Override
  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(com.scalar.db.rpc.GetTableMetadataResponse prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  @java.lang.Override
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @java.lang.Override
  protected Builder newBuilderForType(
      com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * Protobuf type {@code rpc.GetTableMetadataResponse}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:rpc.GetTableMetadataResponse)
      com.scalar.db.rpc.GetTableMetadataResponseOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.scalar.db.rpc.ScalarDbProto.internal_static_rpc_GetTableMetadataResponse_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.scalar.db.rpc.ScalarDbProto.internal_static_rpc_GetTableMetadataResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.scalar.db.rpc.GetTableMetadataResponse.class, com.scalar.db.rpc.GetTableMetadataResponse.Builder.class);
    }

    // Construct using com.scalar.db.rpc.GetTableMetadataResponse.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (com.google.protobuf.GeneratedMessageV3
              .alwaysUseFieldBuilders) {
        getTableMetadataFieldBuilder();
      }
    }
    @java.lang.Override
    public Builder clear() {
      super.clear();
      bitField0_ = 0;
      tableMetadata_ = null;
      if (tableMetadataBuilder_ != null) {
        tableMetadataBuilder_.dispose();
        tableMetadataBuilder_ = null;
      }
      return this;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return com.scalar.db.rpc.ScalarDbProto.internal_static_rpc_GetTableMetadataResponse_descriptor;
    }

    @java.lang.Override
    public com.scalar.db.rpc.GetTableMetadataResponse getDefaultInstanceForType() {
      return com.scalar.db.rpc.GetTableMetadataResponse.getDefaultInstance();
    }

    @java.lang.Override
    public com.scalar.db.rpc.GetTableMetadataResponse build() {
      com.scalar.db.rpc.GetTableMetadataResponse result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @java.lang.Override
    public com.scalar.db.rpc.GetTableMetadataResponse buildPartial() {
      com.scalar.db.rpc.GetTableMetadataResponse result = new com.scalar.db.rpc.GetTableMetadataResponse(this);
      if (bitField0_ != 0) { buildPartial0(result); }
      onBuilt();
      return result;
    }

    private void buildPartial0(com.scalar.db.rpc.GetTableMetadataResponse result) {
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) != 0)) {
        result.tableMetadata_ = tableMetadataBuilder_ == null
            ? tableMetadata_
            : tableMetadataBuilder_.build();
        to_bitField0_ |= 0x00000001;
      }
      result.bitField0_ |= to_bitField0_;
    }

    @java.lang.Override
    public Builder clone() {
      return super.clone();
    }
    @java.lang.Override
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return super.setField(field, value);
    }
    @java.lang.Override
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return super.clearField(field);
    }
    @java.lang.Override
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return super.clearOneof(oneof);
    }
    @java.lang.Override
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, java.lang.Object value) {
      return super.setRepeatedField(field, index, value);
    }
    @java.lang.Override
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return super.addRepeatedField(field, value);
    }
    @java.lang.Override
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof com.scalar.db.rpc.GetTableMetadataResponse) {
        return mergeFrom((com.scalar.db.rpc.GetTableMetadataResponse)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(com.scalar.db.rpc.GetTableMetadataResponse other) {
      if (other == com.scalar.db.rpc.GetTableMetadataResponse.getDefaultInstance()) return this;
      if (other.hasTableMetadata()) {
        mergeTableMetadata(other.getTableMetadata());
      }
      this.mergeUnknownFields(other.getUnknownFields());
      onChanged();
      return this;
    }

    @java.lang.Override
    public final boolean isInitialized() {
      return true;
    }

    @java.lang.Override
    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      if (extensionRegistry == null) {
        throw new java.lang.NullPointerException();
      }
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            case 10: {
              input.readMessage(
                  getTableMetadataFieldBuilder().getBuilder(),
                  extensionRegistry);
              bitField0_ |= 0x00000001;
              break;
            } // case 10
            default: {
              if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                done = true; // was an endgroup tag
              }
              break;
            } // default:
          } // switch (tag)
        } // while (!done)
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.unwrapIOException();
      } finally {
        onChanged();
      } // finally
      return this;
    }
    private int bitField0_;

    private com.scalar.db.rpc.TableMetadata tableMetadata_;
    private com.google.protobuf.SingleFieldBuilderV3<
        com.scalar.db.rpc.TableMetadata, com.scalar.db.rpc.TableMetadata.Builder, com.scalar.db.rpc.TableMetadataOrBuilder> tableMetadataBuilder_;
    /**
     * <code>.rpc.TableMetadata table_metadata = 1;</code>
     * @return Whether the tableMetadata field is set.
     */
    public boolean hasTableMetadata() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>.rpc.TableMetadata table_metadata = 1;</code>
     * @return The tableMetadata.
     */
    public com.scalar.db.rpc.TableMetadata getTableMetadata() {
      if (tableMetadataBuilder_ == null) {
        return tableMetadata_ == null ? com.scalar.db.rpc.TableMetadata.getDefaultInstance() : tableMetadata_;
      } else {
        return tableMetadataBuilder_.getMessage();
      }
    }
    /**
     * <code>.rpc.TableMetadata table_metadata = 1;</code>
     */
    public Builder setTableMetadata(com.scalar.db.rpc.TableMetadata value) {
      if (tableMetadataBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        tableMetadata_ = value;
      } else {
        tableMetadataBuilder_.setMessage(value);
      }
      bitField0_ |= 0x00000001;
      onChanged();
      return this;
    }
    /**
     * <code>.rpc.TableMetadata table_metadata = 1;</code>
     */
    public Builder setTableMetadata(
        com.scalar.db.rpc.TableMetadata.Builder builderForValue) {
      if (tableMetadataBuilder_ == null) {
        tableMetadata_ = builderForValue.build();
      } else {
        tableMetadataBuilder_.setMessage(builderForValue.build());
      }
      bitField0_ |= 0x00000001;
      onChanged();
      return this;
    }
    /**
     * <code>.rpc.TableMetadata table_metadata = 1;</code>
     */
    public Builder mergeTableMetadata(com.scalar.db.rpc.TableMetadata value) {
      if (tableMetadataBuilder_ == null) {
        if (((bitField0_ & 0x00000001) != 0) &&
          tableMetadata_ != null &&
          tableMetadata_ != com.scalar.db.rpc.TableMetadata.getDefaultInstance()) {
          getTableMetadataBuilder().mergeFrom(value);
        } else {
          tableMetadata_ = value;
        }
      } else {
        tableMetadataBuilder_.mergeFrom(value);
      }
      if (tableMetadata_ != null) {
        bitField0_ |= 0x00000001;
        onChanged();
      }
      return this;
    }
    /**
     * <code>.rpc.TableMetadata table_metadata = 1;</code>
     */
    public Builder clearTableMetadata() {
      bitField0_ = (bitField0_ & ~0x00000001);
      tableMetadata_ = null;
      if (tableMetadataBuilder_ != null) {
        tableMetadataBuilder_.dispose();
        tableMetadataBuilder_ = null;
      }
      onChanged();
      return this;
    }
    /**
     * <code>.rpc.TableMetadata table_metadata = 1;</code>
     */
    public com.scalar.db.rpc.TableMetadata.Builder getTableMetadataBuilder() {
      bitField0_ |= 0x00000001;
      onChanged();
      return getTableMetadataFieldBuilder().getBuilder();
    }
    /**
     * <code>.rpc.TableMetadata table_metadata = 1;</code>
     */
    public com.scalar.db.rpc.TableMetadataOrBuilder getTableMetadataOrBuilder() {
      if (tableMetadataBuilder_ != null) {
        return tableMetadataBuilder_.getMessageOrBuilder();
      } else {
        return tableMetadata_ == null ?
            com.scalar.db.rpc.TableMetadata.getDefaultInstance() : tableMetadata_;
      }
    }
    /**
     * <code>.rpc.TableMetadata table_metadata = 1;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        com.scalar.db.rpc.TableMetadata, com.scalar.db.rpc.TableMetadata.Builder, com.scalar.db.rpc.TableMetadataOrBuilder> 
        getTableMetadataFieldBuilder() {
      if (tableMetadataBuilder_ == null) {
        tableMetadataBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            com.scalar.db.rpc.TableMetadata, com.scalar.db.rpc.TableMetadata.Builder, com.scalar.db.rpc.TableMetadataOrBuilder>(
                getTableMetadata(),
                getParentForChildren(),
                isClean());
        tableMetadata_ = null;
      }
      return tableMetadataBuilder_;
    }
    @java.lang.Override
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    @java.lang.Override
    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:rpc.GetTableMetadataResponse)
  }

  // @@protoc_insertion_point(class_scope:rpc.GetTableMetadataResponse)
  private static final com.scalar.db.rpc.GetTableMetadataResponse DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new com.scalar.db.rpc.GetTableMetadataResponse();
  }

  public static com.scalar.db.rpc.GetTableMetadataResponse getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<GetTableMetadataResponse>
      PARSER = new com.google.protobuf.AbstractParser<GetTableMetadataResponse>() {
    @java.lang.Override
    public GetTableMetadataResponse parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      Builder builder = newBuilder();
      try {
        builder.mergeFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(builder.buildPartial());
      } catch (com.google.protobuf.UninitializedMessageException e) {
        throw e.asInvalidProtocolBufferException().setUnfinishedMessage(builder.buildPartial());
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(e)
            .setUnfinishedMessage(builder.buildPartial());
      }
      return builder.buildPartial();
    }
  };

  public static com.google.protobuf.Parser<GetTableMetadataResponse> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<GetTableMetadataResponse> getParserForType() {
    return PARSER;
  }

  @java.lang.Override
  public com.scalar.db.rpc.GetTableMetadataResponse getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

