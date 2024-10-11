// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: scalardb.proto

// Protobuf Java Version: 3.25.5
package com.scalar.db.rpc;

/**
 * Protobuf type {@code rpc.ScanRequest}
 */
public final class ScanRequest extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:rpc.ScanRequest)
    ScanRequestOrBuilder {
private static final long serialVersionUID = 0L;
  // Use ScanRequest.newBuilder() to construct.
  private ScanRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private ScanRequest() {
  }

  @java.lang.Override
  @SuppressWarnings({"unused"})
  protected java.lang.Object newInstance(
      UnusedPrivateParameter unused) {
    return new ScanRequest();
  }

  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return com.scalar.db.rpc.ScalarDbProto.internal_static_rpc_ScanRequest_descriptor;
  }

  @java.lang.Override
  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return com.scalar.db.rpc.ScalarDbProto.internal_static_rpc_ScanRequest_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            com.scalar.db.rpc.ScanRequest.class, com.scalar.db.rpc.ScanRequest.Builder.class);
  }

  private int bitField0_;
  public static final int SCAN_FIELD_NUMBER = 1;
  private com.scalar.db.rpc.Scan scan_;
  /**
   * <code>.rpc.Scan scan = 1;</code>
   * @return Whether the scan field is set.
   */
  @java.lang.Override
  public boolean hasScan() {
    return ((bitField0_ & 0x00000001) != 0);
  }
  /**
   * <code>.rpc.Scan scan = 1;</code>
   * @return The scan.
   */
  @java.lang.Override
  public com.scalar.db.rpc.Scan getScan() {
    return scan_ == null ? com.scalar.db.rpc.Scan.getDefaultInstance() : scan_;
  }
  /**
   * <code>.rpc.Scan scan = 1;</code>
   */
  @java.lang.Override
  public com.scalar.db.rpc.ScanOrBuilder getScanOrBuilder() {
    return scan_ == null ? com.scalar.db.rpc.Scan.getDefaultInstance() : scan_;
  }

  public static final int FETCH_COUNT_FIELD_NUMBER = 2;
  private int fetchCount_ = 0;
  /**
   * <code>optional int32 fetch_count = 2;</code>
   * @return Whether the fetchCount field is set.
   */
  @java.lang.Override
  public boolean hasFetchCount() {
    return ((bitField0_ & 0x00000002) != 0);
  }
  /**
   * <code>optional int32 fetch_count = 2;</code>
   * @return The fetchCount.
   */
  @java.lang.Override
  public int getFetchCount() {
    return fetchCount_;
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
      output.writeMessage(1, getScan());
    }
    if (((bitField0_ & 0x00000002) != 0)) {
      output.writeInt32(2, fetchCount_);
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
        .computeMessageSize(1, getScan());
    }
    if (((bitField0_ & 0x00000002) != 0)) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt32Size(2, fetchCount_);
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
    if (!(obj instanceof com.scalar.db.rpc.ScanRequest)) {
      return super.equals(obj);
    }
    com.scalar.db.rpc.ScanRequest other = (com.scalar.db.rpc.ScanRequest) obj;

    if (hasScan() != other.hasScan()) return false;
    if (hasScan()) {
      if (!getScan()
          .equals(other.getScan())) return false;
    }
    if (hasFetchCount() != other.hasFetchCount()) return false;
    if (hasFetchCount()) {
      if (getFetchCount()
          != other.getFetchCount()) return false;
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
    if (hasScan()) {
      hash = (37 * hash) + SCAN_FIELD_NUMBER;
      hash = (53 * hash) + getScan().hashCode();
    }
    if (hasFetchCount()) {
      hash = (37 * hash) + FETCH_COUNT_FIELD_NUMBER;
      hash = (53 * hash) + getFetchCount();
    }
    hash = (29 * hash) + getUnknownFields().hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static com.scalar.db.rpc.ScanRequest parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.scalar.db.rpc.ScanRequest parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.scalar.db.rpc.ScanRequest parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.scalar.db.rpc.ScanRequest parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.scalar.db.rpc.ScanRequest parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.scalar.db.rpc.ScanRequest parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.scalar.db.rpc.ScanRequest parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static com.scalar.db.rpc.ScanRequest parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  public static com.scalar.db.rpc.ScanRequest parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }

  public static com.scalar.db.rpc.ScanRequest parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static com.scalar.db.rpc.ScanRequest parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static com.scalar.db.rpc.ScanRequest parseFrom(
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
  public static Builder newBuilder(com.scalar.db.rpc.ScanRequest prototype) {
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
   * Protobuf type {@code rpc.ScanRequest}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:rpc.ScanRequest)
      com.scalar.db.rpc.ScanRequestOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.scalar.db.rpc.ScalarDbProto.internal_static_rpc_ScanRequest_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.scalar.db.rpc.ScalarDbProto.internal_static_rpc_ScanRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.scalar.db.rpc.ScanRequest.class, com.scalar.db.rpc.ScanRequest.Builder.class);
    }

    // Construct using com.scalar.db.rpc.ScanRequest.newBuilder()
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
        getScanFieldBuilder();
      }
    }
    @java.lang.Override
    public Builder clear() {
      super.clear();
      bitField0_ = 0;
      scan_ = null;
      if (scanBuilder_ != null) {
        scanBuilder_.dispose();
        scanBuilder_ = null;
      }
      fetchCount_ = 0;
      return this;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return com.scalar.db.rpc.ScalarDbProto.internal_static_rpc_ScanRequest_descriptor;
    }

    @java.lang.Override
    public com.scalar.db.rpc.ScanRequest getDefaultInstanceForType() {
      return com.scalar.db.rpc.ScanRequest.getDefaultInstance();
    }

    @java.lang.Override
    public com.scalar.db.rpc.ScanRequest build() {
      com.scalar.db.rpc.ScanRequest result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @java.lang.Override
    public com.scalar.db.rpc.ScanRequest buildPartial() {
      com.scalar.db.rpc.ScanRequest result = new com.scalar.db.rpc.ScanRequest(this);
      if (bitField0_ != 0) { buildPartial0(result); }
      onBuilt();
      return result;
    }

    private void buildPartial0(com.scalar.db.rpc.ScanRequest result) {
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) != 0)) {
        result.scan_ = scanBuilder_ == null
            ? scan_
            : scanBuilder_.build();
        to_bitField0_ |= 0x00000001;
      }
      if (((from_bitField0_ & 0x00000002) != 0)) {
        result.fetchCount_ = fetchCount_;
        to_bitField0_ |= 0x00000002;
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
      if (other instanceof com.scalar.db.rpc.ScanRequest) {
        return mergeFrom((com.scalar.db.rpc.ScanRequest)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(com.scalar.db.rpc.ScanRequest other) {
      if (other == com.scalar.db.rpc.ScanRequest.getDefaultInstance()) return this;
      if (other.hasScan()) {
        mergeScan(other.getScan());
      }
      if (other.hasFetchCount()) {
        setFetchCount(other.getFetchCount());
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
                  getScanFieldBuilder().getBuilder(),
                  extensionRegistry);
              bitField0_ |= 0x00000001;
              break;
            } // case 10
            case 16: {
              fetchCount_ = input.readInt32();
              bitField0_ |= 0x00000002;
              break;
            } // case 16
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

    private com.scalar.db.rpc.Scan scan_;
    private com.google.protobuf.SingleFieldBuilderV3<
        com.scalar.db.rpc.Scan, com.scalar.db.rpc.Scan.Builder, com.scalar.db.rpc.ScanOrBuilder> scanBuilder_;
    /**
     * <code>.rpc.Scan scan = 1;</code>
     * @return Whether the scan field is set.
     */
    public boolean hasScan() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>.rpc.Scan scan = 1;</code>
     * @return The scan.
     */
    public com.scalar.db.rpc.Scan getScan() {
      if (scanBuilder_ == null) {
        return scan_ == null ? com.scalar.db.rpc.Scan.getDefaultInstance() : scan_;
      } else {
        return scanBuilder_.getMessage();
      }
    }
    /**
     * <code>.rpc.Scan scan = 1;</code>
     */
    public Builder setScan(com.scalar.db.rpc.Scan value) {
      if (scanBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        scan_ = value;
      } else {
        scanBuilder_.setMessage(value);
      }
      bitField0_ |= 0x00000001;
      onChanged();
      return this;
    }
    /**
     * <code>.rpc.Scan scan = 1;</code>
     */
    public Builder setScan(
        com.scalar.db.rpc.Scan.Builder builderForValue) {
      if (scanBuilder_ == null) {
        scan_ = builderForValue.build();
      } else {
        scanBuilder_.setMessage(builderForValue.build());
      }
      bitField0_ |= 0x00000001;
      onChanged();
      return this;
    }
    /**
     * <code>.rpc.Scan scan = 1;</code>
     */
    public Builder mergeScan(com.scalar.db.rpc.Scan value) {
      if (scanBuilder_ == null) {
        if (((bitField0_ & 0x00000001) != 0) &&
          scan_ != null &&
          scan_ != com.scalar.db.rpc.Scan.getDefaultInstance()) {
          getScanBuilder().mergeFrom(value);
        } else {
          scan_ = value;
        }
      } else {
        scanBuilder_.mergeFrom(value);
      }
      if (scan_ != null) {
        bitField0_ |= 0x00000001;
        onChanged();
      }
      return this;
    }
    /**
     * <code>.rpc.Scan scan = 1;</code>
     */
    public Builder clearScan() {
      bitField0_ = (bitField0_ & ~0x00000001);
      scan_ = null;
      if (scanBuilder_ != null) {
        scanBuilder_.dispose();
        scanBuilder_ = null;
      }
      onChanged();
      return this;
    }
    /**
     * <code>.rpc.Scan scan = 1;</code>
     */
    public com.scalar.db.rpc.Scan.Builder getScanBuilder() {
      bitField0_ |= 0x00000001;
      onChanged();
      return getScanFieldBuilder().getBuilder();
    }
    /**
     * <code>.rpc.Scan scan = 1;</code>
     */
    public com.scalar.db.rpc.ScanOrBuilder getScanOrBuilder() {
      if (scanBuilder_ != null) {
        return scanBuilder_.getMessageOrBuilder();
      } else {
        return scan_ == null ?
            com.scalar.db.rpc.Scan.getDefaultInstance() : scan_;
      }
    }
    /**
     * <code>.rpc.Scan scan = 1;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        com.scalar.db.rpc.Scan, com.scalar.db.rpc.Scan.Builder, com.scalar.db.rpc.ScanOrBuilder> 
        getScanFieldBuilder() {
      if (scanBuilder_ == null) {
        scanBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            com.scalar.db.rpc.Scan, com.scalar.db.rpc.Scan.Builder, com.scalar.db.rpc.ScanOrBuilder>(
                getScan(),
                getParentForChildren(),
                isClean());
        scan_ = null;
      }
      return scanBuilder_;
    }

    private int fetchCount_ ;
    /**
     * <code>optional int32 fetch_count = 2;</code>
     * @return Whether the fetchCount field is set.
     */
    @java.lang.Override
    public boolean hasFetchCount() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>optional int32 fetch_count = 2;</code>
     * @return The fetchCount.
     */
    @java.lang.Override
    public int getFetchCount() {
      return fetchCount_;
    }
    /**
     * <code>optional int32 fetch_count = 2;</code>
     * @param value The fetchCount to set.
     * @return This builder for chaining.
     */
    public Builder setFetchCount(int value) {

      fetchCount_ = value;
      bitField0_ |= 0x00000002;
      onChanged();
      return this;
    }
    /**
     * <code>optional int32 fetch_count = 2;</code>
     * @return This builder for chaining.
     */
    public Builder clearFetchCount() {
      bitField0_ = (bitField0_ & ~0x00000002);
      fetchCount_ = 0;
      onChanged();
      return this;
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


    // @@protoc_insertion_point(builder_scope:rpc.ScanRequest)
  }

  // @@protoc_insertion_point(class_scope:rpc.ScanRequest)
  private static final com.scalar.db.rpc.ScanRequest DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new com.scalar.db.rpc.ScanRequest();
  }

  public static com.scalar.db.rpc.ScanRequest getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<ScanRequest>
      PARSER = new com.google.protobuf.AbstractParser<ScanRequest>() {
    @java.lang.Override
    public ScanRequest parsePartialFrom(
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

  public static com.google.protobuf.Parser<ScanRequest> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<ScanRequest> getParserForType() {
    return PARSER;
  }

  @java.lang.Override
  public com.scalar.db.rpc.ScanRequest getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

