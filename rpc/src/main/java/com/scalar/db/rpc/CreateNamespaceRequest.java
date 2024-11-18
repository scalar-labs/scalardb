// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: scalardb.proto

// Protobuf Java Version: 3.25.5
package com.scalar.db.rpc;

/**
 * Protobuf type {@code rpc.CreateNamespaceRequest}
 */
public final class CreateNamespaceRequest extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:rpc.CreateNamespaceRequest)
    CreateNamespaceRequestOrBuilder {
private static final long serialVersionUID = 0L;
  // Use CreateNamespaceRequest.newBuilder() to construct.
  private CreateNamespaceRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private CreateNamespaceRequest() {
    namespace_ = "";
  }

  @java.lang.Override
  @SuppressWarnings({"unused"})
  protected java.lang.Object newInstance(
      UnusedPrivateParameter unused) {
    return new CreateNamespaceRequest();
  }

  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return com.scalar.db.rpc.ScalarDbProto.internal_static_rpc_CreateNamespaceRequest_descriptor;
  }

  @SuppressWarnings({"rawtypes"})
  @java.lang.Override
  protected com.google.protobuf.MapFieldReflectionAccessor internalGetMapFieldReflection(
      int number) {
    switch (number) {
      case 2:
        return internalGetOptions();
      default:
        throw new RuntimeException(
            "Invalid map field number: " + number);
    }
  }
  @java.lang.Override
  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return com.scalar.db.rpc.ScalarDbProto.internal_static_rpc_CreateNamespaceRequest_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            com.scalar.db.rpc.CreateNamespaceRequest.class, com.scalar.db.rpc.CreateNamespaceRequest.Builder.class);
  }

  public static final int NAMESPACE_FIELD_NUMBER = 1;
  @SuppressWarnings("serial")
  private volatile java.lang.Object namespace_ = "";
  /**
   * <code>string namespace = 1;</code>
   * @return The namespace.
   */
  @java.lang.Override
  public java.lang.String getNamespace() {
    java.lang.Object ref = namespace_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      namespace_ = s;
      return s;
    }
  }
  /**
   * <code>string namespace = 1;</code>
   * @return The bytes for namespace.
   */
  @java.lang.Override
  public com.google.protobuf.ByteString
      getNamespaceBytes() {
    java.lang.Object ref = namespace_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      namespace_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int OPTIONS_FIELD_NUMBER = 2;
  private static final class OptionsDefaultEntryHolder {
    static final com.google.protobuf.MapEntry<
        java.lang.String, java.lang.String> defaultEntry =
            com.google.protobuf.MapEntry
            .<java.lang.String, java.lang.String>newDefaultInstance(
                com.scalar.db.rpc.ScalarDbProto.internal_static_rpc_CreateNamespaceRequest_OptionsEntry_descriptor, 
                com.google.protobuf.WireFormat.FieldType.STRING,
                "",
                com.google.protobuf.WireFormat.FieldType.STRING,
                "");
  }
  @SuppressWarnings("serial")
  private com.google.protobuf.MapField<
      java.lang.String, java.lang.String> options_;
  private com.google.protobuf.MapField<java.lang.String, java.lang.String>
  internalGetOptions() {
    if (options_ == null) {
      return com.google.protobuf.MapField.emptyMapField(
          OptionsDefaultEntryHolder.defaultEntry);
    }
    return options_;
  }
  public int getOptionsCount() {
    return internalGetOptions().getMap().size();
  }
  /**
   * <code>map&lt;string, string&gt; options = 2;</code>
   */
  @java.lang.Override
  public boolean containsOptions(
      java.lang.String key) {
    if (key == null) { throw new NullPointerException("map key"); }
    return internalGetOptions().getMap().containsKey(key);
  }
  /**
   * Use {@link #getOptionsMap()} instead.
   */
  @java.lang.Override
  @java.lang.Deprecated
  public java.util.Map<java.lang.String, java.lang.String> getOptions() {
    return getOptionsMap();
  }
  /**
   * <code>map&lt;string, string&gt; options = 2;</code>
   */
  @java.lang.Override
  public java.util.Map<java.lang.String, java.lang.String> getOptionsMap() {
    return internalGetOptions().getMap();
  }
  /**
   * <code>map&lt;string, string&gt; options = 2;</code>
   */
  @java.lang.Override
  public /* nullable */
java.lang.String getOptionsOrDefault(
      java.lang.String key,
      /* nullable */
java.lang.String defaultValue) {
    if (key == null) { throw new NullPointerException("map key"); }
    java.util.Map<java.lang.String, java.lang.String> map =
        internalGetOptions().getMap();
    return map.containsKey(key) ? map.get(key) : defaultValue;
  }
  /**
   * <code>map&lt;string, string&gt; options = 2;</code>
   */
  @java.lang.Override
  public java.lang.String getOptionsOrThrow(
      java.lang.String key) {
    if (key == null) { throw new NullPointerException("map key"); }
    java.util.Map<java.lang.String, java.lang.String> map =
        internalGetOptions().getMap();
    if (!map.containsKey(key)) {
      throw new java.lang.IllegalArgumentException();
    }
    return map.get(key);
  }

  public static final int IF_NOT_EXISTS_FIELD_NUMBER = 3;
  private boolean ifNotExists_ = false;
  /**
   * <code>bool if_not_exists = 3;</code>
   * @return The ifNotExists.
   */
  @java.lang.Override
  public boolean getIfNotExists() {
    return ifNotExists_;
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
    if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(namespace_)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, namespace_);
    }
    com.google.protobuf.GeneratedMessageV3
      .serializeStringMapTo(
        output,
        internalGetOptions(),
        OptionsDefaultEntryHolder.defaultEntry,
        2);
    if (ifNotExists_ != false) {
      output.writeBool(3, ifNotExists_);
    }
    getUnknownFields().writeTo(output);
  }

  @java.lang.Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(namespace_)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, namespace_);
    }
    for (java.util.Map.Entry<java.lang.String, java.lang.String> entry
         : internalGetOptions().getMap().entrySet()) {
      com.google.protobuf.MapEntry<java.lang.String, java.lang.String>
      options__ = OptionsDefaultEntryHolder.defaultEntry.newBuilderForType()
          .setKey(entry.getKey())
          .setValue(entry.getValue())
          .build();
      size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(2, options__);
    }
    if (ifNotExists_ != false) {
      size += com.google.protobuf.CodedOutputStream
        .computeBoolSize(3, ifNotExists_);
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
    if (!(obj instanceof com.scalar.db.rpc.CreateNamespaceRequest)) {
      return super.equals(obj);
    }
    com.scalar.db.rpc.CreateNamespaceRequest other = (com.scalar.db.rpc.CreateNamespaceRequest) obj;

    if (!getNamespace()
        .equals(other.getNamespace())) return false;
    if (!internalGetOptions().equals(
        other.internalGetOptions())) return false;
    if (getIfNotExists()
        != other.getIfNotExists()) return false;
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
    hash = (37 * hash) + NAMESPACE_FIELD_NUMBER;
    hash = (53 * hash) + getNamespace().hashCode();
    if (!internalGetOptions().getMap().isEmpty()) {
      hash = (37 * hash) + OPTIONS_FIELD_NUMBER;
      hash = (53 * hash) + internalGetOptions().hashCode();
    }
    hash = (37 * hash) + IF_NOT_EXISTS_FIELD_NUMBER;
    hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
        getIfNotExists());
    hash = (29 * hash) + getUnknownFields().hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static com.scalar.db.rpc.CreateNamespaceRequest parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.scalar.db.rpc.CreateNamespaceRequest parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.scalar.db.rpc.CreateNamespaceRequest parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.scalar.db.rpc.CreateNamespaceRequest parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.scalar.db.rpc.CreateNamespaceRequest parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.scalar.db.rpc.CreateNamespaceRequest parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.scalar.db.rpc.CreateNamespaceRequest parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static com.scalar.db.rpc.CreateNamespaceRequest parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  public static com.scalar.db.rpc.CreateNamespaceRequest parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }

  public static com.scalar.db.rpc.CreateNamespaceRequest parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static com.scalar.db.rpc.CreateNamespaceRequest parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static com.scalar.db.rpc.CreateNamespaceRequest parseFrom(
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
  public static Builder newBuilder(com.scalar.db.rpc.CreateNamespaceRequest prototype) {
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
   * Protobuf type {@code rpc.CreateNamespaceRequest}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:rpc.CreateNamespaceRequest)
      com.scalar.db.rpc.CreateNamespaceRequestOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.scalar.db.rpc.ScalarDbProto.internal_static_rpc_CreateNamespaceRequest_descriptor;
    }

    @SuppressWarnings({"rawtypes"})
    protected com.google.protobuf.MapFieldReflectionAccessor internalGetMapFieldReflection(
        int number) {
      switch (number) {
        case 2:
          return internalGetOptions();
        default:
          throw new RuntimeException(
              "Invalid map field number: " + number);
      }
    }
    @SuppressWarnings({"rawtypes"})
    protected com.google.protobuf.MapFieldReflectionAccessor internalGetMutableMapFieldReflection(
        int number) {
      switch (number) {
        case 2:
          return internalGetMutableOptions();
        default:
          throw new RuntimeException(
              "Invalid map field number: " + number);
      }
    }
    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.scalar.db.rpc.ScalarDbProto.internal_static_rpc_CreateNamespaceRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.scalar.db.rpc.CreateNamespaceRequest.class, com.scalar.db.rpc.CreateNamespaceRequest.Builder.class);
    }

    // Construct using com.scalar.db.rpc.CreateNamespaceRequest.newBuilder()
    private Builder() {

    }

    private Builder(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      super(parent);

    }
    @java.lang.Override
    public Builder clear() {
      super.clear();
      bitField0_ = 0;
      namespace_ = "";
      internalGetMutableOptions().clear();
      ifNotExists_ = false;
      return this;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return com.scalar.db.rpc.ScalarDbProto.internal_static_rpc_CreateNamespaceRequest_descriptor;
    }

    @java.lang.Override
    public com.scalar.db.rpc.CreateNamespaceRequest getDefaultInstanceForType() {
      return com.scalar.db.rpc.CreateNamespaceRequest.getDefaultInstance();
    }

    @java.lang.Override
    public com.scalar.db.rpc.CreateNamespaceRequest build() {
      com.scalar.db.rpc.CreateNamespaceRequest result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @java.lang.Override
    public com.scalar.db.rpc.CreateNamespaceRequest buildPartial() {
      com.scalar.db.rpc.CreateNamespaceRequest result = new com.scalar.db.rpc.CreateNamespaceRequest(this);
      if (bitField0_ != 0) { buildPartial0(result); }
      onBuilt();
      return result;
    }

    private void buildPartial0(com.scalar.db.rpc.CreateNamespaceRequest result) {
      int from_bitField0_ = bitField0_;
      if (((from_bitField0_ & 0x00000001) != 0)) {
        result.namespace_ = namespace_;
      }
      if (((from_bitField0_ & 0x00000002) != 0)) {
        result.options_ = internalGetOptions();
        result.options_.makeImmutable();
      }
      if (((from_bitField0_ & 0x00000004) != 0)) {
        result.ifNotExists_ = ifNotExists_;
      }
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
      if (other instanceof com.scalar.db.rpc.CreateNamespaceRequest) {
        return mergeFrom((com.scalar.db.rpc.CreateNamespaceRequest)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(com.scalar.db.rpc.CreateNamespaceRequest other) {
      if (other == com.scalar.db.rpc.CreateNamespaceRequest.getDefaultInstance()) return this;
      if (!other.getNamespace().isEmpty()) {
        namespace_ = other.namespace_;
        bitField0_ |= 0x00000001;
        onChanged();
      }
      internalGetMutableOptions().mergeFrom(
          other.internalGetOptions());
      bitField0_ |= 0x00000002;
      if (other.getIfNotExists() != false) {
        setIfNotExists(other.getIfNotExists());
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
              namespace_ = input.readStringRequireUtf8();
              bitField0_ |= 0x00000001;
              break;
            } // case 10
            case 18: {
              com.google.protobuf.MapEntry<java.lang.String, java.lang.String>
              options__ = input.readMessage(
                  OptionsDefaultEntryHolder.defaultEntry.getParserForType(), extensionRegistry);
              internalGetMutableOptions().getMutableMap().put(
                  options__.getKey(), options__.getValue());
              bitField0_ |= 0x00000002;
              break;
            } // case 18
            case 24: {
              ifNotExists_ = input.readBool();
              bitField0_ |= 0x00000004;
              break;
            } // case 24
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

    private java.lang.Object namespace_ = "";
    /**
     * <code>string namespace = 1;</code>
     * @return The namespace.
     */
    public java.lang.String getNamespace() {
      java.lang.Object ref = namespace_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        namespace_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>string namespace = 1;</code>
     * @return The bytes for namespace.
     */
    public com.google.protobuf.ByteString
        getNamespaceBytes() {
      java.lang.Object ref = namespace_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        namespace_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>string namespace = 1;</code>
     * @param value The namespace to set.
     * @return This builder for chaining.
     */
    public Builder setNamespace(
        java.lang.String value) {
      if (value == null) { throw new NullPointerException(); }
      namespace_ = value;
      bitField0_ |= 0x00000001;
      onChanged();
      return this;
    }
    /**
     * <code>string namespace = 1;</code>
     * @return This builder for chaining.
     */
    public Builder clearNamespace() {
      namespace_ = getDefaultInstance().getNamespace();
      bitField0_ = (bitField0_ & ~0x00000001);
      onChanged();
      return this;
    }
    /**
     * <code>string namespace = 1;</code>
     * @param value The bytes for namespace to set.
     * @return This builder for chaining.
     */
    public Builder setNamespaceBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) { throw new NullPointerException(); }
      checkByteStringIsUtf8(value);
      namespace_ = value;
      bitField0_ |= 0x00000001;
      onChanged();
      return this;
    }

    private com.google.protobuf.MapField<
        java.lang.String, java.lang.String> options_;
    private com.google.protobuf.MapField<java.lang.String, java.lang.String>
        internalGetOptions() {
      if (options_ == null) {
        return com.google.protobuf.MapField.emptyMapField(
            OptionsDefaultEntryHolder.defaultEntry);
      }
      return options_;
    }
    private com.google.protobuf.MapField<java.lang.String, java.lang.String>
        internalGetMutableOptions() {
      if (options_ == null) {
        options_ = com.google.protobuf.MapField.newMapField(
            OptionsDefaultEntryHolder.defaultEntry);
      }
      if (!options_.isMutable()) {
        options_ = options_.copy();
      }
      bitField0_ |= 0x00000002;
      onChanged();
      return options_;
    }
    public int getOptionsCount() {
      return internalGetOptions().getMap().size();
    }
    /**
     * <code>map&lt;string, string&gt; options = 2;</code>
     */
    @java.lang.Override
    public boolean containsOptions(
        java.lang.String key) {
      if (key == null) { throw new NullPointerException("map key"); }
      return internalGetOptions().getMap().containsKey(key);
    }
    /**
     * Use {@link #getOptionsMap()} instead.
     */
    @java.lang.Override
    @java.lang.Deprecated
    public java.util.Map<java.lang.String, java.lang.String> getOptions() {
      return getOptionsMap();
    }
    /**
     * <code>map&lt;string, string&gt; options = 2;</code>
     */
    @java.lang.Override
    public java.util.Map<java.lang.String, java.lang.String> getOptionsMap() {
      return internalGetOptions().getMap();
    }
    /**
     * <code>map&lt;string, string&gt; options = 2;</code>
     */
    @java.lang.Override
    public /* nullable */
java.lang.String getOptionsOrDefault(
        java.lang.String key,
        /* nullable */
java.lang.String defaultValue) {
      if (key == null) { throw new NullPointerException("map key"); }
      java.util.Map<java.lang.String, java.lang.String> map =
          internalGetOptions().getMap();
      return map.containsKey(key) ? map.get(key) : defaultValue;
    }
    /**
     * <code>map&lt;string, string&gt; options = 2;</code>
     */
    @java.lang.Override
    public java.lang.String getOptionsOrThrow(
        java.lang.String key) {
      if (key == null) { throw new NullPointerException("map key"); }
      java.util.Map<java.lang.String, java.lang.String> map =
          internalGetOptions().getMap();
      if (!map.containsKey(key)) {
        throw new java.lang.IllegalArgumentException();
      }
      return map.get(key);
    }
    public Builder clearOptions() {
      bitField0_ = (bitField0_ & ~0x00000002);
      internalGetMutableOptions().getMutableMap()
          .clear();
      return this;
    }
    /**
     * <code>map&lt;string, string&gt; options = 2;</code>
     */
    public Builder removeOptions(
        java.lang.String key) {
      if (key == null) { throw new NullPointerException("map key"); }
      internalGetMutableOptions().getMutableMap()
          .remove(key);
      return this;
    }
    /**
     * Use alternate mutation accessors instead.
     */
    @java.lang.Deprecated
    public java.util.Map<java.lang.String, java.lang.String>
        getMutableOptions() {
      bitField0_ |= 0x00000002;
      return internalGetMutableOptions().getMutableMap();
    }
    /**
     * <code>map&lt;string, string&gt; options = 2;</code>
     */
    public Builder putOptions(
        java.lang.String key,
        java.lang.String value) {
      if (key == null) { throw new NullPointerException("map key"); }
      if (value == null) { throw new NullPointerException("map value"); }
      internalGetMutableOptions().getMutableMap()
          .put(key, value);
      bitField0_ |= 0x00000002;
      return this;
    }
    /**
     * <code>map&lt;string, string&gt; options = 2;</code>
     */
    public Builder putAllOptions(
        java.util.Map<java.lang.String, java.lang.String> values) {
      internalGetMutableOptions().getMutableMap()
          .putAll(values);
      bitField0_ |= 0x00000002;
      return this;
    }

    private boolean ifNotExists_ ;
    /**
     * <code>bool if_not_exists = 3;</code>
     * @return The ifNotExists.
     */
    @java.lang.Override
    public boolean getIfNotExists() {
      return ifNotExists_;
    }
    /**
     * <code>bool if_not_exists = 3;</code>
     * @param value The ifNotExists to set.
     * @return This builder for chaining.
     */
    public Builder setIfNotExists(boolean value) {

      ifNotExists_ = value;
      bitField0_ |= 0x00000004;
      onChanged();
      return this;
    }
    /**
     * <code>bool if_not_exists = 3;</code>
     * @return This builder for chaining.
     */
    public Builder clearIfNotExists() {
      bitField0_ = (bitField0_ & ~0x00000004);
      ifNotExists_ = false;
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


    // @@protoc_insertion_point(builder_scope:rpc.CreateNamespaceRequest)
  }

  // @@protoc_insertion_point(class_scope:rpc.CreateNamespaceRequest)
  private static final com.scalar.db.rpc.CreateNamespaceRequest DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new com.scalar.db.rpc.CreateNamespaceRequest();
  }

  public static com.scalar.db.rpc.CreateNamespaceRequest getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<CreateNamespaceRequest>
      PARSER = new com.google.protobuf.AbstractParser<CreateNamespaceRequest>() {
    @java.lang.Override
    public CreateNamespaceRequest parsePartialFrom(
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

  public static com.google.protobuf.Parser<CreateNamespaceRequest> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<CreateNamespaceRequest> getParserForType() {
    return PARSER;
  }

  @java.lang.Override
  public com.scalar.db.rpc.CreateNamespaceRequest getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}
