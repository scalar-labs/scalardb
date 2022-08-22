// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: scalardb.proto

package com.scalar.db.rpc;

/**
 * Protobuf type {@code rpc.MutateCondition}
 */
public final class MutateCondition extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:rpc.MutateCondition)
    MutateConditionOrBuilder {
private static final long serialVersionUID = 0L;
  // Use MutateCondition.newBuilder() to construct.
  private MutateCondition(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private MutateCondition() {
    type_ = 0;
    expressions_ = java.util.Collections.emptyList();
  }

  @java.lang.Override
  @SuppressWarnings({"unused"})
  protected java.lang.Object newInstance(
      UnusedPrivateParameter unused) {
    return new MutateCondition();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private MutateCondition(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    this();
    if (extensionRegistry == null) {
      throw new java.lang.NullPointerException();
    }
    int mutable_bitField0_ = 0;
    com.google.protobuf.UnknownFieldSet.Builder unknownFields =
        com.google.protobuf.UnknownFieldSet.newBuilder();
    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          case 8: {
            int rawValue = input.readEnum();

            type_ = rawValue;
            break;
          }
          case 18: {
            if (!((mutable_bitField0_ & 0x00000001) != 0)) {
              expressions_ = new java.util.ArrayList<com.scalar.db.rpc.ConditionalExpression>();
              mutable_bitField0_ |= 0x00000001;
            }
            expressions_.add(
                input.readMessage(com.scalar.db.rpc.ConditionalExpression.parser(), extensionRegistry));
            break;
          }
          default: {
            if (!parseUnknownField(
                input, unknownFields, extensionRegistry, tag)) {
              done = true;
            }
            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (com.google.protobuf.UninitializedMessageException e) {
      throw e.asInvalidProtocolBufferException().setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new com.google.protobuf.InvalidProtocolBufferException(
          e).setUnfinishedMessage(this);
    } finally {
      if (((mutable_bitField0_ & 0x00000001) != 0)) {
        expressions_ = java.util.Collections.unmodifiableList(expressions_);
      }
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return com.scalar.db.rpc.ScalarDbProto.internal_static_rpc_MutateCondition_descriptor;
  }

  @java.lang.Override
  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return com.scalar.db.rpc.ScalarDbProto.internal_static_rpc_MutateCondition_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            com.scalar.db.rpc.MutateCondition.class, com.scalar.db.rpc.MutateCondition.Builder.class);
  }

  /**
   * Protobuf enum {@code rpc.MutateCondition.Type}
   */
  public enum Type
      implements com.google.protobuf.ProtocolMessageEnum {
    /**
     * <code>PUT_IF = 0;</code>
     */
    PUT_IF(0),
    /**
     * <code>PUT_IF_EXISTS = 1;</code>
     */
    PUT_IF_EXISTS(1),
    /**
     * <code>PUT_IF_NOT_EXISTS = 2;</code>
     */
    PUT_IF_NOT_EXISTS(2),
    /**
     * <code>DELETE_IF = 3;</code>
     */
    DELETE_IF(3),
    /**
     * <code>DELETE_IF_EXISTS = 4;</code>
     */
    DELETE_IF_EXISTS(4),
    UNRECOGNIZED(-1),
    ;

    /**
     * <code>PUT_IF = 0;</code>
     */
    public static final int PUT_IF_VALUE = 0;
    /**
     * <code>PUT_IF_EXISTS = 1;</code>
     */
    public static final int PUT_IF_EXISTS_VALUE = 1;
    /**
     * <code>PUT_IF_NOT_EXISTS = 2;</code>
     */
    public static final int PUT_IF_NOT_EXISTS_VALUE = 2;
    /**
     * <code>DELETE_IF = 3;</code>
     */
    public static final int DELETE_IF_VALUE = 3;
    /**
     * <code>DELETE_IF_EXISTS = 4;</code>
     */
    public static final int DELETE_IF_EXISTS_VALUE = 4;


    public final int getNumber() {
      if (this == UNRECOGNIZED) {
        throw new java.lang.IllegalArgumentException(
            "Can't get the number of an unknown enum value.");
      }
      return value;
    }

    /**
     * @param value The numeric wire value of the corresponding enum entry.
     * @return The enum associated with the given numeric wire value.
     * @deprecated Use {@link #forNumber(int)} instead.
     */
    @java.lang.Deprecated
    public static Type valueOf(int value) {
      return forNumber(value);
    }

    /**
     * @param value The numeric wire value of the corresponding enum entry.
     * @return The enum associated with the given numeric wire value.
     */
    public static Type forNumber(int value) {
      switch (value) {
        case 0: return PUT_IF;
        case 1: return PUT_IF_EXISTS;
        case 2: return PUT_IF_NOT_EXISTS;
        case 3: return DELETE_IF;
        case 4: return DELETE_IF_EXISTS;
        default: return null;
      }
    }

    public static com.google.protobuf.Internal.EnumLiteMap<Type>
        internalGetValueMap() {
      return internalValueMap;
    }
    private static final com.google.protobuf.Internal.EnumLiteMap<
        Type> internalValueMap =
          new com.google.protobuf.Internal.EnumLiteMap<Type>() {
            public Type findValueByNumber(int number) {
              return Type.forNumber(number);
            }
          };

    public final com.google.protobuf.Descriptors.EnumValueDescriptor
        getValueDescriptor() {
      if (this == UNRECOGNIZED) {
        throw new java.lang.IllegalStateException(
            "Can't get the descriptor of an unrecognized enum value.");
      }
      return getDescriptor().getValues().get(ordinal());
    }
    public final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptorForType() {
      return getDescriptor();
    }
    public static final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptor() {
      return com.scalar.db.rpc.MutateCondition.getDescriptor().getEnumTypes().get(0);
    }

    private static final Type[] VALUES = values();

    public static Type valueOf(
        com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
      if (desc.getType() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "EnumValueDescriptor is not for this type.");
      }
      if (desc.getIndex() == -1) {
        return UNRECOGNIZED;
      }
      return VALUES[desc.getIndex()];
    }

    private final int value;

    private Type(int value) {
      this.value = value;
    }

    // @@protoc_insertion_point(enum_scope:rpc.MutateCondition.Type)
  }

  public static final int TYPE_FIELD_NUMBER = 1;
  private int type_;
  /**
   * <code>.rpc.MutateCondition.Type type = 1;</code>
   * @return The enum numeric value on the wire for type.
   */
  @java.lang.Override public int getTypeValue() {
    return type_;
  }
  /**
   * <code>.rpc.MutateCondition.Type type = 1;</code>
   * @return The type.
   */
  @java.lang.Override public com.scalar.db.rpc.MutateCondition.Type getType() {
    @SuppressWarnings("deprecation")
    com.scalar.db.rpc.MutateCondition.Type result = com.scalar.db.rpc.MutateCondition.Type.valueOf(type_);
    return result == null ? com.scalar.db.rpc.MutateCondition.Type.UNRECOGNIZED : result;
  }

  public static final int EXPRESSIONS_FIELD_NUMBER = 2;
  private java.util.List<com.scalar.db.rpc.ConditionalExpression> expressions_;
  /**
   * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
   */
  @java.lang.Override
  public java.util.List<com.scalar.db.rpc.ConditionalExpression> getExpressionsList() {
    return expressions_;
  }
  /**
   * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
   */
  @java.lang.Override
  public java.util.List<? extends com.scalar.db.rpc.ConditionalExpressionOrBuilder> 
      getExpressionsOrBuilderList() {
    return expressions_;
  }
  /**
   * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
   */
  @java.lang.Override
  public int getExpressionsCount() {
    return expressions_.size();
  }
  /**
   * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
   */
  @java.lang.Override
  public com.scalar.db.rpc.ConditionalExpression getExpressions(int index) {
    return expressions_.get(index);
  }
  /**
   * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
   */
  @java.lang.Override
  public com.scalar.db.rpc.ConditionalExpressionOrBuilder getExpressionsOrBuilder(
      int index) {
    return expressions_.get(index);
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
    if (type_ != com.scalar.db.rpc.MutateCondition.Type.PUT_IF.getNumber()) {
      output.writeEnum(1, type_);
    }
    for (int i = 0; i < expressions_.size(); i++) {
      output.writeMessage(2, expressions_.get(i));
    }
    unknownFields.writeTo(output);
  }

  @java.lang.Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (type_ != com.scalar.db.rpc.MutateCondition.Type.PUT_IF.getNumber()) {
      size += com.google.protobuf.CodedOutputStream
        .computeEnumSize(1, type_);
    }
    for (int i = 0; i < expressions_.size(); i++) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(2, expressions_.get(i));
    }
    size += unknownFields.getSerializedSize();
    memoizedSize = size;
    return size;
  }

  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof com.scalar.db.rpc.MutateCondition)) {
      return super.equals(obj);
    }
    com.scalar.db.rpc.MutateCondition other = (com.scalar.db.rpc.MutateCondition) obj;

    if (type_ != other.type_) return false;
    if (!getExpressionsList()
        .equals(other.getExpressionsList())) return false;
    if (!unknownFields.equals(other.unknownFields)) return false;
    return true;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    hash = (37 * hash) + TYPE_FIELD_NUMBER;
    hash = (53 * hash) + type_;
    if (getExpressionsCount() > 0) {
      hash = (37 * hash) + EXPRESSIONS_FIELD_NUMBER;
      hash = (53 * hash) + getExpressionsList().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static com.scalar.db.rpc.MutateCondition parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.scalar.db.rpc.MutateCondition parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.scalar.db.rpc.MutateCondition parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.scalar.db.rpc.MutateCondition parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.scalar.db.rpc.MutateCondition parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.scalar.db.rpc.MutateCondition parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.scalar.db.rpc.MutateCondition parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static com.scalar.db.rpc.MutateCondition parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static com.scalar.db.rpc.MutateCondition parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static com.scalar.db.rpc.MutateCondition parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static com.scalar.db.rpc.MutateCondition parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static com.scalar.db.rpc.MutateCondition parseFrom(
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
  public static Builder newBuilder(com.scalar.db.rpc.MutateCondition prototype) {
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
   * Protobuf type {@code rpc.MutateCondition}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:rpc.MutateCondition)
      com.scalar.db.rpc.MutateConditionOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.scalar.db.rpc.ScalarDbProto.internal_static_rpc_MutateCondition_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.scalar.db.rpc.ScalarDbProto.internal_static_rpc_MutateCondition_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.scalar.db.rpc.MutateCondition.class, com.scalar.db.rpc.MutateCondition.Builder.class);
    }

    // Construct using com.scalar.db.rpc.MutateCondition.newBuilder()
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
        getExpressionsFieldBuilder();
      }
    }
    @java.lang.Override
    public Builder clear() {
      super.clear();
      type_ = 0;

      if (expressionsBuilder_ == null) {
        expressions_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
      } else {
        expressionsBuilder_.clear();
      }
      return this;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return com.scalar.db.rpc.ScalarDbProto.internal_static_rpc_MutateCondition_descriptor;
    }

    @java.lang.Override
    public com.scalar.db.rpc.MutateCondition getDefaultInstanceForType() {
      return com.scalar.db.rpc.MutateCondition.getDefaultInstance();
    }

    @java.lang.Override
    public com.scalar.db.rpc.MutateCondition build() {
      com.scalar.db.rpc.MutateCondition result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @java.lang.Override
    public com.scalar.db.rpc.MutateCondition buildPartial() {
      com.scalar.db.rpc.MutateCondition result = new com.scalar.db.rpc.MutateCondition(this);
      int from_bitField0_ = bitField0_;
      result.type_ = type_;
      if (expressionsBuilder_ == null) {
        if (((bitField0_ & 0x00000001) != 0)) {
          expressions_ = java.util.Collections.unmodifiableList(expressions_);
          bitField0_ = (bitField0_ & ~0x00000001);
        }
        result.expressions_ = expressions_;
      } else {
        result.expressions_ = expressionsBuilder_.build();
      }
      onBuilt();
      return result;
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
      if (other instanceof com.scalar.db.rpc.MutateCondition) {
        return mergeFrom((com.scalar.db.rpc.MutateCondition)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(com.scalar.db.rpc.MutateCondition other) {
      if (other == com.scalar.db.rpc.MutateCondition.getDefaultInstance()) return this;
      if (other.type_ != 0) {
        setTypeValue(other.getTypeValue());
      }
      if (expressionsBuilder_ == null) {
        if (!other.expressions_.isEmpty()) {
          if (expressions_.isEmpty()) {
            expressions_ = other.expressions_;
            bitField0_ = (bitField0_ & ~0x00000001);
          } else {
            ensureExpressionsIsMutable();
            expressions_.addAll(other.expressions_);
          }
          onChanged();
        }
      } else {
        if (!other.expressions_.isEmpty()) {
          if (expressionsBuilder_.isEmpty()) {
            expressionsBuilder_.dispose();
            expressionsBuilder_ = null;
            expressions_ = other.expressions_;
            bitField0_ = (bitField0_ & ~0x00000001);
            expressionsBuilder_ = 
              com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders ?
                 getExpressionsFieldBuilder() : null;
          } else {
            expressionsBuilder_.addAllMessages(other.expressions_);
          }
        }
      }
      this.mergeUnknownFields(other.unknownFields);
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
      com.scalar.db.rpc.MutateCondition parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (com.scalar.db.rpc.MutateCondition) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private int type_ = 0;
    /**
     * <code>.rpc.MutateCondition.Type type = 1;</code>
     * @return The enum numeric value on the wire for type.
     */
    @java.lang.Override public int getTypeValue() {
      return type_;
    }
    /**
     * <code>.rpc.MutateCondition.Type type = 1;</code>
     * @param value The enum numeric value on the wire for type to set.
     * @return This builder for chaining.
     */
    public Builder setTypeValue(int value) {
      
      type_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>.rpc.MutateCondition.Type type = 1;</code>
     * @return The type.
     */
    @java.lang.Override
    public com.scalar.db.rpc.MutateCondition.Type getType() {
      @SuppressWarnings("deprecation")
      com.scalar.db.rpc.MutateCondition.Type result = com.scalar.db.rpc.MutateCondition.Type.valueOf(type_);
      return result == null ? com.scalar.db.rpc.MutateCondition.Type.UNRECOGNIZED : result;
    }
    /**
     * <code>.rpc.MutateCondition.Type type = 1;</code>
     * @param value The type to set.
     * @return This builder for chaining.
     */
    public Builder setType(com.scalar.db.rpc.MutateCondition.Type value) {
      if (value == null) {
        throw new NullPointerException();
      }
      
      type_ = value.getNumber();
      onChanged();
      return this;
    }
    /**
     * <code>.rpc.MutateCondition.Type type = 1;</code>
     * @return This builder for chaining.
     */
    public Builder clearType() {
      
      type_ = 0;
      onChanged();
      return this;
    }

    private java.util.List<com.scalar.db.rpc.ConditionalExpression> expressions_ =
      java.util.Collections.emptyList();
    private void ensureExpressionsIsMutable() {
      if (!((bitField0_ & 0x00000001) != 0)) {
        expressions_ = new java.util.ArrayList<com.scalar.db.rpc.ConditionalExpression>(expressions_);
        bitField0_ |= 0x00000001;
       }
    }

    private com.google.protobuf.RepeatedFieldBuilderV3<
        com.scalar.db.rpc.ConditionalExpression, com.scalar.db.rpc.ConditionalExpression.Builder, com.scalar.db.rpc.ConditionalExpressionOrBuilder> expressionsBuilder_;

    /**
     * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
     */
    public java.util.List<com.scalar.db.rpc.ConditionalExpression> getExpressionsList() {
      if (expressionsBuilder_ == null) {
        return java.util.Collections.unmodifiableList(expressions_);
      } else {
        return expressionsBuilder_.getMessageList();
      }
    }
    /**
     * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
     */
    public int getExpressionsCount() {
      if (expressionsBuilder_ == null) {
        return expressions_.size();
      } else {
        return expressionsBuilder_.getCount();
      }
    }
    /**
     * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
     */
    public com.scalar.db.rpc.ConditionalExpression getExpressions(int index) {
      if (expressionsBuilder_ == null) {
        return expressions_.get(index);
      } else {
        return expressionsBuilder_.getMessage(index);
      }
    }
    /**
     * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
     */
    public Builder setExpressions(
        int index, com.scalar.db.rpc.ConditionalExpression value) {
      if (expressionsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureExpressionsIsMutable();
        expressions_.set(index, value);
        onChanged();
      } else {
        expressionsBuilder_.setMessage(index, value);
      }
      return this;
    }
    /**
     * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
     */
    public Builder setExpressions(
        int index, com.scalar.db.rpc.ConditionalExpression.Builder builderForValue) {
      if (expressionsBuilder_ == null) {
        ensureExpressionsIsMutable();
        expressions_.set(index, builderForValue.build());
        onChanged();
      } else {
        expressionsBuilder_.setMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
     */
    public Builder addExpressions(com.scalar.db.rpc.ConditionalExpression value) {
      if (expressionsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureExpressionsIsMutable();
        expressions_.add(value);
        onChanged();
      } else {
        expressionsBuilder_.addMessage(value);
      }
      return this;
    }
    /**
     * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
     */
    public Builder addExpressions(
        int index, com.scalar.db.rpc.ConditionalExpression value) {
      if (expressionsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureExpressionsIsMutable();
        expressions_.add(index, value);
        onChanged();
      } else {
        expressionsBuilder_.addMessage(index, value);
      }
      return this;
    }
    /**
     * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
     */
    public Builder addExpressions(
        com.scalar.db.rpc.ConditionalExpression.Builder builderForValue) {
      if (expressionsBuilder_ == null) {
        ensureExpressionsIsMutable();
        expressions_.add(builderForValue.build());
        onChanged();
      } else {
        expressionsBuilder_.addMessage(builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
     */
    public Builder addExpressions(
        int index, com.scalar.db.rpc.ConditionalExpression.Builder builderForValue) {
      if (expressionsBuilder_ == null) {
        ensureExpressionsIsMutable();
        expressions_.add(index, builderForValue.build());
        onChanged();
      } else {
        expressionsBuilder_.addMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
     */
    public Builder addAllExpressions(
        java.lang.Iterable<? extends com.scalar.db.rpc.ConditionalExpression> values) {
      if (expressionsBuilder_ == null) {
        ensureExpressionsIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, expressions_);
        onChanged();
      } else {
        expressionsBuilder_.addAllMessages(values);
      }
      return this;
    }
    /**
     * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
     */
    public Builder clearExpressions() {
      if (expressionsBuilder_ == null) {
        expressions_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
        onChanged();
      } else {
        expressionsBuilder_.clear();
      }
      return this;
    }
    /**
     * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
     */
    public Builder removeExpressions(int index) {
      if (expressionsBuilder_ == null) {
        ensureExpressionsIsMutable();
        expressions_.remove(index);
        onChanged();
      } else {
        expressionsBuilder_.remove(index);
      }
      return this;
    }
    /**
     * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
     */
    public com.scalar.db.rpc.ConditionalExpression.Builder getExpressionsBuilder(
        int index) {
      return getExpressionsFieldBuilder().getBuilder(index);
    }
    /**
     * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
     */
    public com.scalar.db.rpc.ConditionalExpressionOrBuilder getExpressionsOrBuilder(
        int index) {
      if (expressionsBuilder_ == null) {
        return expressions_.get(index);  } else {
        return expressionsBuilder_.getMessageOrBuilder(index);
      }
    }
    /**
     * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
     */
    public java.util.List<? extends com.scalar.db.rpc.ConditionalExpressionOrBuilder> 
         getExpressionsOrBuilderList() {
      if (expressionsBuilder_ != null) {
        return expressionsBuilder_.getMessageOrBuilderList();
      } else {
        return java.util.Collections.unmodifiableList(expressions_);
      }
    }
    /**
     * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
     */
    public com.scalar.db.rpc.ConditionalExpression.Builder addExpressionsBuilder() {
      return getExpressionsFieldBuilder().addBuilder(
          com.scalar.db.rpc.ConditionalExpression.getDefaultInstance());
    }
    /**
     * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
     */
    public com.scalar.db.rpc.ConditionalExpression.Builder addExpressionsBuilder(
        int index) {
      return getExpressionsFieldBuilder().addBuilder(
          index, com.scalar.db.rpc.ConditionalExpression.getDefaultInstance());
    }
    /**
     * <code>repeated .rpc.ConditionalExpression expressions = 2;</code>
     */
    public java.util.List<com.scalar.db.rpc.ConditionalExpression.Builder> 
         getExpressionsBuilderList() {
      return getExpressionsFieldBuilder().getBuilderList();
    }
    private com.google.protobuf.RepeatedFieldBuilderV3<
        com.scalar.db.rpc.ConditionalExpression, com.scalar.db.rpc.ConditionalExpression.Builder, com.scalar.db.rpc.ConditionalExpressionOrBuilder> 
        getExpressionsFieldBuilder() {
      if (expressionsBuilder_ == null) {
        expressionsBuilder_ = new com.google.protobuf.RepeatedFieldBuilderV3<
            com.scalar.db.rpc.ConditionalExpression, com.scalar.db.rpc.ConditionalExpression.Builder, com.scalar.db.rpc.ConditionalExpressionOrBuilder>(
                expressions_,
                ((bitField0_ & 0x00000001) != 0),
                getParentForChildren(),
                isClean());
        expressions_ = null;
      }
      return expressionsBuilder_;
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


    // @@protoc_insertion_point(builder_scope:rpc.MutateCondition)
  }

  // @@protoc_insertion_point(class_scope:rpc.MutateCondition)
  private static final com.scalar.db.rpc.MutateCondition DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new com.scalar.db.rpc.MutateCondition();
  }

  public static com.scalar.db.rpc.MutateCondition getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<MutateCondition>
      PARSER = new com.google.protobuf.AbstractParser<MutateCondition>() {
    @java.lang.Override
    public MutateCondition parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new MutateCondition(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<MutateCondition> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<MutateCondition> getParserForType() {
    return PARSER;
  }

  @java.lang.Override
  public com.scalar.db.rpc.MutateCondition getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

