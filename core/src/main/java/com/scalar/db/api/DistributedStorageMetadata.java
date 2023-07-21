package com.scalar.db.api;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

/** Metadata for a distributed storage. */
@Immutable
public class DistributedStorageMetadata {

  /** The type of the storage. */
  private final String type;

  /**
   * The name of the storage. If you use `multi-storage`, it is the storage name configured in your
   * multi-storage configuration. Otherwise, it is the same as the type.
   */
  private final String name;

  /** Whether the linearizable scan all is supported or not. */
  private final boolean linearizableScanAllSupported;

  /** The atomicity level of the storage. */
  private final DistributedStorageAtomicityLevel atomicityLevel;

  private DistributedStorageMetadata(
      String type,
      String name,
      boolean linearizableScanAllSupported,
      DistributedStorageAtomicityLevel atomicityLevel) {
    this.type = type;
    this.name = name;
    this.linearizableScanAllSupported = linearizableScanAllSupported;
    this.atomicityLevel = atomicityLevel;
  }

  /**
   * Returns the type of the storage.
   *
   * @return the type of the storage
   */
  public String getType() {
    return type;
  }

  /**
   * Returns the name of the storage. If you use `multi-storage`, it is the storage name configured
   * in your multi-storage configurations. Otherwise, it is the same as the type.
   *
   * @return the name of the storage
   */
  public String getName() {
    return name;
  }

  /**
   * Returns whether the linearizable scan all is supported or not.
   *
   * @return whether the linearizable scan all is supported or not
   */
  public boolean isLinearizableScanAllSupported() {
    return linearizableScanAllSupported;
  }

  /**
   * Returns the atomicity level of the storage.
   *
   * @return the atomicity level of the storage
   */
  public DistributedStorageAtomicityLevel getAtomicityLevel() {
    return atomicityLevel;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DistributedStorageMetadata)) {
      return false;
    }
    DistributedStorageMetadata that = (DistributedStorageMetadata) o;
    return type.equals(that.type) && name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, name);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("type", type)
        .add("name", name)
        .add("linearizableScanAllSupported", linearizableScanAllSupported)
        .add("atomicityLevel", atomicityLevel)
        .toString();
  }

  /**
   * Creates a new {@link Builder} instance.
   *
   * @return a new {@link Builder} instance
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Creates a new {@link Builder} instance based on {@link DistributedStorageMetadata} instance.
   *
   * @param prototype a {@link DistributedStorageMetadata} instance
   * @return a new {@link Builder} instance
   */
  public static Builder newBuilder(DistributedStorageMetadata prototype) {
    return new Builder(prototype);
  }

  /** A builder for {@link DistributedStorageMetadata}. */
  public static class Builder {
    private String type;
    private String name;
    private boolean linearizableScanAllSupported;
    private DistributedStorageAtomicityLevel atomicityLevel =
        DistributedStorageAtomicityLevel.RECORD;

    private Builder() {}

    private Builder(DistributedStorageMetadata prototype) {
      this.type = prototype.type;
      this.name = prototype.name;
      this.linearizableScanAllSupported = prototype.linearizableScanAllSupported;
      this.atomicityLevel = prototype.atomicityLevel;
    }

    /**
     * Sets the type of the storage.
     *
     * @param type the type of the storage
     * @return this {@link Builder} instance
     */
    public Builder type(String type) {
      this.type = type;
      return this;
    }

    /**
     * Sets the name of the storage.
     *
     * @param name the name of the storage
     * @return this {@link Builder} instance
     */
    public Builder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * Specifies that the linearizable scan all is supported.
     *
     * @return this {@link Builder} instance
     */
    public Builder linearizableScanAllSupported() {
      this.linearizableScanAllSupported = true;
      return this;
    }

    /**
     * Sets whether the linearizable scan all is supported or not.
     *
     * @param linearizableScanAllSupported whether the linearizable scan all is supported or not
     * @return this {@link Builder} instance
     */
    public Builder linearizableScanAllSupported(boolean linearizableScanAllSupported) {
      this.linearizableScanAllSupported = linearizableScanAllSupported;
      return this;
    }

    /**
     * Sets the atomicity level of the storage.
     *
     * @param atomicityLevel the atomicity level of the storage
     * @return this {@link Builder} instance
     */
    public Builder atomicityLevel(DistributedStorageAtomicityLevel atomicityLevel) {
      this.atomicityLevel = atomicityLevel;
      return this;
    }

    /**
     * Builds a {@link DistributedStorageMetadata} instance.
     *
     * @return a {@link DistributedStorageMetadata} instance
     */
    public DistributedStorageMetadata build() {
      if (type == null) {
        throw new IllegalStateException("type is not set");
      }
      if (name == null) {
        throw new IllegalStateException("name is not set");
      }

      return new DistributedStorageMetadata(
          type, name, linearizableScanAllSupported, atomicityLevel);
    }
  }
}
