package com.scalar.db.api;

import com.google.common.base.MoreObjects;
import com.scalar.db.exception.storage.ExecutionException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/** Metadata for a distributed transaction manager. */
@Immutable
public class DistributedTransactionManagerMetadata {

  /** The type of the transaction manager. */
  private final String type;

  /**
   * The admin for the distributed storage. It is used to get an underlying distributed storage
   * metadata. If it's null, that indicates the underlying distributed storage metadata is not
   * available.
   */
  @Nullable private final DistributedStorageAdmin distributedStorageAdmin;

  private DistributedTransactionManagerMetadata(
      String type, @Nullable DistributedStorageAdmin distributedStorageAdmin) {
    this.type = type;
    this.distributedStorageAdmin = distributedStorageAdmin;
  }

  /**
   * Returns the type of the transaction manager.
   *
   * @return the type of the transaction manager
   */
  public String getType() {
    return type;
  }

  /**
   * Returns whether the underlying distributed storage metadata is available or not.
   *
   * @return whether the underlying distributed storage metadata is available or not
   */
  public boolean isDistributedStorageMetadataAvailable() {
    return distributedStorageAdmin != null;
  }

  /**
   * Returns the underlying distributed storage metadata for a specified namespace. If the
   * underlying metadata is not available, it throws an {@link UnsupportedOperationException}. You
   * can check if the underlying distributed storage metadata is available or not by {@link
   * #isDistributedStorageMetadataAvailable()}.
   *
   * @param namespace the namespace
   * @return the underlying distributed storage metadata
   * @throws UnsupportedOperationException if the underlying distributed storage metadata is not
   *     available
   * @throws IllegalArgumentException if the namespace does not exist
   * @throws ExecutionException if the operation fails
   */
  public DistributedStorageMetadata getDistributedStorageMetadata(String namespace)
      throws ExecutionException {
    if (distributedStorageAdmin == null) {
      throw new UnsupportedOperationException(
          "the underlying distributed storage metadata is not available");
    }

    return distributedStorageAdmin.getDistributedStorageMetadata(namespace);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DistributedTransactionManagerMetadata)) {
      return false;
    }
    DistributedTransactionManagerMetadata that = (DistributedTransactionManagerMetadata) o;
    return type.equals(that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("type", type).toString();
  }

  /**
   * Returns a new builder for {@code DistributedTransactionManagerMetadata}.
   *
   * @return a new builder for {@code DistributedTransactionManagerMetadata}
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /** A builder for {@code DistributedTransactionManagerMetadata}. */
  public static class Builder {
    private String type;
    private DistributedStorageAdmin distributedStorageAdmin;

    private Builder() {}

    /**
     * Sets the type of the transaction manager.
     *
     * @param type the type of the transaction manager
     * @return this builder
     */
    public Builder type(String type) {
      this.type = type;
      return this;
    }

    /**
     * Sets the admin for the distributed storage. It is used to get an underlying distributed
     * storage metadata.
     *
     * @param distributedStorageAdmin the admin for the distributed storage
     * @return this builder
     */
    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public Builder distributedStorageAdmin(DistributedStorageAdmin distributedStorageAdmin) {
      this.distributedStorageAdmin = distributedStorageAdmin;
      return this;
    }

    /**
     * Builds an instance of {@code DistributedTransactionManagerMetadata}.
     *
     * @return an instance of {@code DistributedTransactionManagerMetadata}
     */
    public DistributedTransactionManagerMetadata build() {
      if (type == null) {
        throw new IllegalStateException("type is not set");
      }

      return new DistributedTransactionManagerMetadata(type, distributedStorageAdmin);
    }
  }
}
