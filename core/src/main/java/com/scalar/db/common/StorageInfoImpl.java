package com.scalar.db.common;

import com.google.common.base.MoreObjects;
import com.scalar.db.api.StorageInfo;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class StorageInfoImpl implements StorageInfo {

  private final String storageName;
  private final MutationAtomicityUnit mutationAtomicityUnit;
  private final int maxAtomicMutationCount;

  public StorageInfoImpl(
      String storageName, MutationAtomicityUnit mutationAtomicityUnit, int maxAtomicMutationCount) {
    this.storageName = storageName;
    this.mutationAtomicityUnit = mutationAtomicityUnit;
    this.maxAtomicMutationCount = maxAtomicMutationCount;
  }

  @Override
  public String getStorageName() {
    return storageName;
  }

  @Override
  public MutationAtomicityUnit getMutationAtomicityUnit() {
    return mutationAtomicityUnit;
  }

  @Override
  public int getMaxAtomicMutationCount() {
    return maxAtomicMutationCount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StorageInfoImpl)) {
      return false;
    }
    StorageInfoImpl that = (StorageInfoImpl) o;
    return getMaxAtomicMutationCount() == that.getMaxAtomicMutationCount()
        && Objects.equals(getStorageName(), that.getStorageName())
        && getMutationAtomicityUnit() == that.getMutationAtomicityUnit();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getStorageName(), getMutationAtomicityUnit(), getMaxAtomicMutationCount());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("storageName", storageName)
        .add("mutationAtomicityUnit", mutationAtomicityUnit)
        .add("maxAtomicMutationCount", maxAtomicMutationCount)
        .toString();
  }
}
