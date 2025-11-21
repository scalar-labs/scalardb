package com.scalar.db.common;

import com.google.common.base.MoreObjects;
import com.scalar.db.api.StorageInfo;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class StorageInfoImpl implements StorageInfo {

  private final String storageName;
  private final MutationAtomicityUnit mutationAtomicityUnit;
  private final int maxAtomicMutationsCount;
  private final boolean consistentVirtualTableRead;

  public StorageInfoImpl(
      String storageName,
      MutationAtomicityUnit mutationAtomicityUnit,
      int maxAtomicMutationsCount,
      boolean consistentVirtualTableRead) {
    this.storageName = storageName;
    this.mutationAtomicityUnit = mutationAtomicityUnit;
    this.maxAtomicMutationsCount = maxAtomicMutationsCount;
    this.consistentVirtualTableRead = consistentVirtualTableRead;
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
  public int getMaxAtomicMutationsCount() {
    return maxAtomicMutationsCount;
  }

  @Override
  public boolean isConsistentVirtualTableRead() {
    return consistentVirtualTableRead;
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
    return getMaxAtomicMutationsCount() == that.getMaxAtomicMutationsCount()
        && Objects.equals(getStorageName(), that.getStorageName())
        && getMutationAtomicityUnit() == that.getMutationAtomicityUnit()
        && isConsistentVirtualTableRead() == that.isConsistentVirtualTableRead();
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getStorageName(),
        getMutationAtomicityUnit(),
        getMaxAtomicMutationsCount(),
        isConsistentVirtualTableRead());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("storageName", storageName)
        .add("mutationAtomicityUnit", mutationAtomicityUnit)
        .add("maxAtomicMutationsCount", maxAtomicMutationsCount)
        .add("consistentVirtualTableRead", consistentVirtualTableRead)
        .toString();
  }
}
