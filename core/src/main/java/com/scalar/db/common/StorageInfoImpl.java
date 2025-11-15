package com.scalar.db.common;

import com.google.common.base.MoreObjects;
import com.scalar.db.api.StorageInfo;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class StorageInfoImpl implements StorageInfo {

  private final String storageName;
  private final AtomicityUnit atomicityUnit;
  private final int maxAtomicMutationsCount;
  private final boolean consistentReadGuaranteed;

  public StorageInfoImpl(
      String storageName,
      AtomicityUnit atomicityUnit,
      int maxAtomicMutationsCount,
      boolean consistentReadGuaranteed) {
    this.storageName = storageName;
    this.atomicityUnit = atomicityUnit;
    this.maxAtomicMutationsCount = maxAtomicMutationsCount;
    this.consistentReadGuaranteed = consistentReadGuaranteed;
  }

  @Override
  public String getStorageName() {
    return storageName;
  }

  @Override
  public AtomicityUnit getAtomicityUnit() {
    return atomicityUnit;
  }

  @Override
  public int getMaxAtomicMutationsCount() {
    return maxAtomicMutationsCount;
  }

  @Override
  public boolean isConsistentReadGuaranteed() {
    return consistentReadGuaranteed;
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
        && getAtomicityUnit() == that.getAtomicityUnit()
        && isConsistentReadGuaranteed() == that.isConsistentReadGuaranteed();
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getStorageName(),
        getAtomicityUnit(),
        getMaxAtomicMutationsCount(),
        isConsistentReadGuaranteed());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("storageName", storageName)
        .add("atomicityUnit", atomicityUnit)
        .add("maxAtomicMutationsCount", maxAtomicMutationsCount)
        .add("consistentReadGuaranteed", consistentReadGuaranteed)
        .toString();
  }
}
