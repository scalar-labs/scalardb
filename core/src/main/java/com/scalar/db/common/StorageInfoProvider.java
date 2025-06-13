package com.scalar.db.common;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.StorageInfo;
import com.scalar.db.exception.storage.ExecutionException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class StorageInfoProvider {

  private final DistributedStorageAdmin admin;

  // Cache to store storage information. A map namespace to StorageInfo.
  private final ConcurrentMap<String, StorageInfo> storageInfoCache = new ConcurrentHashMap<>();

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public StorageInfoProvider(DistributedStorageAdmin admin) {
    this.admin = admin;
  }

  public StorageInfo getStorageInfo(String namespace) throws ExecutionException {
    try {
      return storageInfoCache.computeIfAbsent(namespace, this::getStorageInfoInternal);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof ExecutionException) {
        throw (ExecutionException) e.getCause();
      }
      throw e;
    }
  }

  private StorageInfo getStorageInfoInternal(String namespace) {
    try {
      return admin.getStorageInfo(namespace);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
