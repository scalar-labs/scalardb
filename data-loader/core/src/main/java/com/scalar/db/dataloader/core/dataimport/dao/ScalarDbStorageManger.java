package com.scalar.db.dataloader.core.dataimport.dao;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.service.StorageFactory;
import java.io.IOException;
import javax.annotation.Nullable;

public class ScalarDbStorageManger {

  @Nullable private final DistributedStorage storage;
  private final DistributedStorageAdmin storageAdmin;

  /**
   * Class constructor
   *
   * @param storageFactory Factory to create all the necessary ScalarDB data managers
   */
  public ScalarDbStorageManger(StorageFactory storageFactory) throws IOException {
    storage = storageFactory.getStorage();
    storageAdmin = storageFactory.getStorageAdmin();
  }

  /** @return storage for ScalarDB connection that is running in storage mode */
  public DistributedStorage getDistributedStorage() {
    return storage;
  }

  /** @return Distributed storage admin for ScalarDB admin operations */
  public DistributedStorageAdmin getDistributedStorageAdmin() {
    return storageAdmin;
  }
}
