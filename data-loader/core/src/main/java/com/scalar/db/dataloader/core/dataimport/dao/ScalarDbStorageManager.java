package com.scalar.db.dataloader.core.dataimport.dao;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.service.StorageFactory;
import java.io.IOException;
import javax.annotation.Nullable;

public class ScalarDbStorageManager {

  @Nullable private final DistributedStorage storage;
  private final DistributedStorageAdmin storageAdmin;

  /**
   * Class constructor
   *
   * @param storageFactory Factory to create all the necessary ScalarDB data managers
   */
  public ScalarDbStorageManager(StorageFactory storageFactory) throws IOException {
    storage = storageFactory.getStorage();
    storageAdmin = storageFactory.getStorageAdmin();
  }

  /** Returns distributed storage for ScalarDB connection that is running in storage mode */
  public DistributedStorage getDistributedStorage() {
    return storage;
  }

  /** Returns distributed storage admin for ScalarDB admin operations */
  public DistributedStorageAdmin getDistributedStorageAdmin() {
    return storageAdmin;
  }
}
