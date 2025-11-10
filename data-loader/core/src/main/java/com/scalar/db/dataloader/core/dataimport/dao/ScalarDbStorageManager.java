package com.scalar.db.dataloader.core.dataimport.dao;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.service.StorageFactory;
import java.io.IOException;
import javax.annotation.Nullable;

public class ScalarDbStorageManager {

  @Nullable private final DistributedStorage storage;
  private final DistributedStorageAdmin storageAdmin;
  @Nullable private final DistributedTransactionManager distributedTransactionManager;

  /**
   * Class constructor
   *
   * @param storageFactory Factory to create all the necessary ScalarDB data managers
   */
  public ScalarDbStorageManager(StorageFactory storageFactory, DistributedTransactionManager manager) throws IOException {
    storage = storageFactory.getStorage();
    storageAdmin = storageFactory.getStorageAdmin();
    distributedTransactionManager = manager;
  }

  /** Returns distributed storage for ScalarDB connection that is running in storage mode */
  public DistributedStorage getDistributedStorage() {
    return storage;
  }

  /** Returns distributed storage admin for ScalarDB admin operations */
  public DistributedStorageAdmin getDistributedStorageAdmin() {
    return storageAdmin;
  }

  public DistributedTransactionManager getDistributedTransactionManager(){ return  distributedTransactionManager;}
}
