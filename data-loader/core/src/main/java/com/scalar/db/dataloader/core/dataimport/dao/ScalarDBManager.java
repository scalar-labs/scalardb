package com.scalar.db.dataloader.core.dataimport.dao;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import java.io.IOException;

/**
 * A manager to retrieve the various ScalarDB managers based on the running mode
 *
 * @author Yves Peckstadt
 */
public class ScalarDBManager {

  /* Distributed storage for ScalarDB connection that is running in storage mode. */
  private final DistributedStorage storage;
  /* Distributed Transaction manager for ScalarDB connection that is running in transaction mode */
  private final DistributedTransactionManager transactionManager;
  /* Distributed storage admin for ScalarDB admin operations */
  private final DistributedStorageAdmin storageAdmin;
  private final DistributedTransactionAdmin transactionAdmin;

  /**
   * Class constructor
   *
   * @param storageFactory Factory to create all the necessary ScalarDB data managers
   */
  public ScalarDBManager(StorageFactory storageFactory) throws IOException {
    storage = storageFactory.getStorage();
    storageAdmin = storageFactory.getStorageAdmin();
    transactionManager = null;
    transactionAdmin = null;
  }

  /**
   * Class constructor
   *
   * @param transactionFactory Factory to create all the necessary ScalarDB data managers
   */
  public ScalarDBManager(TransactionFactory transactionFactory) throws IOException {
    transactionManager = transactionFactory.getTransactionManager();
    transactionAdmin = transactionFactory.getTransactionAdmin();
    storageAdmin = null;
    storage = null;
  }

  /** @return storage for ScalarDB connection that is running in storage mode */
  public DistributedStorage getDistributedStorage() {
    return storage;
  }

  /**
   * @return Distributed Transaction manager for ScalarDB connection that is running in transaction
   *     mode
   */
  public DistributedTransactionManager getDistributedTransactionManager() {
    return transactionManager;
  }

  /** @return Distributed storage admin for ScalarDB admin operations */
  public DistributedStorageAdmin getDistributedStorageAdmin() {
    return storageAdmin;
  }
}
