package com.scalar.db.dataloader.core.dataimport.dao;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.service.StorageFactory;
import javax.annotation.Nullable;

/**
 * A manager class for handling ScalarDB operations in storage mode.
 *
 * <p>Provides access to {@link DistributedStorage} for data operations and {@link
 * DistributedStorageAdmin} for administrative operations such as schema management.
 *
 * <p>This class is typically used when interacting with ScalarDB in a non-transactional,
 * storage-only configuration.
 */
public class ScalarDbStorageManager {

  @Nullable private final DistributedStorage storage;
  private final DistributedStorageAdmin storageAdmin;

  /**
   * Constructs a {@code ScalarDbStorageManager} using the provided {@link StorageFactory}.
   *
   * @param storageFactory the factory used to create the ScalarDB storage and admin instances
   */
  public ScalarDbStorageManager(StorageFactory storageFactory) {
    storage = storageFactory.getStorage();
    storageAdmin = storageFactory.getStorageAdmin();
  }

  /**
   * Returns distributed storage for ScalarDB connection that is running in storage mode
   *
   * @return distributed storage object
   */
  public DistributedStorage getDistributedStorage() {
    return storage;
  }

  /**
   * Returns distributed storage admin for ScalarDB admin operations
   *
   * @return distributed storage admin object
   */
  public DistributedStorageAdmin getDistributedStorageAdmin() {
    return storageAdmin;
  }
}
