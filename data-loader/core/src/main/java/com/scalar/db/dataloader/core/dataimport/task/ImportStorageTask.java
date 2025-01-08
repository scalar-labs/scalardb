package com.scalar.db.dataloader.core.dataimport.task;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Result;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDBDaoException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import java.util.*;

public class ImportStorageTask extends ImportTask {

  private final DistributedStorage storage;

  public ImportStorageTask(ImportTaskParams params, DistributedStorage storage) {
    super(params);
    this.storage = storage;
  }

  @Override
  protected Optional<Result> getDataRecord(
      String namespace, String tableName, Key partitionKey, Key clusteringKey)
      throws ScalarDBDaoException {
    return params.getDao().get(namespace, tableName, partitionKey, clusteringKey, this.storage);
  }

  @Override
  protected void saveRecord(
      String namespace,
      String tableName,
      Key partitionKey,
      Key clusteringKey,
      List<Column<?>> columns)
      throws ScalarDBDaoException {
    params.getDao().put(namespace, tableName, partitionKey, clusteringKey, columns, this.storage);
  }
}
