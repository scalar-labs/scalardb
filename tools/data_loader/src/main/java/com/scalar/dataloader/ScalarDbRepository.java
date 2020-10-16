package com.scalar.dataloader;

import static com.scalar.dataloader.common.Constants.ERROR_COMMITTING_TRANSACTION;
import static com.scalar.dataloader.common.Constants.ERROR_GETTING_DATA_FROM_DB;
import static com.scalar.dataloader.common.Constants.ERROR_INSERTING_DATA_TO_DB;
import static com.scalar.dataloader.common.Constants.ERROR_MATCHING_REGEX;
import static com.scalar.dataloader.common.Constants.ERROR_OBJECT_NOT_FOUND;
import static com.scalar.dataloader.common.Constants.ERROR_READING_PROPERTIES;

import com.google.inject.Guice;
import com.scalar.dataloader.common.Constants;
import com.scalar.dataloader.exception.DataLoaderException;
import com.scalar.dataloader.model.DataValue;
import com.scalar.dataloader.model.Rule;
import com.scalar.dataloader.model.TableMetadata;
import com.scalar.dataloader.model.TableType;
import com.scalar.dataloader.model.ValueType;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.service.StorageModule;
import com.scalar.db.service.StorageService;
import com.scalar.db.service.TransactionModule;
import com.scalar.db.service.TransactionService;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.bouncycastle.util.encoders.Base64;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ScalarDbRepository {
  private final TableMetadata table;
  private final JSONArray dataFileArray;
  private final DatabaseConfig scalarDbConfig;
  private DistributedStorage storage;
  private DistributedTransaction tx;
  private JSONObject dataFileObject;

  public ScalarDbRepository(Path properties, TableMetadata table, JSONArray dataFileArray) {
    this.table = table;
    this.dataFileArray = dataFileArray;
    try {
      InputStream inputStream = Files.newInputStream(properties);
      scalarDbConfig = new DatabaseConfig(inputStream);
    } catch (IOException e) {
      throw new DataLoaderException(ERROR_READING_PROPERTIES, e);
    }
  }

  public void loadDataIntoDb(Rule rule) {
    for (int i = 0; i < dataFileArray.length(); i++) {
      dataFileObject = dataFileArray.getJSONObject(i);
      Key pk = loadKeys(table.getPartitionsKeys());
      // There may be no clustering keys
      Key ck = table.getClusteringKeys().isEmpty() ? null : loadKeys(table.getClusteringKeys());
      if (rule.equals(Rule.insert)) {
        executePut(pk, ck);
      } else if (rule.equals(Rule.update)) {
        executeGet(pk, ck);
        executePut(pk, ck);
      } else if (rule.equals(Rule.delete)) {
        executeGet(pk, ck);
        executeDelete(pk, ck);
      }
    }
    if (table.getTableType().equals(TableType.TRANSACTION)) {
      try {
        getTx().commit();
      } catch (CommitException | UnknownTransactionStatusException e) {
        throw new DataLoaderException(ERROR_COMMITTING_TRANSACTION);
      }
    }
  }

  private void executeDelete(Key pk, @Nullable Key ck) {
    Delete delete =
        new Delete(pk, ck).forNamespace(table.getKeyspace()).forTable(table.getTableName());
    if (table.getTableType().equals(TableType.TRANSACTION)) {
      getTx().delete(delete);
    } else {
      try {
        getStorage().delete(delete);
      } catch (ExecutionException e) {
        throw new DataLoaderException(ERROR_INSERTING_DATA_TO_DB, e);
      }
    }
  }

  private void executeGet(Key pk, @Nullable Key ck) {
    Get get = new Get(pk, ck).forNamespace(table.getKeyspace()).forTable(table.getTableName());
    Optional<Result> result;
    try {
      if (table.getTableType().equals(TableType.TRANSACTION)) {
        result = getTx().get(get);
      } else {
        result = getStorage().get(get);
      }
      if (!result.isPresent()) {
        throw new DataLoaderException(String.format(ERROR_OBJECT_NOT_FOUND, get.toString()));
      }
    } catch (ExecutionException | CrudException e) {
      throw new DataLoaderException(ERROR_GETTING_DATA_FROM_DB, e);
    }
  }

  private void executePut(Key pk, @Nullable Key ck) {
    Put put = new Put(pk, ck);
    loadPut(put, table.getColumns());
    put.forNamespace(table.getKeyspace()).forTable(table.getTableName());
    if (table.getTableType().equals(TableType.TRANSACTION)) {
      getTx().put(put);
    } else {
      try {
        getStorage().put(put);
      } catch (ExecutionException e) {
        throw new DataLoaderException(ERROR_INSERTING_DATA_TO_DB, e);
      }
    }
  }

  private void loadPut(Put put, List<DataValue> dataValues) {
    List<Value> values = new ArrayList<>();

    loadKeyValues(values, dataValues);
    put.withValues(values);
  }

  private Key loadKeys(List<DataValue> dataValues) {
    List<Value> values = new ArrayList<>();

    loadKeyValues(values, dataValues);
    return new Key(values);
  }

  private void loadKeyValues(List<Value> values, List<DataValue> dataValues) {
    try {
      for (DataValue dataValue : dataValues) {
        String valueName = dataValue.getName();
        if (dataValue.getType().equals(ValueType.TEXT)) {
          if (dataValue.getPattern() != null) {
            validatePattern(dataValue.getPattern(), dataFileObject.getString(valueName));
          }
          values.add(new TextValue(valueName, dataFileObject.getString(valueName)));
        } else if (dataValue.getType().equals(ValueType.BIGINT)) {
          values.add(new BigIntValue(valueName, dataFileObject.getLong(valueName)));
        } else if (dataValue.getType().equals(ValueType.INT)) {
          values.add(new IntValue(valueName, dataFileObject.getInt(valueName)));
        } else if (dataValue.getType().equals(ValueType.BOOLEAN)) {
          values.add(new BooleanValue(valueName, dataFileObject.getBoolean(valueName)));
        } else if (dataValue.getType().equals(ValueType.DOUBLE)) {
          values.add(new DoubleValue(valueName, dataFileObject.getDouble(valueName)));
        } else if (dataValue.getType().equals(ValueType.FLOAT)) {
          values.add(new FloatValue(valueName, dataFileObject.getFloat(valueName)));
        } else if (dataValue.getType().equals(ValueType.BLOB)) {
          values.add(new BlobValue(valueName, Base64.decode(dataFileObject.getString(valueName))));
        }
      }
    } catch (JSONException e) {
      throw new DataLoaderException(Constants.ERROR_READING_DATA_FILE_VALUE, e);
    }
  }

  private void validatePattern(String regexPattern, String value) {
    if (!value.matches(regexPattern)) {
      throw new DataLoaderException(String.format(ERROR_MATCHING_REGEX, value, regexPattern));
    }
  }

  private DistributedStorage getStorage() {
    // Lazy loading
    if (storage == null) {
      storage =
          Guice.createInjector(new StorageModule(scalarDbConfig)).getInstance(StorageService.class);
    }
    return storage;
  }

  private DistributedTransaction getTx() {
    // lazy loading
    if (tx == null) {
      tx =
          Guice.createInjector(new TransactionModule(scalarDbConfig))
              .getInstance(TransactionService.class)
              .start();
    }
    return tx;
  }
}
