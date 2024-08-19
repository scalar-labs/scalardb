package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder.Buildable;
import com.scalar.db.api.Result;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.Utils;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record.Value;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationRecordRepository {
  private static final Logger logger = LoggerFactory.getLogger(ReplicationRecordRepository.class);
  private static final TypeReference<Set<Value>> typeRefForValueInRecords =
      new TypeReference<Set<Value>>() {};
  private static final TypeReference<Set<String>> typeRefForInsertTxIdsInRecords =
      new TypeReference<Set<String>>() {};

  private final DistributedStorage replicationDbStorage;
  private final ObjectMapper objectMapper;
  private final String replicationDbNamespace;
  private final String replicationDbRecordsTable;
  private final String emptySet;

  public ReplicationRecordRepository(
      DistributedStorage replicationDbStorage,
      ObjectMapper objectMapper,
      String replicationDbNamespace,
      String replicationDbRecordsTable) {
    this.replicationDbStorage = replicationDbStorage;
    this.objectMapper = objectMapper;
    this.replicationDbNamespace = replicationDbNamespace;
    this.replicationDbRecordsTable = replicationDbRecordsTable;
    try {
      emptySet = objectMapper.writeValueAsString(Collections.EMPTY_SET);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize an empty set");
    }
  }

  public Key createKey(
      String namespace,
      String table,
      com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Key partitionKey,
      com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Key clusteringKey) {
    try {
      return Key.newBuilder()
          .addText("namespace", namespace)
          .addText("table", table)
          .addText("pk", objectMapper.writeValueAsString(partitionKey))
          .addText("ck", objectMapper.writeValueAsString(clusteringKey))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          String.format(
              "Failed to create a key for record. namespace:%s, table:%s, pk:%s, ck:%s",
              namespace, table, partitionKey, clusteringKey));
    }
  }

  public Optional<Record> get(Key key) throws ExecutionException {
    Optional<Result> result =
        replicationDbStorage.get(
            Get.newBuilder()
                .namespace(replicationDbNamespace)
                .table(replicationDbRecordsTable)
                // TODO: Provision for performance
                .partitionKey(key)
                .build());

    logger.debug(
        "[get]\n  key:{}\n  record:{}\n",
        key,
        Utils.convOptReplDbRecordsTableResultMetadataToString(result));

    return result.flatMap(
        r -> {
          try {
            Record record =
                new Record(
                    Objects.requireNonNull(r.getText("namespace")),
                    Objects.requireNonNull(r.getText("table")),
                    objectMapper.readValue(
                        r.getText("pk"),
                        com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Key
                            .class),
                    objectMapper.readValue(
                        r.getText("ck"),
                        com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Key
                            .class),
                    r.getBigInt("version"),
                    r.getText("current_tx_id"),
                    r.getText("prep_tx_id"),
                    r.getBoolean("deleted"),
                    objectMapper.readValue(r.getText("values"), typeRefForValueInRecords),
                    objectMapper.readValue(
                        r.getText("insert_tx_ids"), typeRefForInsertTxIdsInRecords),
                    Instant.ofEpochMilli(r.getBigInt("appended_at")),
                    Instant.ofEpochMilli(r.getBigInt("shrinked_at")));

            logger.debug(
                "[get(details)]\n  key:{}\n  record:{}\n", key, record.toStringOnlyWithMetadata());

            return Optional.of(record);
          } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize `values` for key:" + key, e);
          }
        });
  }

  private void setCasCondition(Buildable buildable, Optional<Record> existingRecord) {
    if (existingRecord.isPresent()) {
      long currentVersion = existingRecord.get().version;
      buildable.value(BigIntColumn.of("version", currentVersion + 1));
      buildable.condition(
          ConditionBuilder.putIf(
                  ConditionBuilder.buildConditionalExpression(
                      BigIntColumn.of("version", currentVersion), Operator.EQ))
              .build());
    } else {
      buildable.value(BigIntColumn.of("version", 1));
      buildable.condition(ConditionBuilder.putIfNotExists());
    }
  }

  public long nextVersion(Optional<Record> recordOpt) {
    // This logic must be always along with the one in `setCasCondition()`.
    return recordOpt.map(record -> record.version + 1).orElse(1L);
  }

  public void upsertWithNewValue(Key key, Optional<Record> recordOpt, Value newValue)
      throws ExecutionException {
    Buildable putBuilder =
        Put.newBuilder()
            .namespace(replicationDbNamespace)
            .table(replicationDbRecordsTable)
            .partitionKey(key);
    setCasCondition(putBuilder, recordOpt);

    Set<Value> values = new HashSet<>();
    recordOpt.ifPresent(record -> values.addAll(record.values));
    if (!values.add(newValue)) {
      logger.warn("The new value is already stored. key:{}, txId:{}", key, newValue.txId);
    }

    if (!recordOpt.isPresent()) {
      putBuilder.textValue("insert_tx_ids", emptySet);
    }

    try {
      logger.debug(
          "[upsertWithNewValue]\n  key:{}\n  values=[{}]\n", key, Utils.convValuesToString(values));

      replicationDbStorage.put(
          putBuilder.textValue("values", objectMapper.writeValueAsString(values)).build());
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize `values` for key:" + key, e);
    }
  }

  public void updateWithValues(
      Key key,
      Record record,
      @Nullable String newTxId,
      boolean deleted,
      Collection<Value> values,
      Collection<String> newInsertions)
      throws ExecutionException {
    Buildable putBuilder =
        Put.newBuilder()
            .namespace(replicationDbNamespace)
            .table(replicationDbRecordsTable)
            .partitionKey(key);
    setCasCondition(putBuilder, Optional.of(record));

    if (newTxId != null && newTxId.equals(record.currentTxId)) {
      logger.warn("`tx_id` isn't changed. old:{}, new:{}", record.currentTxId, newTxId);
    }

    if (!newInsertions.isEmpty()) {
      Set<String> insertedTxIds = new HashSet<>();
      insertedTxIds.addAll(record.insertTxIds);
      insertedTxIds.addAll(newInsertions);
      try {
        putBuilder.textValue("insert_tx_ids", objectMapper.writeValueAsString(insertedTxIds));
      } catch (JsonProcessingException e) {
        throw new RuntimeException("Failed to serialize `insert_tx_ids` for key:" + key, e);
      }
    }

    try {
      logger.debug(
          "[updateWithValues]\n  key:{}\n  record:{}\n  newTxId:{}\n  values=[{}]\n",
          key,
          record.toStringOnlyWithMetadata(),
          newTxId,
          values);

      replicationDbStorage.put(
          putBuilder
              .textValue("values", objectMapper.writeValueAsString(values))
              .textValue("current_tx_id", newTxId)
              // Clear `prep_tx_id` for subsequent transactions
              .textValue("prep_tx_id", null)
              .booleanValue("deleted", deleted)
              .build());
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize `values` for key:" + key, e);
    }
  }

  public void updateWithPrepTxId(Key key, Record record, String prepTxId)
      throws ExecutionException {
    long currentVersion = record.version;
    Buildable putBuilder =
        Put.newBuilder()
            .namespace(replicationDbNamespace)
            .table(replicationDbRecordsTable)
            .partitionKey(key);
    putBuilder.condition(
        ConditionBuilder.putIf(
            Arrays.asList(
                ConditionBuilder.buildConditionalExpression(
                    BigIntColumn.of("version", currentVersion), Operator.EQ),
                ConditionBuilder.buildConditionalExpression(
                    TextColumn.of("prep_tx_id", null), Operator.IS_NULL))));

    logger.debug(
        "[updatePrepTxId]\n  key:{}\n  record:{}\n  prepTxId:{}\n",
        key,
        record.toStringOnlyWithMetadata(),
        prepTxId);

    replicationDbStorage.put(putBuilder.textValue("prep_tx_id", prepTxId).build());
  }
}
