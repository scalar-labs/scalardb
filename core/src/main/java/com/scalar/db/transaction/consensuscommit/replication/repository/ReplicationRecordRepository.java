package com.scalar.db.transaction.consensuscommit.replication.repository;

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
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.transaction.consensuscommit.replication.model.Record;
import com.scalar.db.transaction.consensuscommit.replication.model.Record.Value;
import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationRecordRepository {
  private static final Logger logger = LoggerFactory.getLogger(ReplicationRecordRepository.class);
  private static final TypeReference<Set<Value>> typeRefForValueInRecords =
      new TypeReference<Set<Value>>() {};

  private final DistributedStorage replicationDbStorage;
  private final ObjectMapper objectMapper;
  private final String replicationDbNamespace;
  private final String replicationDbRecordsTable;

  public ReplicationRecordRepository(
      DistributedStorage replicationDbStorage,
      ObjectMapper objectMapper,
      String replicationDbNamespace,
      String replicationDbRecordsTable) {
    this.replicationDbStorage = replicationDbStorage;
    this.objectMapper = objectMapper;
    this.replicationDbNamespace = replicationDbNamespace;
    this.replicationDbRecordsTable = replicationDbRecordsTable;
  }

  public Key createKey(
      String namespace,
      String table,
      com.scalar.db.transaction.consensuscommit.replication.model.Key partitionKey,
      com.scalar.db.transaction.consensuscommit.replication.model.Key clusteringKey)
      throws JsonProcessingException {
    return Key.newBuilder()
        .addText("namespace", namespace)
        .addText("table", table)
        .addText("pk", objectMapper.writeValueAsString(partitionKey))
        .addText("ck", objectMapper.writeValueAsString(clusteringKey))
        .build();
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

    logger.info("[get] key:{}\n  result:{}", key, result);

    return result.flatMap(
        r -> {
          try {
            return Optional.of(
                new Record(
                    Objects.requireNonNull(r.getText("namespace")),
                    Objects.requireNonNull(r.getText("table")),
                    objectMapper.readValue(
                        r.getText("pk"),
                        com.scalar.db.transaction.consensuscommit.replication.model.Key.class),
                    objectMapper.readValue(
                        r.getText("ck"),
                        com.scalar.db.transaction.consensuscommit.replication.model.Key.class),
                    r.getText("current_tx_id"),
                    objectMapper.readValue(r.getText("values"), typeRefForValueInRecords),
                    Instant.ofEpochMilli(r.getBigInt("appended_at")),
                    Instant.ofEpochMilli(r.getBigInt("shrinked_at"))));
          } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize `values` for key:" + key, e);
          }
        });
  }

  // TODO: Maybe `txId` should be replaced with `version`
  public void appendValue(Key key, Value newValue) throws ExecutionException {
    Optional<Record> recordOpt = get(key);

    Buildable putBuilder =
        Put.newBuilder()
            .namespace(replicationDbNamespace)
            .table(replicationDbRecordsTable)
            .partitionKey(key);

    Set<Value> values;
    if (recordOpt.isPresent()) {
      Record record = recordOpt.get();
      // FIXME: Use a condition using `version`
      values = record.values();
      if (record.currentTxId() == null) {
        putBuilder.condition(
            ConditionBuilder.putIf(
                    ConditionBuilder.buildConditionalExpression(
                        TextColumn.of("current_tx_id", null), Operator.IS_NULL))
                .build());
      } else {
        putBuilder.condition(
            ConditionBuilder.putIf(
                    ConditionBuilder.buildConditionalExpression(
                        TextColumn.of("current_tx_id", recordOpt.get().currentTxId()), Operator.EQ))
                .build());
      }
    } else {
      values = new HashSet<>();
      putBuilder.condition(ConditionBuilder.putIfNotExists());
    }

    if (!values.add(newValue)) {
      logger.warn("The new value is already stored. key:{}, txId:{}", key, newValue.txId);
    }

    try {
      logger.info("[appendValue] key:{}\n  values={}", key, values);
      replicationDbStorage.put(
          putBuilder.textValue("values", objectMapper.writeValueAsString(values)).build());
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize `values` for key:" + key, e);
    }
  }

  // TODO: Maybe `txId` should be replaced with `version`
  public void updateValues(Key key, String currentTxId, String newTxId, Collection<Value> values)
      throws ExecutionException {
    Buildable putBuilder =
        Put.newBuilder()
            .namespace(replicationDbNamespace)
            .table(replicationDbRecordsTable)
            .partitionKey(key);

    if (currentTxId == null) {
      putBuilder.condition(
          ConditionBuilder.putIf(
                  ConditionBuilder.buildConditionalExpression(
                      TextColumn.of("current_tx_id", null), Operator.IS_NULL))
              .build());
    } else {
      putBuilder.condition(
          ConditionBuilder.putIf(
                  ConditionBuilder.buildConditionalExpression(
                      TextColumn.of("current_tx_id", currentTxId), Operator.EQ))
              .build());
    }

    try {
      logger.info(
          "[updateValue] key:{}\n  currentTxId:{}\n  newTxId:{}\n  values={}",
          key,
          currentTxId,
          newTxId,
          values);
      replicationDbStorage.put(
          putBuilder
              .textValue("values", objectMapper.writeValueAsString(values))
              .textValue("current_tx_id", newTxId)
              .build());
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize `values` for key:" + key, e);
    }
  }
}
