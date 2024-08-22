package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.UpdatedRecord;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationUpdatedRecordRepository {
  private static final Logger logger =
      LoggerFactory.getLogger(ReplicationUpdatedRecordRepository.class);

  private final DistributedStorage replicationDbStorage;
  private final ObjectMapper objectMapper;
  private final String replicationDbNamespace;
  private final String replicationDbUpdatedRecordTable;

  public ReplicationUpdatedRecordRepository(
      DistributedStorage replicationDbStorage,
      ObjectMapper objectMapper,
      String replicationDbNamespace,
      String replicationDbUpdatedRecordTable) {
    this.replicationDbStorage = replicationDbStorage;
    this.objectMapper = objectMapper;
    this.replicationDbNamespace = replicationDbNamespace;
    this.replicationDbUpdatedRecordTable = replicationDbUpdatedRecordTable;
  }

  public List<UpdatedRecord> scan(
      int partitionId, int fetchSize, long skipRecentDataThresholdMillis)
      throws ExecutionException, IOException {
    try (Scanner scan =
        replicationDbStorage.scan(
            Scan.newBuilder()
                .namespace(replicationDbNamespace)
                .table(replicationDbUpdatedRecordTable)
                .partitionKey(Key.ofInt("partition_id", partitionId))
                .ordering(Ordering.asc("updated_at"))
                .limit(fetchSize)
                .end(
                    Key.ofBigInt(
                        "updated_at", System.currentTimeMillis() - skipRecentDataThresholdMillis))
                .build())) {
      List<UpdatedRecord> updatedRecords =
          scan.all().stream()
              .map(
                  result -> {
                    try {
                      return new UpdatedRecord(
                          partitionId,
                          result.getText("namespace"),
                          result.getText("table"),
                          objectMapper.readValue(
                              result.getText("pk"),
                              com.scalar.db.transaction.consensuscommit.replication.semisyncrepl
                                  .model.Key.class),
                          objectMapper.readValue(
                              result.getText("ck"),
                              com.scalar.db.transaction.consensuscommit.replication.semisyncrepl
                                  .model.Key.class),
                          Instant.ofEpochMilli(result.getBigInt("updated_at")),
                          result.getBigInt("version"));
                    } catch (JsonProcessingException e) {
                      throw new RuntimeException(e);
                    }
                  })
              .collect(Collectors.toList());

      if (!updatedRecords.isEmpty()) {
        logger.debug(
            "[scan]\n  partitionId:{}\n  records:{}\n",
            partitionId,
            updatedRecords.stream().map(UpdatedRecord::toString).collect(Collectors.joining(",")));
      }

      return updatedRecords;
    }
  }

  private Put createPutFromUpdatedRecord(UpdatedRecord updatedRecord) {
    try {
      return Put.newBuilder()
          .namespace(replicationDbNamespace)
          .table(replicationDbUpdatedRecordTable)
          .partitionKey(Key.ofInt("partition_id", updatedRecord.partitionId))
          .clusteringKey(
              Key.newBuilder()
                  .addBigInt("updated_at", updatedRecord.updatedAt.toEpochMilli())
                  .addText("namespace", updatedRecord.namespace)
                  .addText("table", updatedRecord.table)
                  .addText("pk", objectMapper.writeValueAsString(updatedRecord.pk))
                  .addText("ck", objectMapper.writeValueAsString(updatedRecord.ck))
                  .addBigInt("version", updatedRecord.version)
                  .build())
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          String.format(
              "Failed to create the updated record information. UpdatedRecord:%s", updatedRecord),
          e);
    }
  }

  public void add(UpdatedRecord updatedRecord) throws ExecutionException {
    logger.debug("[add]\n  record:{}\n", updatedRecord);

    replicationDbStorage.put(createPutFromUpdatedRecord(updatedRecord));
  }

  public void delete(UpdatedRecord updatedRecord) throws ExecutionException {
    try {
      logger.debug("[delete]\n  record:{}\n", updatedRecord);

      replicationDbStorage.delete(
          Delete.newBuilder()
              .namespace(replicationDbNamespace)
              .table(replicationDbUpdatedRecordTable)
              .partitionKey(Key.ofInt("partition_id", updatedRecord.partitionId))
              .clusteringKey(
                  Key.newBuilder()
                      .addBigInt("updated_at", updatedRecord.updatedAt.toEpochMilli())
                      .addText("namespace", updatedRecord.namespace)
                      .addText("table", updatedRecord.table)
                      .addText("pk", objectMapper.writeValueAsString(updatedRecord.pk))
                      .addText("ck", objectMapper.writeValueAsString(updatedRecord.ck))
                      .addBigInt("version", updatedRecord.version)
                      .build())
              .build());
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          String.format(
              "Failed to delete the updated record information. UpdatedRecord:%s", updatedRecord),
          e);
    }
  }

  public UpdatedRecord updateUpdatedAt(UpdatedRecord updatedRecord) throws ExecutionException {
    UpdatedRecord newUpdatedRecord =
        new UpdatedRecord(
            updatedRecord.partitionId,
            updatedRecord.namespace,
            updatedRecord.table,
            updatedRecord.pk,
            updatedRecord.ck,
            Instant.now(),
            updatedRecord.version);

    logger.debug("[updateUpdatedAt]\n  updatedRecord:{}\n", updatedRecord);

    add(newUpdatedRecord);
    // It's okay if deleting the old record remains
    delete(updatedRecord);

    return newUpdatedRecord;
  }
}
