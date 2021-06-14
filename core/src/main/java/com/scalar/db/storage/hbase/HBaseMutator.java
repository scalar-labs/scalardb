package com.scalar.db.storage.hbase;

import com.google.protobuf.ServiceException;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.storage.hbase.query.Query;
import com.scalar.db.storage.hbase.query.QueryBuilder;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MultiRowMutationService;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsResponse;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.index.IndexMetaDataCacheClient;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO refactor
public class HBaseMutator {
  private static final Logger LOGGER = LoggerFactory.getLogger(HBaseMutator.class);

  // TODO it's dynamic actually
  private static final byte[] DUMMY_COLUMN_FAMILY = Bytes.toBytes("0");

  private static final byte[] DUMMY_COLUMN_QUALIFIER = Bytes.toBytes(0);
  private static final byte[] DUMMY_COLUMN_VALUE = Bytes.toBytes("x");

  private final HBaseConnection hbaseConnection;
  private final QueryBuilder queryBuilder;

  public HBaseMutator(HBaseConnection hbaseConnection, QueryBuilder queryBuilder) {
    this.hbaseConnection = Objects.requireNonNull(hbaseConnection);
    this.queryBuilder = Objects.requireNonNull(queryBuilder);
  }

  public void mutate(List<? extends Mutation> mutations) throws ExecutionException {
    PhoenixConnection connection = null;
    try {
      connection = hbaseConnection.getConnection();
      connection.setAutoCommit(false);

      MutateRowsRequest.Builder requestBuilder = MutateRowsRequest.newBuilder();

      byte[] hbaseTable = null;
      List<org.apache.hadoop.hbase.client.Mutation> hbaseMutations = new ArrayList<>();
      for (Mutation mutation : mutations) {
        Pair<byte[], org.apache.hadoop.hbase.client.Mutation> hBaseTableNameAndMutation =
            getHBaseTableNameAndMutation(connection, mutation);
        hbaseTable = hBaseTableNameAndMutation.getFirst();
        org.apache.hadoop.hbase.client.Mutation hbaseMutation =
            hBaseTableNameAndMutation.getSecond();
        hbaseMutations.add(hbaseMutation);

        if (mutation.getCondition().isPresent()) {
          addCondition(connection, mutation, hbaseMutation, requestBuilder);
        }
      }
      assert hbaseTable != null;
      addAttributes(connection, hbaseTable, hbaseMutations);

      for (org.apache.hadoop.hbase.client.Mutation hbaseMutation : hbaseMutations) {
        if (hbaseMutation instanceof org.apache.hadoop.hbase.client.Put) {
          requestBuilder.addMutationRequest(
              ProtobufUtil.toMutation(MutationType.PUT, hbaseMutation));
        } else {
          requestBuilder.addMutationRequest(
              ProtobufUtil.toMutation(MutationType.DELETE, hbaseMutation));
        }
      }

      try (Table table = connection.getQueryServices().getTable(hbaseTable)) {
        CoprocessorRpcChannel channel = table.coprocessorService(hbaseMutations.get(0).getRow());
        MultiRowMutationService.BlockingInterface service =
            MultiRowMutationService.newBlockingStub(channel);
        MutateRowsResponse response = service.mutateRows(null, requestBuilder.build());
        if (!response.getProcessed()) {
          throw new NoMutationException("no mutation was applied");
        }
      }
    } catch (SQLException | IOException | ServiceException e) {
      throw new ExecutionException("failed to execute the mutations", e);
    } finally {
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        LOGGER.warn("failed to close the connection", e);
      }
    }
  }

  private Pair<byte[], org.apache.hadoop.hbase.client.Mutation> getHBaseTableNameAndMutation(
      PhoenixConnection connection, Mutation mutation) throws ExecutionException {
    Query query;
    if (mutation instanceof Put) {
      Put put = (Put) mutation;
      query =
          queryBuilder
              .upsertInto(put.forFullNamespace().get(), put.forTable().get())
              .values(put.getPartitionKey(), put.getClusteringKey(), put.getValues())
              .build();
    } else {
      Delete delete = (Delete) mutation;
      query =
          queryBuilder
              .deleteFrom(delete.forFullNamespace().get(), delete.forTable().get())
              .where(delete.getPartitionKey(), delete.getClusteringKey())
              .build();
    }

    return getHBaseTableNameAndMutation(connection, query);
  }

  private Pair<byte[], org.apache.hadoop.hbase.client.Mutation> getHBaseTableNameAndMutation(
      PhoenixConnection connection, Mutation mutation, ConditionalExpression expression)
      throws ExecutionException {
    Query query =
        queryBuilder
            .upsertInto(mutation.forFullNamespace().get(), mutation.forTable().get())
            .values(
                mutation.getPartitionKey(),
                mutation.getClusteringKey(),
                Collections.singletonMap(expression.getName(), expression.getValue()))
            .build();

    return getHBaseTableNameAndMutation(connection, query);
  }

  private Pair<byte[], org.apache.hadoop.hbase.client.Mutation> getHBaseTableNameAndMutation(
      PhoenixConnection connection, Query query) throws ExecutionException {
    try (PreparedStatement preparedStatement = query.prepareAndBind(connection)) {
      preparedStatement.execute();
      final Iterator<Pair<byte[], List<org.apache.hadoop.hbase.client.Mutation>>> iterator =
          connection.getMutationState().toMutations(false);

      byte[] tableName = null;
      org.apache.hadoop.hbase.client.Mutation hbaseMutation = null;
      while (iterator.hasNext()) {
        Pair<byte[], List<org.apache.hadoop.hbase.client.Mutation>> kvPair = iterator.next();
        tableName = kvPair.getFirst();

        // TODO could be multiple hbase mutations?
        hbaseMutation = kvPair.getSecond().get(0);
      }
      connection.rollback();

      if (tableName == null && hbaseMutation == null) {
        throw new ExecutionException("failed to get the table name and the HBase mutation");
      }
      return new Pair<>(tableName, hbaseMutation);
    } catch (SQLException e) {
      throw new ExecutionException("failed to get the table name and the HBase mutation", e);
    }
  }

  private void addAttributes(
      PhoenixConnection connection,
      byte[] hbaseTableName,
      List<? extends org.apache.hadoop.hbase.client.Mutation> hbaseMutations)
      throws ExecutionException {
    try {
      PTable ptable = connection.getTable(new PTableKey(null, Bytes.toString(hbaseTableName)));
      ImmutableBytesWritable indexMetaDataPtr = new ImmutableBytesWritable();
      ptable.getIndexMaintainers(indexMetaDataPtr, connection);
      IndexMetaDataCacheClient.setMetaDataOnMutations(
          connection, ptable, hbaseMutations, indexMetaDataPtr);
    } catch (SQLException e) {
      throw new ExecutionException("failed to add the attributes to the mutations", e);
    }
  }

  private void addCondition(
      PhoenixConnection connection,
      Mutation mutation,
      org.apache.hadoop.hbase.client.Mutation hbaseMutation,
      MutateRowsRequest.Builder requestBuilder)
      throws ExecutionException {
    try {
      MutationCondition condition = mutation.getCondition().get();
      if (condition instanceof PutIf || condition instanceof DeleteIf) {
        for (ConditionalExpression expression : condition.getExpressions()) {
          Cell conditionCell = getConditionCell(connection, mutation, expression);
          requestBuilder.addCondition(
              ProtobufUtil.toCondition(
                  hbaseMutation.getRow(),
                  CellUtil.cloneFamily(conditionCell),
                  CellUtil.cloneQualifier(conditionCell),
                  toCompareOperator(expression.getOperator()),
                  CellUtil.cloneValue(conditionCell),
                  TimeRange.allTime()));
        }
      } else if (condition instanceof PutIfExists || condition instanceof DeleteIfExists) {
        requestBuilder.addCondition(
            ProtobufUtil.toCondition(
                hbaseMutation.getRow(),
                DUMMY_COLUMN_FAMILY,
                DUMMY_COLUMN_QUALIFIER,
                CompareOperator.EQUAL,
                DUMMY_COLUMN_VALUE,
                TimeRange.allTime()));
      } else { // PutIfNotExists
        requestBuilder.addCondition(
            ProtobufUtil.toCondition(
                hbaseMutation.getRow(),
                DUMMY_COLUMN_FAMILY,
                DUMMY_COLUMN_QUALIFIER,
                CompareOperator.EQUAL,
                null,
                TimeRange.allTime()));
      }
    } catch (IOException e) {
      throw new ExecutionException("failed to add condition", e);
    }
  }

  private Cell getConditionCell(
      PhoenixConnection connection, Mutation mutation, ConditionalExpression expression)
      throws ExecutionException {

    // TODO Better if we can create a conditionCell directly, not from UPSERT query

    org.apache.hadoop.hbase.client.Mutation conditionPut =
        getHBaseTableNameAndMutation(connection, mutation, expression).getSecond();

    for (Map.Entry<byte[], List<Cell>> entry : conditionPut.getFamilyCellMap().entrySet()) {
      for (Cell cell : entry.getValue()) {
        if (!Bytes.equals(CellUtil.cloneQualifier(cell), DUMMY_COLUMN_QUALIFIER)) {
          return cell;
        }
      }
    }
    throw new ExecutionException("failed to get the condition cell");
  }

  private CompareOperator toCompareOperator(ConditionalExpression.Operator operator) {
    switch (operator) {
      case EQ:
        return CompareOperator.EQUAL;
      case NE:
        return CompareOperator.NOT_EQUAL;
      case GT:
        return CompareOperator.GREATER;
      case GTE:
        return CompareOperator.GREATER_OR_EQUAL;
      case LT:
        return CompareOperator.LESS;
      case LTE:
        return CompareOperator.LESS_OR_EQUAL;
      default:
        throw new AssertionError();
    }
  }
}
