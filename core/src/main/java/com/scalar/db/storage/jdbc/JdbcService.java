package com.scalar.db.storage.jdbc;

import static com.google.common.base.Preconditions.checkArgument;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.checker.OperationChecker;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.jdbc.query.DeleteQuery;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A service class to perform get/scan/put/delete/mutate operations.
 *
 * @author Toshihiro Suzuki
 */
@SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
@ThreadSafe
public class JdbcService {

  private final TableMetadataManager tableMetadataManager;
  private final OperationChecker operationChecker;
  private final QueryBuilder queryBuilder;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public JdbcService(
      TableMetadataManager tableMetadataManager,
      OperationChecker operationChecker,
      QueryBuilder queryBuilder) {
    this.tableMetadataManager = Objects.requireNonNull(tableMetadataManager);
    this.operationChecker = Objects.requireNonNull(operationChecker);
    this.queryBuilder = Objects.requireNonNull(queryBuilder);
  }

  public Optional<Result> get(Get get, Connection connection)
      throws SQLException, ExecutionException {
    operationChecker.check(get);
    TableMetadata tableMetadata = tableMetadataManager.getTableMetadata(get);

    SelectQuery selectQuery =
        queryBuilder
            .select(get.getProjections())
            .from(get.forNamespace().get(), get.forTable().get(), tableMetadata)
            .where(get.getPartitionKey(), get.getClusteringKey())
            .build();

    try (PreparedStatement preparedStatement = connection.prepareStatement(selectQuery.sql())) {
      selectQuery.bind(preparedStatement);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        if (resultSet.next()) {
          Optional<Result> ret =
              Optional.of(
                  new ResultInterpreter(get.getProjections(), tableMetadata).interpret(resultSet));
          if (resultSet.next()) {
            throw new IllegalArgumentException("Please use scan() for non-exact match selection");
          }
          return ret;
        }
        return Optional.empty();
      }
    }
  }

  @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE")
  public Scanner getScanner(Scan scan, Connection connection)
      throws SQLException, ExecutionException {
    operationChecker.check(scan);

    TableMetadata tableMetadata = tableMetadataManager.getTableMetadata(scan);

    SelectQuery selectQuery = buildSelectQuery(scan, tableMetadata);
    PreparedStatement preparedStatement = connection.prepareStatement(selectQuery.sql());
    selectQuery.bind(preparedStatement);
    ResultSet resultSet = preparedStatement.executeQuery();
    return new ScannerImpl(
        new ResultInterpreter(scan.getProjections(), tableMetadata),
        connection,
        preparedStatement,
        resultSet);
  }

  public List<Result> scan(Scan scan, Connection connection)
      throws SQLException, ExecutionException {
    operationChecker.check(scan);

    TableMetadata tableMetadata = tableMetadataManager.getTableMetadata(scan);

    SelectQuery selectQuery = buildSelectQuery(scan, tableMetadata);
    try (PreparedStatement preparedStatement = connection.prepareStatement(selectQuery.sql())) {
      selectQuery.bind(preparedStatement);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        List<Result> ret = new ArrayList<>();
        ResultInterpreter resultInterpreter =
            new ResultInterpreter(scan.getProjections(), tableMetadata);
        while (resultSet.next()) {
          ret.add(resultInterpreter.interpret(resultSet));
        }
        return ret;
      }
    }
  }

  private SelectQuery buildSelectQuery(Scan scan, TableMetadata tableMetadata) {
    if (scan instanceof ScanAll) {
      ScanAll scanAll = (ScanAll) scan;
      if (ScalarDbUtils.isRelational(scan)) {
        return buildSelectQueryForRelationalScan(scanAll, tableMetadata);
      } else {
        return buildSelectQueryForScanAll(scanAll, tableMetadata);
      }
    } else {
      return buildSelectQueryForScan(scan, tableMetadata);
    }
  }

  private SelectQuery buildSelectQueryForScan(Scan scan, TableMetadata tableMetadata) {
    return queryBuilder
        .select(scan.getProjections())
        .from(scan.forNamespace().get(), scan.forTable().get(), tableMetadata)
        .where(
            scan.getPartitionKey(),
            scan.getStartClusteringKey(),
            scan.getStartInclusive(),
            scan.getEndClusteringKey(),
            scan.getEndInclusive())
        .orderBy(scan.getOrderings())
        .limit(scan.getLimit())
        .build();
  }

  private SelectQuery buildSelectQueryForScanAll(ScanAll scanAll, TableMetadata tableMetadata) {
    return queryBuilder
        .select(scanAll.getProjections())
        .from(scanAll.forNamespace().get(), scanAll.forTable().get(), tableMetadata)
        .limit(scanAll.getLimit())
        .build();
  }

  private SelectQuery buildSelectQueryForRelationalScan(ScanAll scan, TableMetadata tableMetadata) {
    return queryBuilder
        .select(scan.getProjections())
        .from(scan.forNamespace().get(), scan.forTable().get(), tableMetadata)
        .where(scan.getConjunctions())
        .orderBy(scan.getOrderings())
        .limit(scan.getLimit())
        .build();
  }

  public boolean put(Put put, Connection connection) throws SQLException, ExecutionException {
    operationChecker.check(put);
    return putInternal(put, connection);
  }

  private boolean putInternal(Put put, Connection connection)
      throws SQLException, ExecutionException {
    TableMetadata tableMetadata = tableMetadataManager.getTableMetadata(put);

    if (!put.getCondition().isPresent()) {
      UpsertQuery upsertQuery =
          queryBuilder
              .upsertInto(put.forNamespace().get(), put.forTable().get(), tableMetadata)
              .values(put.getPartitionKey(), put.getClusteringKey(), put.getColumns())
              .build();
      try (PreparedStatement preparedStatement = connection.prepareStatement(upsertQuery.sql())) {
        upsertQuery.bind(preparedStatement);
        preparedStatement.executeUpdate();
        return true;
      }
    } else {
      return new ConditionalMutator(put, tableMetadata, connection, queryBuilder).mutate();
    }
  }

  public boolean delete(Delete delete, Connection connection)
      throws SQLException, ExecutionException {
    operationChecker.check(delete);
    return deleteInternal(delete, connection);
  }

  private boolean deleteInternal(Delete delete, Connection connection)
      throws SQLException, ExecutionException {
    TableMetadata tableMetadata = tableMetadataManager.getTableMetadata(delete);

    if (!delete.getCondition().isPresent()) {
      DeleteQuery deleteQuery =
          queryBuilder
              .deleteFrom(delete.forNamespace().get(), delete.forTable().get(), tableMetadata)
              .where(delete.getPartitionKey(), delete.getClusteringKey())
              .build();
      try (PreparedStatement preparedStatement = connection.prepareStatement(deleteQuery.sql())) {
        deleteQuery.bind(preparedStatement);
        preparedStatement.executeUpdate();
        return true;
      }
    } else {
      return new ConditionalMutator(delete, tableMetadata, connection, queryBuilder).mutate();
    }
  }

  public boolean mutate(List<? extends Mutation> mutations, Connection connection)
      throws SQLException, ExecutionException {
    checkArgument(mutations.size() != 0);
    operationChecker.check(mutations);

    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        if (!putInternal((Put) mutation, connection)) {
          return false;
        }
      } else {
        assert mutation instanceof Delete;
        if (!deleteInternal((Delete) mutation, connection)) {
          return false;
        }
      }
    }
    return true;
  }
}
