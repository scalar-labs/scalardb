package com.scalar.db.storage.jdbc;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class ScannerImpl implements Scanner {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScannerImpl.class);

  private final SelectQuery selectQuery;
  private final Connection connection;
  private final PreparedStatement preparedStatement;
  private final ResultSet resultSet;

  private ScannerIterator scannerIterator;

  public ScannerImpl(
      SelectQuery selectQuery,
      Connection connection,
      PreparedStatement preparedStatement,
      ResultSet resultSet) {
    this.selectQuery = Objects.requireNonNull(selectQuery);
    this.connection = Objects.requireNonNull(connection);
    this.preparedStatement = Objects.requireNonNull(preparedStatement);
    this.resultSet = Objects.requireNonNull(resultSet);
  }

  @Override
  public Optional<Result> one() throws ExecutionException {
    try {
      if (resultSet.next()) {
        return Optional.of(selectQuery.getResult(resultSet));
      }
      return Optional.empty();
    } catch (SQLException e) {
      throw new ExecutionException("failed to fetch the next result", e);
    }
  }

  @Override
  public List<Result> all() throws ExecutionException {
    List<Result> ret = new ArrayList<>();
    while (true) {
      Optional<Result> one = one();
      if (one.isPresent()) {
        ret.add(one.get());
      } else {
        break;
      }
    }
    return ret;
  }

  @Override
  @Nonnull
  public Iterator<Result> iterator() {
    if (scannerIterator == null) {
      scannerIterator = new ScannerIterator(this);
    }
    return scannerIterator;
  }

  @Override
  public void close() throws IOException {
    try {
      resultSet.close();
    } catch (SQLException e) {
      LOGGER.warn("failed to close the resultSet", e);
    }
    try {
      preparedStatement.close();
    } catch (SQLException e) {
      LOGGER.warn("failed to close the preparedStatement", e);
    }
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.warn("failed to close the connection", e);
    }
  }
}
