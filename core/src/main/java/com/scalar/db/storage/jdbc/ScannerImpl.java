package com.scalar.db.storage.jdbc;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scanner;
import com.scalar.db.common.ScannerIterator;
import com.scalar.db.exception.storage.ExecutionException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
  private static final Logger logger = LoggerFactory.getLogger(ScannerImpl.class);

  private final ResultInterpreter resultInterpreter;
  private final Connection connection;
  private final PreparedStatement preparedStatement;
  private final ResultSet resultSet;

  private ScannerIterator scannerIterator;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public ScannerImpl(
      ResultInterpreter resultInterpreter,
      Connection connection,
      PreparedStatement preparedStatement,
      ResultSet resultSet) {
    this.resultInterpreter = Objects.requireNonNull(resultInterpreter);
    this.connection = Objects.requireNonNull(connection);
    this.preparedStatement = Objects.requireNonNull(preparedStatement);
    this.resultSet = Objects.requireNonNull(resultSet);
  }

  @Override
  public Optional<Result> one() throws ExecutionException {
    try {
      if (resultSet.next()) {
        return Optional.of(resultInterpreter.interpret(resultSet));
      }
      return Optional.empty();
    } catch (SQLException e) {
      throw new ExecutionException("Failed to fetch the next result", e);
    }
  }

  @Override
  public List<Result> all() throws ExecutionException {
    try {
      List<Result> ret = new ArrayList<>();
      while (resultSet.next()) {
        ret.add(resultInterpreter.interpret(resultSet));
      }
      return ret;
    } catch (SQLException e) {
      throw new ExecutionException("Failed to fetch the next result", e);
    }
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
  public void close() {
    try {
      resultSet.close();
    } catch (SQLException e) {
      logger.warn("Failed to close the resultSet", e);
    }
    try {
      preparedStatement.close();
    } catch (SQLException e) {
      logger.warn("Failed to close the preparedStatement", e);
    }
    try {
      connection.close();
    } catch (SQLException e) {
      logger.warn("Failed to close the connection", e);
    }
  }
}
