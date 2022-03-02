package com.scalar.db.sql;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.sql.exception.SqlException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;

public class ScannerResultSet implements ResultSet {

  private final Scanner scanner;
  private final ImmutableList<String> projectedColumnNames;

  public ScannerResultSet(Scanner scanner, ImmutableList<String> projectedColumnNames) {
    this.scanner = scanner;
    this.projectedColumnNames = projectedColumnNames;
  }

  @Override
  public Optional<Record> one() {
    try {
      return scanner.one().map(r -> new ResultRecord(r, projectedColumnNames));
    } catch (ExecutionException e) {
      throw new SqlException("Failed to get a result from the scanner", e);
    }
  }

  @Override
  public List<Record> all() {
    try {
      return scanner.all().stream()
          .map(r -> new ResultRecord(r, projectedColumnNames))
          .collect(Collectors.toList());
    } catch (ExecutionException e) {
      throw new SqlException("Failed to get results from the scanner", e);
    }
  }

  @Override
  public Iterator<Record> iterator() {
    return new ResultIterator(scanner.iterator(), projectedColumnNames);
  }

  @Override
  public void close() {
    try {
      scanner.close();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close the scanner", e);
    }
  }

  private static class ResultIterator implements Iterator<Record> {

    private final Iterator<Result> iterator;
    private final ImmutableList<String> projectedColumnNames;

    public ResultIterator(Iterator<Result> iterator, ImmutableList<String> projectedColumnNames) {
      this.iterator = iterator;
      this.projectedColumnNames = projectedColumnNames;
    }

    @Override
    public boolean hasNext() {
      try {
        return iterator.hasNext();
      } catch (RuntimeException e) {
        throw new SqlException("Failed to get a result from the scanner", e);
      }
    }

    @Override
    public Record next() {
      try {
        return new ResultRecord(iterator.next(), projectedColumnNames);
      } catch (NoSuchElementException e) {
        throw e;
      } catch (RuntimeException e) {
        throw new SqlException("Failed to get a result from the scanner", e);
      }
    }
  }
}
