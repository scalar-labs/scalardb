package com.scalar.db.util;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.storage.cosmos.CosmosAdmin;
import com.scalar.db.storage.cosmos.CosmosConfig;
import com.scalar.db.storage.dynamo.DynamoAdmin;
import com.scalar.db.storage.dynamo.DynamoConfig;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assertions;

public final class TestUtils {

  public static final int MAX_TEXT_COUNT = 20;
  public static final int MAX_BLOB_LENGTH = 20;

  private TestUtils() {}

  public static Value<?> getRandomValue(Random random, String columnName, DataType dataType) {
    return getRandomValue(random, columnName, dataType, false);
  }

  public static Value<?> getRandomValue(
      Random random, String columnName, DataType dataType, boolean allowEmpty) {
    switch (dataType) {
      case BIGINT:
        return new BigIntValue(columnName, nextBigInt(random));
      case INT:
        return new IntValue(columnName, random.nextInt());
      case FLOAT:
        return new FloatValue(columnName, nextFloat(random));
      case DOUBLE:
        return new DoubleValue(columnName, nextDouble(random));
      case BLOB:
        int length =
            allowEmpty ? random.nextInt(MAX_BLOB_LENGTH) : random.nextInt(MAX_BLOB_LENGTH - 1) + 1;
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return new BlobValue(columnName, bytes);
      case TEXT:
        int count =
            allowEmpty ? random.nextInt(MAX_TEXT_COUNT) : random.nextInt(MAX_TEXT_COUNT - 1) + 1;
        return new TextValue(
            columnName, RandomStringUtils.random(count, 0, 0, true, true, null, random));
      case BOOLEAN:
        return new BooleanValue(columnName, random.nextBoolean());
      default:
        throw new AssertionError();
    }
  }

  public static Column<?> getColumnWithRandomValue(
      Random random, String columnName, DataType dataType, boolean allowEmpty) {
    switch (dataType) {
      case BOOLEAN:
        return BooleanColumn.of(columnName, random.nextBoolean());
      case INT:
        return IntColumn.of(columnName, random.nextInt());
      case BIGINT:
        return BigIntColumn.of(columnName, nextBigInt(random));
      case FLOAT:
        return FloatColumn.of(columnName, nextFloat(random));
      case DOUBLE:
        return DoubleColumn.of(columnName, nextDouble(random));
      case TEXT:
        int count =
            allowEmpty ? random.nextInt(MAX_TEXT_COUNT) : random.nextInt(MAX_TEXT_COUNT - 1) + 1;
        return TextColumn.of(
            columnName, RandomStringUtils.random(count, 0, 0, true, true, null, random));
      case BLOB:
        int length =
            allowEmpty ? random.nextInt(MAX_BLOB_LENGTH) : random.nextInt(MAX_BLOB_LENGTH - 1) + 1;
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return BlobColumn.of(columnName, bytes);
      default:
        throw new AssertionError();
    }
  }

  public static long nextBigInt(Random random) {
    return random
        .longs(BigIntValue.MIN_VALUE, (BigIntValue.MAX_VALUE + 1))
        .limit(1)
        .findFirst()
        .orElse(0);
  }

  public static float nextFloat(Random random) {
    return (float)
        random.doubles(Float.MIN_VALUE, Float.MAX_VALUE).limit(1).findFirst().orElse(0.0d);
  }

  public static double nextDouble(Random random) {
    return random.doubles(Double.MIN_VALUE, Double.MAX_VALUE).limit(1).findFirst().orElse(0.0d);
  }

  public static Value<?> getMinValue(String columnName, DataType dataType) {
    return getMinValue(columnName, dataType, false);
  }

  public static Value<?> getMinValue(String columnName, DataType dataType, boolean allowEmpty) {
    switch (dataType) {
      case BIGINT:
        return new BigIntValue(columnName, BigIntValue.MIN_VALUE);
      case INT:
        return new IntValue(columnName, Integer.MIN_VALUE);
      case FLOAT:
        return new FloatValue(columnName, Float.MIN_VALUE);
      case DOUBLE:
        return new DoubleValue(columnName, Double.MIN_VALUE);
      case BLOB:
        return new BlobValue(columnName, allowEmpty ? new byte[0] : new byte[] {0x00});
      case TEXT:
        return new TextValue(columnName, allowEmpty ? "" : "\u0001");
      case BOOLEAN:
        return new BooleanValue(columnName, false);
      default:
        throw new AssertionError();
    }
  }

  public static Value<?> getMaxValue(String columnName, DataType dataType) {
    switch (dataType) {
      case BIGINT:
        return new BigIntValue(columnName, BigIntValue.MAX_VALUE);
      case INT:
        return new IntValue(columnName, Integer.MAX_VALUE);
      case FLOAT:
        return new FloatValue(columnName, Float.MAX_VALUE);
      case DOUBLE:
        return new DoubleValue(columnName, Double.MAX_VALUE);
      case BLOB:
        byte[] blobBytes = new byte[MAX_BLOB_LENGTH];
        Arrays.fill(blobBytes, (byte) 0xff);
        return new BlobValue(columnName, blobBytes);
      case TEXT:
        StringBuilder builder = new StringBuilder();
        IntStream.range(0, MAX_TEXT_COUNT).forEach(i -> builder.append(Character.MAX_VALUE));
        return new TextValue(columnName, builder.toString());
      case BOOLEAN:
        return new BooleanValue(columnName, true);
      default:
        throw new AssertionError();
    }
  }

  public static List<BooleanValue> booleanValues(String columnName) {
    return Arrays.asList(new BooleanValue(columnName, false), new BooleanValue(columnName, true));
  }

  /**
   * Add a suffix to the metadata database/namespace/schema name and the consensus-commit
   * coordinator table name.
   *
   * @param config the original config
   * @param testName used for the suffix
   * @return config added the suffix
   */
  public static DatabaseConfig addSuffix(DatabaseConfig config, String testName) {
    Properties properties = new Properties();
    properties.putAll(config.getProperties());

    // for Cosmos
    String tableMetadataDatabase = properties.getProperty(CosmosConfig.TABLE_METADATA_DATABASE);
    if (tableMetadataDatabase == null) {
      tableMetadataDatabase = CosmosAdmin.METADATA_DATABASE;
    }
    properties.setProperty(
        CosmosConfig.TABLE_METADATA_DATABASE, tableMetadataDatabase + "_" + testName);

    // for Dynamo
    String tableMetadataNamespace = properties.getProperty(DynamoConfig.TABLE_METADATA_NAMESPACE);
    if (tableMetadataNamespace == null) {
      tableMetadataNamespace = DynamoAdmin.METADATA_NAMESPACE;
    }
    properties.setProperty(
        DynamoConfig.TABLE_METADATA_NAMESPACE, tableMetadataNamespace + "_" + testName);

    // for JDBC
    String tableMetadataSchema = properties.getProperty(JdbcConfig.TABLE_METADATA_SCHEMA);
    if (tableMetadataSchema == null) {
      tableMetadataSchema = JdbcAdmin.METADATA_SCHEMA;
    }
    properties.setProperty(JdbcConfig.TABLE_METADATA_SCHEMA, tableMetadataSchema + "_" + testName);

    // for consensus-commit
    String coordinatorNamespace =
        properties.getProperty(ConsensusCommitConfig.COORDINATOR_NAMESPACE);
    if (coordinatorNamespace == null) {
      coordinatorNamespace = Coordinator.NAMESPACE;
    }
    properties.setProperty(
        ConsensusCommitConfig.COORDINATOR_NAMESPACE, coordinatorNamespace + "_" + testName);

    return new DatabaseConfig(properties);
  }

  public static Order reverseOrder(Order order) {
    switch (order) {
      case ASC:
        return Order.DESC;
      case DESC:
        return Order.ASC;
      default:
        throw new AssertionError();
    }
  }

  //  public static void assertResultsContainsExactly(
  //      List<Result> results, List<ExpectedResult> expectedResults) {
  //    for (Result r : results) {
  //      boolean match = false;
  //      System.out.println("ers size" + expectedResults.size());
  //      for (ExpectedResult er : expectedResults) {
  //        match =
  //            r.getPartitionKey().orElse(Key.of()).equals(er.getPartitionKey())
  //                && (er.getClusteringKey().isPresent()
  //                    &&
  // er.getClusteringKey().orElse(Key.of()).equals(er.getClusteringKey().get()))
  //                && r.getContainedColumnNames().size() == er.getColumnsSize();
  //        for (Column<?> erc : er.getNonKeyColumns()) {
  //          match = match && Objects.equals(r.getAsObject(erc.getName()), erc.getValueAsObject());
  //        }
  //        if (match) {
  //          System.out.println("Remove " + er);
  //          expectedResults.remove(er);
  //          break;
  //        }
  //      }
  //      if (!match) {
  //        Assertions.fail("The result " + r + " is not expected");
  //      }
  //    }
  //    if (!expectedResults.isEmpty()) {
  //      Assertions.fail("The given expected results are missing " + expectedResults);
  //    }
  //  }

  /**
   * Find and return an expected result that matches the result
   *
   * @param result a result
   * @param expectedResults a list of expected results
   * @return the first {@link ExpectedResult} that matches the {@link Result}, otherwise return null
   */
  private static ExpectedResult findFirstMatchingResult(
      Result result, List<ExpectedResult> expectedResults) {
    for (ExpectedResult er : expectedResults) {
      if (er.equalsResult(result)) {
        return er;
      }
    }
    return null;
  }

  /**
   * Asserts the actualResults and expectedResults lists elements are equals without taking the list
   * order into consideration
   *
   * @param actualResults a list of results
   * @param expectedResults a list of expected results
   */
  public static void assertResultsContainsExactlyInAnyOrder(
      List<Result> actualResults, List<ExpectedResult> expectedResults) {
    expectedResults = new ArrayList<>(expectedResults);
    for (Result actualResult : actualResults) {
      ExpectedResult matchedExpectedResult = findFirstMatchingResult(actualResult, expectedResults);
      if (matchedExpectedResult == null) {
        Assertions.fail("The actual result " + actualResult + " is not expected");
      } else {
        expectedResults.remove(matchedExpectedResult);
      }
    }
    if (!expectedResults.isEmpty()) {
      Assertions.fail(
          "The given expected results are missing from the actual results" + expectedResults);
    }
  }

  /**
   * Asserts the actualResults are a subset of expectedResults. In other words, actualResults are
   * contained into expectedResults
   *
   * @param actualResults of list of results
   * @param expectedResults a list of expected results
   */
  public static void assertResultsAreASubsetOf(
      List<Result> actualResults, List<ExpectedResult> expectedResults) {
    expectedResults = new ArrayList<>(expectedResults);
    for (Result actualResult : actualResults) {
      ExpectedResult matchedExpectedResult = findFirstMatchingResult(actualResult, expectedResults);
      if (matchedExpectedResult == null) {
        Assertions.fail("The actual result " + actualResult + " is not expected");
      } else {
        expectedResults.remove(matchedExpectedResult);
      }
    }
  }

  /** Utility class used in testing to facilitate the comparison of {@link Result} */
  public static class ExpectedResult {
    public Optional<Key> getPartitionKey() {
      return partitionKey;
    }

    public Optional<Key> getClusteringKey() {
      return clusteringKey;
    }

    public Set<Column<?>> getColumns() {
      return columns;
    }

    private final Optional<Key> partitionKey;
    private final Optional<Key> clusteringKey;
    private final Set<Column<?>> columns;

    private ExpectedResult(ExpectedResultBuilder builder) {
      this.partitionKey = Optional.ofNullable(builder.partitionKey);
      this.clusteringKey = Optional.ofNullable(builder.clusteringKey);
      this.columns = new HashSet<>();
      if (builder.nonKeyColumns != null) {
        this.columns.addAll(builder.nonKeyColumns);
      }
      partitionKey.ifPresent(pk -> this.columns.addAll(pk.getColumns()));
      clusteringKey.ifPresent(ck -> this.columns.addAll(ck.getColumns()));
    }

    /**
     * Verify the equality with a {@link Result} but ignores the columns ordering.
     *
     * @param other a Result
     * @return true if this object is equal to the other
     */
    public boolean equalsResult(Result other) {
      if (!this.getPartitionKey().equals(other.getPartitionKey())) {
        return false;
      }

      if (!this.getClusteringKey().equals(other.getClusteringKey())) {
        return false;
      }

      Set<Column<?>> otherComparableColumns = new HashSet<>(other.getColumns().values());
      // ignore columns with null value
      otherComparableColumns.removeIf(Column::hasNullValue);

      if (this.columns.size() != otherComparableColumns.size()) {
        return false;
      }
      // Columns ordering is not taken into account
      for (Column<?> column : this.getColumns()) {
        if (!Objects.equals(column.getValueAsObject(), other.getAsObject(column.getName()))) {
          return false;
        }
      }

      return true;
    }

    @Override
    public String toString() {
      ToStringHelper toStringHelper = MoreObjects.toStringHelper(this);
      partitionKey.ifPresent((pk) -> toStringHelper.addValue(pk.toString()));
      clusteringKey.ifPresent((ck) -> toStringHelper.addValue(ck.toString()));
      getColumns().forEach(c -> toStringHelper.add(c.getName(), c.getValueAsObject()));
      return toStringHelper.toString();
    }

    public static class ExpectedResultBuilder {
      private Key partitionKey;
      private Key clusteringKey;
      private List<Column<?>> nonKeyColumns;

      public ExpectedResultBuilder() {}

      public ExpectedResultBuilder partitionKey(Key partitionKey) {
        this.partitionKey = partitionKey;
        return this;
      }

      public ExpectedResultBuilder clusteringKey(Key clusteringKey) {
        this.clusteringKey = clusteringKey;
        return this;
      }

      public ExpectedResultBuilder nonKeyColumns(List<Column<?>> nonKeyColumns) {
        this.nonKeyColumns = nonKeyColumns;
        return this;
      }

      public ExpectedResult build() {
        return new ExpectedResult(this);
      }
    }
  }
}
