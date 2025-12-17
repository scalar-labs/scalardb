package com.scalar.db.dataloader.cli;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * Helper class for validating data in integration tests. Provides methods to verify imported and
 * exported data.
 */
public class TestDataValidationHelper {

  /**
   * Verifies that a record exists in the database with the specified partition key.
   *
   * @param configPath path to ScalarDB config file
   * @param namespace namespace name
   * @param table table name
   * @param partitionKeyValue partition key value
   * @return true if record exists, false otherwise
   */
  public static boolean verifyRecordExists(
      Path configPath, String namespace, String table, int partitionKeyValue) throws IOException {
    try {
      Properties props = new Properties();
      props.load(Files.newInputStream(configPath));
      StorageFactory factory = StorageFactory.create(props);
      DistributedStorage storage = factory.getStorage();

      Key partitionKey = Key.ofInt("id", partitionKeyValue);
      Get get =
          Get.newBuilder().namespace(namespace).table(table).partitionKey(partitionKey).build();

      Optional<Result> result = storage.get(get);
      storage.close();
      return result.isPresent();
    } catch (Exception e) {
      throw new RuntimeException("Failed to verify record existence", e);
    }
  }

  /**
   * Counts the number of records in a table by scanning all records.
   *
   * @param configPath path to ScalarDB config file
   * @param namespace namespace name
   * @param table table name
   * @return number of records in the table
   */
  public static int countRecords(Path configPath, String namespace, String table)
      throws IOException {
    try {
      Properties props = new Properties();
      props.load(Files.newInputStream(configPath));
      StorageFactory factory = StorageFactory.create(props);
      DistributedStorage storage = factory.getStorage();

      Scan scan = Scan.newBuilder().namespace(namespace).table(table).all().build();

      List<Result> results = new ArrayList<>();
      try (Scanner scanner = storage.scan(scan)) {
        for (Result result : scanner) {
          results.add(result);
        }
      } catch (IOException e) {
        // Scanner.close() may throw IOException, but we've already collected results
        // Log warning but continue
      }
      storage.close();
      return results.size();
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to count records", e);
    }
  }

  /**
   * Verifies that an exported CSV file contains the expected number of lines (including header).
   *
   * @param exportedFile path to exported file
   * @param expectedLineCount expected number of lines
   * @return true if file has expected line count
   */
  public static boolean verifyExportedFileLineCount(Path exportedFile, int expectedLineCount)
      throws IOException {
    try (Stream<String> lines = Files.lines(exportedFile)) {
      long actualLineCount = lines.count();
      return actualLineCount == expectedLineCount;
    }
  }

  /**
   * Verifies that an exported file exists and is not empty.
   *
   * @param exportedFile path to exported file
   * @return true if file exists and has content
   */
  public static boolean verifyExportedFileExists(Path exportedFile) throws IOException {
    return Files.exists(exportedFile) && Files.size(exportedFile) > 0;
  }

  /**
   * Verifies that an exported JSON/JSONL file contains the expected number of records.
   *
   * @param exportedFile path to exported file
   * @param expectedRecordCount expected number of records
   * @return true if file has expected record count
   */
  public static boolean verifyExportedJsonRecordCount(Path exportedFile, int expectedRecordCount)
      throws IOException {
    try (Stream<String> lines = Files.lines(exportedFile)) {
      long actualLineCount = lines.count();
      // For JSONL, each line is a record
      // For JSON array, we'd need to parse it, but for now assume JSONL format
      return actualLineCount == expectedRecordCount;
    }
  }
}
