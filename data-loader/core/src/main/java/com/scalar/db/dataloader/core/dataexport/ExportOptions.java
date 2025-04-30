package com.scalar.db.dataloader.core.dataexport;

import com.scalar.db.api.Scan;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.ScanRange;
import com.scalar.db.io.Key;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import lombok.Data;

/** Options for a ScalarDB export data operation. */
@SuppressWarnings({"SameNameButDifferent", "MissingSummary"})
@Builder(builderMethodName = "hiddenBuilder")
@Data
public class ExportOptions {

  private final String namespace;
  private final String tableName;
  private final Key scanPartitionKey;
  private final FileFormat outputFileFormat;
  private final ScanRange scanRange;
  private final int limit;
  private final int maxThreadCount;
  private final boolean prettyPrintJson;

  @Builder.Default private final int dataChunkSize = 200;
  @Builder.Default private final String delimiter = ";";
  @Builder.Default private final boolean excludeHeaderRow = false;
  @Builder.Default private final boolean includeTransactionMetadata = false;
  @Builder.Default private List<String> projectionColumns = Collections.emptyList();
  private List<Scan.Ordering> sortOrders;

  /**
   * Generates and returns an export options builder.
   *
   * @param namespace namespaces for export
   * @param tableName tableName for export
   * @param scanPartitionKey scan partition key for export
   * @param outputFileFormat output file format for export
   * @return a configured export options builder
   */
  public static ExportOptionsBuilder builder(
      String namespace, String tableName, Key scanPartitionKey, FileFormat outputFileFormat) {
    return hiddenBuilder()
        .namespace(namespace)
        .tableName(tableName)
        .scanPartitionKey(scanPartitionKey)
        .outputFileFormat(outputFileFormat);
  }
}
