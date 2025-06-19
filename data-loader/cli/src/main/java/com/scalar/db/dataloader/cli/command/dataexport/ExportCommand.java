package com.scalar.db.dataloader.cli.command.dataexport;

import static com.scalar.db.dataloader.cli.util.CommandLineInputUtils.validatePositiveValue;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.cli.exception.DirectoryValidationException;
import com.scalar.db.dataloader.cli.util.DirectoryUtils;
import com.scalar.db.dataloader.cli.util.FileUtils;
import com.scalar.db.dataloader.cli.util.InvalidFilePathException;
import com.scalar.db.dataloader.core.ColumnKeyValue;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.ScanRange;
import com.scalar.db.dataloader.core.dataexport.CsvExportManager;
import com.scalar.db.dataloader.core.dataexport.ExportManager;
import com.scalar.db.dataloader.core.dataexport.ExportOptions;
import com.scalar.db.dataloader.core.dataexport.ExportReport;
import com.scalar.db.dataloader.core.dataexport.JsonExportManager;
import com.scalar.db.dataloader.core.dataexport.JsonLineExportManager;
import com.scalar.db.dataloader.core.dataexport.producer.ProducerTaskFactory;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDbDao;
import com.scalar.db.dataloader.core.exception.ColumnParsingException;
import com.scalar.db.dataloader.core.exception.KeyParsingException;
import com.scalar.db.dataloader.core.tablemetadata.TableMetadataException;
import com.scalar.db.dataloader.core.tablemetadata.TableMetadataService;
import com.scalar.db.dataloader.core.util.KeyUtils;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import java.io.BufferedWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@CommandLine.Command(name = "export", description = "export data from a ScalarDB table")
public class ExportCommand extends ExportCommandOptions implements Callable<Integer> {

  @Spec CommandSpec spec;

  @Override
  public Integer call() throws Exception {
    String scalarDbPropertiesFilePath = getScalarDbPropertiesFilePath();

    try {
      validateOutputDirectory();
      FileUtils.validateFilePath(scalarDbPropertiesFilePath);
      validatePositiveValue(
          spec.commandLine(), dataChunkSize, CoreError.DATA_LOADER_INVALID_DATA_CHUNK_SIZE);
      validatePositiveValue(
          spec.commandLine(), maxThreads, CoreError.DATA_LOADER_INVALID_MAX_THREADS);

      StorageFactory storageFactory = StorageFactory.create(scalarDbPropertiesFilePath);
      TableMetadataService metaDataService =
          new TableMetadataService(storageFactory.getStorageAdmin());
      ScalarDbDao scalarDbDao = new ScalarDbDao();

      ExportManager exportManager = createExportManager(storageFactory, scalarDbDao, outputFormat);

      TableMetadata tableMetadata = metaDataService.getTableMetadata(namespace, table);

      Key partitionKey =
          partitionKeyValue != null ? getKeysFromList(partitionKeyValue, tableMetadata) : null;
      Key scanStartKey =
          scanStartKeyValue != null
              ? getKey(scanStartKeyValue, namespace, table, tableMetadata)
              : null;
      Key scanEndKey =
          scanEndKeyValue != null ? getKey(scanEndKeyValue, namespace, table, tableMetadata) : null;

      ScanRange scanRange =
          new ScanRange(scanStartKey, scanEndKey, scanStartInclusive, scanEndInclusive);
      ExportOptions exportOptions = buildExportOptions(partitionKey, scanRange);

      String filePath =
          getOutputAbsoluteFilePath(
              outputDirectory, outputFileName, exportOptions.getOutputFileFormat());

      try (BufferedWriter writer =
          Files.newBufferedWriter(Paths.get(filePath), Charset.defaultCharset(), CREATE, APPEND)) {
        ConsoleExportProgressReporter reporter = new ConsoleExportProgressReporter(filePath);
        ExportReport report = exportManager.startExport(exportOptions, tableMetadata, writer);
        reporter.reportCompletion(report.getExportedRowCount());
      }

    } catch (DirectoryValidationException e) {
      ConsoleExportProgressReporter.reportError("Invalid output directory: " + outputDirectory, e);
      return 1;
    } catch (InvalidFilePathException e) {
      ConsoleExportProgressReporter.reportError(
          "Invalid ScalarDB connection file path: " + scalarDbPropertiesFilePath, e);
      return 1;
    } catch (TableMetadataException e) {
      ConsoleExportProgressReporter.reportError("Failed to retrieve table metadata", e);
      return 1;
    }
    return 0;
  }

  private String getScalarDbPropertiesFilePath() {
    if (StringUtils.isBlank(configFilePath)) {
      throw new IllegalArgumentException(
          CoreError.DATA_LOADER_CONFIG_FILE_PATH_BLANK.buildMessage());
    }
    return Objects.equals(configFilePath, DEFAULT_CONFIG_FILE_NAME)
        ? Paths.get("").toAbsolutePath().resolve(DEFAULT_CONFIG_FILE_NAME).toString()
        : configFilePath;
  }

  private void validateOutputDirectory() throws DirectoryValidationException {
    if (StringUtils.isBlank(outputDirectory)) {
      DirectoryUtils.validateWorkingDirectory();
    } else {
      DirectoryUtils.validateOrCreateTargetDirectory(outputDirectory);
    }
  }

  private ExportManager createExportManager(
      StorageFactory storageFactory, ScalarDbDao scalarDbDao, FileFormat fileFormat) {
    ProducerTaskFactory taskFactory =
        new ProducerTaskFactory(delimiter, includeTransactionMetadata, prettyPrintJson);
    DistributedStorage storage = storageFactory.getStorage();
    switch (fileFormat) {
      case JSON:
        return new JsonExportManager(storage, scalarDbDao, taskFactory);
      case JSONL:
        return new JsonLineExportManager(storage, scalarDbDao, taskFactory);
      case CSV:
        return new CsvExportManager(storage, scalarDbDao, taskFactory);
      default:
        throw new AssertionError("Invalid file format" + fileFormat);
    }
  }

  private ExportOptions buildExportOptions(Key partitionKey, ScanRange scanRange) {
    ExportOptions.ExportOptionsBuilder builder =
        ExportOptions.builder(namespace, table, partitionKey, outputFormat)
            .sortOrders(sortOrders)
            .excludeHeaderRow(excludeHeader)
            .includeTransactionMetadata(includeTransactionMetadata)
            .delimiter(delimiter)
            .limit(limit)
            .maxThreadCount(maxThreads)
            .dataChunkSize(dataChunkSize)
            .prettyPrintJson(prettyPrintJson)
            .scanRange(scanRange);

    if (projectionColumns != null) {
      builder.projectionColumns(projectionColumns);
    }

    return builder.build();
  }

  private String getOutputAbsoluteFilePath(
      String outputDirectory, String outputFileName, FileFormat outputFormat) {
    String fileName =
        StringUtils.isBlank(outputFileName)
            ? String.format(
                "export.%s.%s.%s.%s",
                namespace, table, System.nanoTime(), outputFormat.toString().toLowerCase())
            : outputFileName;

    if (StringUtils.isBlank(outputDirectory)) {
      return Paths.get("").toAbsolutePath().resolve(fileName).toAbsolutePath().toString();
    } else {
      return Paths.get(outputDirectory).resolve(fileName).toAbsolutePath().toString();
    }
  }

  /**
   * Convert ColumnKeyValue list to a key
   *
   * @param keyValueList key value list
   * @param tableMetadata table metadata
   * @return key
   * @throws ColumnParsingException if any error occur during parsing column value
   */
  private Key getKeysFromList(List<ColumnKeyValue> keyValueList, TableMetadata tableMetadata)
      throws ColumnParsingException {
    return KeyUtils.parseMultipleKeyValues(keyValueList, tableMetadata);
  }

  /**
   * Convert ColumnKeyValue to a key
   *
   * @param keyValue key value
   * @param tableMetadata table metadata
   * @return key
   * @throws KeyParsingException if any error occur during decoding key
   */
  private Key getKey(
      ColumnKeyValue keyValue, String namespace, String table, TableMetadata tableMetadata)
      throws KeyParsingException {
    return KeyUtils.parseKeyValue(keyValue, namespace, table, tableMetadata);
  }
}
