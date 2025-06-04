package com.scalar.db.dataloader.cli.command.dataexport;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.cli.exception.DirectoryValidationException;
import com.scalar.db.dataloader.cli.util.DirectoryUtils;
import com.scalar.db.dataloader.cli.util.FileUtils;
import com.scalar.db.dataloader.cli.util.InvalidFilePathException;
import com.scalar.db.dataloader.core.ColumnKeyValue;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.ScalarDbMode;
import com.scalar.db.dataloader.core.ScanRange;
import com.scalar.db.dataloader.core.dataexport.CsvExportManager;
import com.scalar.db.dataloader.core.dataexport.ExportManager;
import com.scalar.db.dataloader.core.dataexport.ExportOptions;
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
import com.scalar.db.service.TransactionFactory;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@CommandLine.Command(name = "export", description = "export data from a ScalarDB table")
public class ExportCommand extends ExportCommandOptions implements Callable<Integer> {

  private static final String EXPORT_FILE_NAME_FORMAT = "export.%s.%s.%s.%s";
  private static final Logger logger = LoggerFactory.getLogger(ExportCommand.class);

  @Spec CommandSpec spec;

  @Override
  public Integer call() throws Exception {
    String scalarDbPropertiesFilePath = getScalarDbPropertiesFilePath();

    try {
      validateOutputDirectory();
      FileUtils.validateFilePath(scalarDbPropertiesFilePath);
      TableMetadataService tableMetadataService =
          createTableMetadataService(scalarDbMode, scalarDbPropertiesFilePath);
      ScalarDbDao scalarDbDao = new ScalarDbDao();

      ExportManager exportManager =
          createExportManager(scalarDbMode, scalarDbDao, outputFormat, scalarDbPropertiesFilePath);

      TableMetadata tableMetadata = tableMetadataService.getTableMetadata(namespace, table);

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
      logger.info("Exporting data to file: {}", filePath);

      try (BufferedWriter writer =
          Files.newBufferedWriter(Paths.get(filePath), Charset.defaultCharset(), CREATE, APPEND)) {
        exportManager.startExport(exportOptions, tableMetadata, writer);
      }

    } catch (DirectoryValidationException e) {
      logger.error("Invalid output directory path: {}", outputDirectory);
      return 1;
    } catch (InvalidFilePathException e) {
      logger.error(
          "The ScalarDB connection settings file path is invalid or the file is missing: {}",
          scalarDbPropertiesFilePath);
      return 1;
    } catch (TableMetadataException e) {
      logger.error("Failed to retrieve table metadata: {}", e.getMessage());
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

  private TableMetadataService createTableMetadataService(
      ScalarDbMode scalarDbMode, String scalarDbPropertiesFilePath) throws IOException {
    if (scalarDbMode.equals(ScalarDbMode.TRANSACTION)) {
      TransactionFactory transactionFactory = TransactionFactory.create(scalarDbPropertiesFilePath);
      return new TableMetadataService(transactionFactory.getTransactionAdmin());
    }
    StorageFactory storageFactory = StorageFactory.create(scalarDbPropertiesFilePath);
    return new TableMetadataService(storageFactory.getStorageAdmin());
  }

  private ExportManager createExportManager(
      ScalarDbMode scalarDbMode,
      ScalarDbDao scalarDbDao,
      FileFormat fileFormat,
      String scalarDbPropertiesFilePath)
      throws IOException {
    ProducerTaskFactory taskFactory =
        new ProducerTaskFactory(delimiter, includeTransactionMetadata, prettyPrintJson);
    if (scalarDbMode.equals(ScalarDbMode.TRANSACTION)) {
      DistributedStorage storage = StorageFactory.create(scalarDbPropertiesFilePath).getStorage();
      return createExportManagerWithStorage(storage, scalarDbDao, fileFormat, taskFactory);
    } else {
      DistributedTransactionManager distributedTransactionManager =
          TransactionFactory.create(scalarDbPropertiesFilePath).getTransactionManager();
      return createExportManagerWithTransaction(
          distributedTransactionManager, scalarDbDao, fileFormat, taskFactory);
    }
  }

  private ExportManager createExportManagerWithTransaction(
      DistributedTransactionManager distributedTransactionManager,
      ScalarDbDao scalarDbDao,
      FileFormat fileFormat,
      ProducerTaskFactory taskFactory) {
    switch (fileFormat) {
      case JSON:
        return new JsonExportManager(distributedTransactionManager, scalarDbDao, taskFactory);
      case JSONL:
        return new JsonLineExportManager(distributedTransactionManager, scalarDbDao, taskFactory);
      case CSV:
        return new CsvExportManager(distributedTransactionManager, scalarDbDao, taskFactory);
      default:
        throw new AssertionError("Invalid file format" + fileFormat);
    }
  }

  private ExportManager createExportManagerWithStorage(
      DistributedStorage storage,
      ScalarDbDao scalarDbDao,
      FileFormat fileFormat,
      ProducerTaskFactory taskFactory) {
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
                EXPORT_FILE_NAME_FORMAT,
                namespace,
                table,
                System.nanoTime(),
                outputFormat.toString().toLowerCase())
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
