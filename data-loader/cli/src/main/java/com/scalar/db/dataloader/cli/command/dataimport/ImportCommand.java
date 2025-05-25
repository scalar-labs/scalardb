package com.scalar.db.dataloader.cli.command.dataimport;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.ScalarDbMode;
import com.scalar.db.dataloader.core.dataimport.ImportManager;
import com.scalar.db.dataloader.core.dataimport.ImportOptions;
import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFile;
import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFileTable;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDbStorageManager;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDbTransactionManager;
import com.scalar.db.dataloader.core.dataimport.log.ImportLoggerConfig;
import com.scalar.db.dataloader.core.dataimport.log.LogMode;
import com.scalar.db.dataloader.core.dataimport.log.SingleFileImportLogger;
import com.scalar.db.dataloader.core.dataimport.log.SplitByDataChunkImportLogger;
import com.scalar.db.dataloader.core.dataimport.log.writer.DefaultLogWriterFactory;
import com.scalar.db.dataloader.core.dataimport.log.writer.LogWriterFactory;
import com.scalar.db.dataloader.core.dataimport.processor.DefaultImportProcessorFactory;
import com.scalar.db.dataloader.core.dataimport.processor.ImportProcessorFactory;
import com.scalar.db.dataloader.core.tablemetadata.TableMetadataException;
import com.scalar.db.dataloader.core.tablemetadata.TableMetadataService;
import com.scalar.db.dataloader.core.util.TableMetadataUtil;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

@CommandLine.Command(name = "import", description = "Import data into a ScalarDB table")
public class ImportCommand extends ImportCommandOptions implements Callable<Integer> {

  /** Spec injected by PicoCli */
  @Spec CommandSpec spec;

  @Override
  public Integer call() throws Exception {
    validateImportTarget(controlFilePath, namespace, tableName);
    validateLogDirectory(logDirectory);
    ControlFile controlFile = parseControlFileFromPath(controlFilePath).orElse(null);
    ImportOptions importOptions = createImportOptions(controlFile);
    ImportLoggerConfig config =
        ImportLoggerConfig.builder()
            .logDirectoryPath(logDirectory)
            .isLogRawSourceRecordsEnabled(importOptions.isLogRawRecord())
            .isLogSuccessRecordsEnabled(importOptions.isLogSuccessRecords())
            .prettyPrint(prettyPrint)
            .build();
    LogWriterFactory logWriterFactory = createLogWriterFactory(config);
    Map<String, TableMetadata> tableMetadataMap =
        createTableMetadataMap(controlFile, namespace, tableName);
    try (BufferedReader reader =
        Files.newBufferedReader(Paths.get(sourceFilePath), Charset.defaultCharset())) {
      ImportManager importManager =
          createImportManager(importOptions, tableMetadataMap, reader, logWriterFactory, config);
      importManager.startImport();
    }
    return 0;
  }

  /**
   * Create LogWriterFactory object
   *
   * @return LogWriterFactory object
   */
  private LogWriterFactory createLogWriterFactory(ImportLoggerConfig config) {
    return new DefaultLogWriterFactory(config);
  }

  /**
   * Create TableMetadata Map from provided controlfile/ namespace, table name
   *
   * @param controlFile control file
   * @param namespace Namespace
   * @param tableName Single table name
   * @return {@code Map<String, TableMetadata>} a table metadata map
   * @throws ParameterException if one of the argument values is wrong
   */
  private Map<String, TableMetadata> createTableMetadataMap(
      ControlFile controlFile, String namespace, String tableName)
      throws IOException, TableMetadataException {
    File configFile = new File(configFilePath);
    StorageFactory storageFactory = StorageFactory.create(configFile);
    TableMetadataService tableMetadataService =
        new TableMetadataService(storageFactory.getStorageAdmin());
    Map<String, TableMetadata> tableMetadataMap = new HashMap<>();
    if (controlFile != null) {
      for (ControlFileTable table : controlFile.getTables()) {
        tableMetadataMap.put(
            TableMetadataUtil.getTableLookupKey(table.getNamespace(), table.getTable()),
            tableMetadataService.getTableMetadata(table.getNamespace(), table.getTable()));
      }
    } else {
      tableMetadataMap.put(
          TableMetadataUtil.getTableLookupKey(namespace, tableName),
          tableMetadataService.getTableMetadata(namespace, tableName));
    }
    return tableMetadataMap;
  }

  /**
   * Create ImportManager object from data
   *
   * @param importOptions import options
   * @param tableMetadataMap table metadata map
   * @param reader buffered reader with source data
   * @param logWriterFactory log writer factory object
   * @param config import logging config
   * @return ImportManager object
   */
  private ImportManager createImportManager(
      ImportOptions importOptions,
      Map<String, TableMetadata> tableMetadataMap,
      BufferedReader reader,
      LogWriterFactory logWriterFactory,
      ImportLoggerConfig config)
      throws IOException {
    File configFile = new File(configFilePath);
    ImportProcessorFactory importProcessorFactory = new DefaultImportProcessorFactory();
    ImportManager importManager;
    if (scalarDbMode == ScalarDbMode.TRANSACTION) {
      ScalarDbTransactionManager scalarDbTransactionManager =
          new ScalarDbTransactionManager(TransactionFactory.create(configFile));
      importManager =
          new ImportManager(
              tableMetadataMap,
              reader,
              importOptions,
              importProcessorFactory,
              ScalarDbMode.TRANSACTION,
              null,
              scalarDbTransactionManager.getDistributedTransactionManager());
    } else {
      ScalarDbStorageManager scalarDbStorageManager =
          new ScalarDbStorageManager(StorageFactory.create(configFile));
      importManager =
          new ImportManager(
              tableMetadataMap,
              reader,
              importOptions,
              importProcessorFactory,
              ScalarDbMode.STORAGE,
              scalarDbStorageManager.getDistributedStorage(),
              null);
    }
    if (importOptions.getLogMode().equals(LogMode.SPLIT_BY_DATA_CHUNK)) {
      importManager.addListener(new SplitByDataChunkImportLogger(config, logWriterFactory));
    } else importManager.addListener(new SingleFileImportLogger(config, logWriterFactory));
    return importManager;
  }

  /**
   * Validate import targets
   *
   * @param controlFilePath control file path
   * @param namespace Namespace
   * @param tableName Single table name
   * @throws ParameterException if one of the argument values is wrong
   */
  private void validateImportTarget(String controlFilePath, String namespace, String tableName) {
    // Throw an error if there was no clear imports target specified
    if (StringUtils.isBlank(controlFilePath)
        && (StringUtils.isBlank(namespace) || StringUtils.isBlank(tableName))) {
      throw new ParameterException(
          spec.commandLine(), CoreError.DATA_LOADER_IMPORT_TARGET_MISSING.buildMessage());
    }

    // Make sure the control file exists when a path is provided
    if (!StringUtils.isBlank(controlFilePath)) {
      Path path = Paths.get(controlFilePath);
      if (!Files.exists(path)) {
        throw new ParameterException(
            spec.commandLine(),
            CoreError.DATA_LOADER_MISSING_IMPORT_FILE.buildMessage(
                controlFilePath, FILE_OPTION_NAME_LONG_FORMAT));
      }
    }
  }

  /**
   * Validate log directory path
   *
   * @param logDirectory log directory path
   * @throws ParameterException if the path is invalid
   */
  private void validateLogDirectory(String logDirectory) throws ParameterException {
    Path logDirectoryPath;
    if (!StringUtils.isBlank(logDirectory)) {
      // User-provided log directory via CLI argument
      logDirectoryPath = Paths.get(logDirectory);

      if (Files.exists(logDirectoryPath)) {
        // Check if the provided directory is writable
        if (!Files.isWritable(logDirectoryPath)) {
          throw new ParameterException(
              spec.commandLine(),
              CoreError.DATA_LOADER_LOG_DIRECTORY_CREATION_FAILED.buildMessage(
                  logDirectoryPath.toAbsolutePath()));
        }
      } else {
        // Create the log directory if it doesn't exist
        try {
          Files.createDirectories(logDirectoryPath);
        } catch (IOException e) {
          throw new ParameterException(
              spec.commandLine(),
              CoreError.DATA_LOADER_LOG_DIRECTORY_CREATION_FAILED.buildMessage(
                  logDirectoryPath.toAbsolutePath()));
        }
      }
      return;
    }

    // Use the current working directory as the log directory
    logDirectoryPath = Paths.get(System.getProperty("user.dir"));

    // Check if the current working directory is writable
    if (!Files.isWritable(logDirectoryPath)) {
      throw new ParameterException(
          spec.commandLine(),
          CoreError.DATA_LOADER_LOG_DIRECTORY_WRITE_ACCESS_DENIED.buildMessage(
              logDirectoryPath.toAbsolutePath()));
    }
  }

  /**
   * Generate control file from a valid control file path
   *
   * @param controlFilePath control directory path
   * @return {@code Optional<ControlFile>} generated control file object
   * @throws ParameterException if the path is invalid
   */
  private Optional<ControlFile> parseControlFileFromPath(String controlFilePath) {
    if (StringUtils.isBlank(controlFilePath)) {
      return Optional.empty();
    }
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      ControlFile controlFile =
          objectMapper.readValue(new File(controlFilePath), ControlFile.class);
      return Optional.of(controlFile);
    } catch (IOException e) {
      throw new ParameterException(
          spec.commandLine(),
          CoreError.DATA_LOADER_INVALID_CONTROL_FILE.buildMessage(controlFilePath));
    }
  }

  /**
   * Generate import options object from provided cli parameter data
   *
   * @param controlFile control file
   * @return ImportOptions generated import options object
   */
  private ImportOptions createImportOptions(ControlFile controlFile) {
    ImportOptions.ImportOptionsBuilder builder =
        ImportOptions.builder()
            .fileFormat(sourceFileFormat)
            .requireAllColumns(requireAllColumns)
            .prettyPrint(prettyPrint)
            .controlFile(controlFile)
            .controlFileValidationLevel(controlFileValidation)
            .logRawRecord(logRawRecord)
            .logSuccessRecords(logSuccessRecords)
            .ignoreNullValues(ignoreNullValues)
            .namespace(namespace)
            .dataChunkSize(dataChunkSize)
            .transactionBatchSize(transactionSize)
            .maxThreads(maxThreads)
            .dataChunkQueueSize(dataChunkQueueSize)
            .tableName(tableName);

    // Import mode
    if (importMode != null) {
      builder.importMode(importMode);
    }
    if (!splitLogMode) {
      builder.logMode(LogMode.SINGLE_FILE);
    }

    // CSV options
    if (sourceFileFormat.equals(FileFormat.CSV)) {
      builder.delimiter(delimiter);
      if (!StringUtils.isBlank(customHeaderRow)) {
        builder.customHeaderRow(customHeaderRow);
      }
    }
    return builder.build();
  }
}
