package com.scalar.db.dataloader.cli.command.dataexport;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.cli.exception.DirectoryValidationException;
import com.scalar.db.dataloader.cli.exception.InvalidFileExtensionException;
import com.scalar.db.dataloader.cli.util.DirectoryUtils;
import com.scalar.db.dataloader.core.tablemetadata.TableMetadataService;
import com.scalar.db.dataloader.core.util.KeyUtils;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@CommandLine.Command(name = "export", description = "Export data from a ScalarDB table")
public class ExportCommand extends ExportCommandOptions implements Callable<Integer> {

  private static final List<String> ALLOWED_EXTENSIONS = Arrays.asList("csv", "json", "jsonl");

  @Spec CommandSpec spec;

  @Override
  public Integer call() throws Exception {
    validateOutputDirectory(outputFilePath);
    StorageFactory storageFactory = createStorageFactory(configFilePath);
    TableMetadataService metadataService = createTableMetadataService(storageFactory);
    TableMetadata tableMetadata = metadataService.getTableMetadata(namespace, tableName);

    Key partitionKey = KeyUtils.parseKeyValue(partitionKeyValue, tableMetadata);
    Key scanStartKey = KeyUtils.parseKeyValue(scanStartKeyValue, tableMetadata);
    Key scanEndKey = KeyUtils.parseKeyValue(scanEndKeyValue, tableMetadata);

    // Print out the values for now (to avoid spotbugs warnings)
    System.out.println("partitionKey: " + partitionKey);
    System.out.println("scanStartKey: " + scanStartKey);
    System.out.println("scanEndKey: " + scanEndKey);

    return 0;
  }

  private void validateOutputDirectory(String path)
      throws DirectoryValidationException, InvalidFileExtensionException {
    if (path == null || path.isEmpty()) {
      throw new IllegalArgumentException("Output file path cannot be null or empty");
    }

    File file = new File(path);

    if (file.isDirectory()) {
      validateDirectory(path);
    } else {
      validateFileExtension(file.getName());
      validateDirectory(file.getParent());
    }
  }

  private void validateDirectory(String directoryPath) throws DirectoryValidationException {
    // If the directory path is null or empty, use the current working directory
    if (directoryPath == null || directoryPath.isEmpty()) {
      DirectoryUtils.validateTargetDirectory(DirectoryUtils.getCurrentWorkingDirectory());
    } else {
      DirectoryUtils.validateTargetDirectory(directoryPath);
    }
  }

  private void validateFileExtension(String filename) throws InvalidFileExtensionException {
    String extension = FilenameUtils.getExtension(filename);
    if (StringUtils.isBlank(extension)) {
      throw new InvalidFileExtensionException("File extension not found");
    }
    if (!ALLOWED_EXTENSIONS.contains(extension.toLowerCase())) {
      throw new InvalidFileExtensionException("Invalid file extension: " + extension);
    }
  }

  protected StorageFactory createStorageFactory(String configFilePath) throws IOException {
    return StorageFactory.create(configFilePath);
  }

  protected TableMetadataService createTableMetadataService(StorageFactory storageFactory) {
    return new TableMetadataService(storageFactory.getStorageAdmin());
  }
}
