package com.scalar.db.dataloader.cli.command.dataexport;

import com.scalar.db.dataloader.cli.exception.DirectoryValidationException;
import com.scalar.db.dataloader.cli.exception.InvalidFileExtensionException;
import com.scalar.db.dataloader.cli.util.DirectoryValidationUtil;
import com.scalar.db.dataloader.cli.util.FileUtil;
import java.io.File;
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
    FileUtil.validateFilePath(configFilePath);
    validateOutputDirectory(outputFilePath);
    return 0;
  }

  private void validateOutputDirectory(String filePath)
      throws DirectoryValidationException, InvalidFileExtensionException {
    if (filePath == null || filePath.isEmpty()) {
      throw new IllegalArgumentException("Output file path cannot be null or empty");
    }

    File file = new File(filePath);

    if (file.isDirectory()) {
      validateDirectory(filePath);
    } else {
      validateFileExtension(file.getName());
      validateDirectory(file.getParent());
    }
  }

  private void validateDirectory(String directoryPath) throws DirectoryValidationException {
    // If the directory path is null or empty, use the current working directory
    if (directoryPath == null || directoryPath.isEmpty()) {
      DirectoryValidationUtil.validateWorkingDirectory();
    } else {
      DirectoryValidationUtil.validateTargetDirectory(directoryPath);
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
}
