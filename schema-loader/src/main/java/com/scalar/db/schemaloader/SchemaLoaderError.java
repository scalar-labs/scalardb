package com.scalar.db.schemaloader;

import com.scalar.db.common.error.Category;
import com.scalar.db.common.error.ScalarDbError;

public enum SchemaLoaderError implements ScalarDbError {

  //
  // Errors for the user error category
  //
  TABLE_NOT_FOUND(Category.USER_ERROR, "0000", "The table does not exist. Table: %s", "", "Solution: Verify the table name is correct or create the table first."),
  ALTERING_PARTITION_KEYS_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0001",
      "The partition keys for the table %s.%s were modified, but altering partition keys is not supported",
      "",
      "Solution: Recreate the table with the new partition keys and migrate data if needed."),
  ALTERING_CLUSTERING_KEYS_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0002",
      "The clustering keys for the table %s.%s were modified, but altering clustering keys is not supported",
      "",
      "Solution: Recreate the table with the new clustering keys and migrate data if needed."),
  ALTERING_CLUSTERING_ORDER_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0003",
      "The clustering order of the table %s.%s were modified, but altering the clustering order is not supported",
      "",
      "Solution: Recreate the table with the new clustering order and migrate data if needed."),
  DELETING_COLUMN_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0004",
      "The column %s in the table %s.%s has been deleted. Column deletion is not supported when altering a table",
      "",
      "Solution: Remove the column deletion from your schema file. Use schema-loader without the --alter option if you need to drop the table and recreate it."),
  ALTERING_COLUMN_DATA_TYPE_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0005",
      "The data type for the column %s in the table %s.%s was modified, but altering data types is not supported",
      "",
      "Solution: Recreate the table with the new column data type and migrate data if needed."),
  SPECIFYING_SCHEMA_FILE_REQUIRED_WHEN_USING_REPAIR_ALL(
      Category.USER_ERROR,
      "0006",
      "Specifying the '--schema-file' option is required when using the '--repair-all' option",
      "",
      "Solution: Specify the '--schema-file' option with the path to your schema file."),
  SPECIFYING_SCHEMA_FILE_REQUIRED_WHEN_USING_ALTER(
      Category.USER_ERROR,
      "0007",
      "Specifying the '--schema-file' option is required when using the '--alter' option",
      "",
      "Solution: Specify the '--schema-file' option with the path to your schema file."),
  SPECIFYING_SCHEMA_FILE_REQUIRED_WHEN_USING_IMPORT(
      Category.USER_ERROR,
      "0008",
      "Specifying the '--schema-file' option is required when using the '--import' option",
      "",
      "Solution: Specify the '--schema-file' option with the path to your schema file."),
  SPECIFYING_COORDINATOR_WITH_IMPORT_NOT_ALLOWED(
      Category.USER_ERROR,
      "0009",
      "Specifying the '--coordinator' option with the '--import' option is not allowed."
          + " Create Coordinator tables separately",
      "",
      "Solution: Remove the '--coordinator' option and create coordinator tables separately."),
  READING_CONFIG_FILE_FAILED(
      Category.USER_ERROR, "0010", "Reading the configuration file failed. File: %s", "", "Solution: Verify the configuration file path is correct and the file is readable."),
  READING_SCHEMA_FILE_FAILED(
      Category.USER_ERROR, "0011", "Reading the schema file failed. File: %s", "", "Solution: Verify the schema file path is correct and the file is readable."),
  PARSING_SCHEMA_JSON_FAILED(
      Category.USER_ERROR, "0012", "Parsing the schema JSON failed. Details: %s", "", "Solution: Verify the schema file contains valid JSON. Check for syntax errors."),
  PARSE_ERROR_TABLE_NAME_MUST_CONTAIN_NAMESPACE_AND_TABLE(
      Category.USER_ERROR,
      "0013",
      "The table name must contain the namespace and the table. Table: %s",
      "",
      "Solution: Specify the table name in the format 'namespace.table_name' in your schema file."),
  PARSE_ERROR_PARTITION_KEY_MUST_BE_SPECIFIED(
      Category.USER_ERROR, "0014", "The partition key must be specified. Table: %s", "", "Solution: Add the 'partition-key' field to the table definition in your schema file."),
  PARSE_ERROR_INVALID_CLUSTERING_KEY_FORMAT(
      Category.USER_ERROR,
      "0015",
      "Invalid clustering-key format. The clustering key must be in the format of 'column_name' or 'column_name ASC/DESC'."
          + " Table: %s; Clustering key: %s",
      "",
      "Solution: Specify clustering keys in the format 'column_name' or 'column_name ASC/DESC' in your schema file."),
  PARSE_ERROR_COLUMNS_NOT_SPECIFIED(
      Category.USER_ERROR, "0016", "Columns must be specified. Table: %s", "", "Solution: Add the 'columns' field with at least one column to the table definition in your schema file."),
  PARSE_ERROR_INVALID_COLUMN_TYPE(
      Category.USER_ERROR, "0017", "Invalid column type. Table: %s; Column: %s; Type: %s", "", "Solution: Use a valid ScalarDB column type (BOOLEAN, INT, BIGINT, FLOAT, DOUBLE, TEXT, BLOB, DATE, TIME, TIMESTAMP, TIMESTAMPTZ)."),
  ;

  private static final String COMPONENT_NAME = "DB-SCHEMA-LOADER";

  private final Category category;
  private final String id;
  private final String message;
  private final String cause;
  private final String solution;

  SchemaLoaderError(Category category, String id, String message, String cause, String solution) {
    validate(COMPONENT_NAME, category, id, message, cause, solution);

    this.category = category;
    this.id = id;
    this.message = message;
    this.cause = cause;
    this.solution = solution;
  }

  @Override
  public String getComponentName() {
    return COMPONENT_NAME;
  }

  @Override
  public Category getCategory() {
    return category;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getMessage() {
    return message;
  }

  @Override
  public String getCause() {
    return cause;
  }

  @Override
  public String getSolution() {
    return solution;
  }
}
