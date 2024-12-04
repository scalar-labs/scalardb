package com.scalar.db.dataloader.core;

/** The constants that are used in the com.scalar.dataloader.core package */
public class Constants {

  public static final String IMPORT_LOG_ENTRY_STATUS_FIELD = "data_loader_import_status";
  public static final String TABLE_LOOKUP_KEY_FORMAT = "%s.%s";

  public static final String LOG_UPDATE_SUCCESS = "Row %s has been updated in table %s.%s";
  public static final String LOG_INSERT_SUCCESS = "Row %s has been inserted into table %s.%s";
  public static final String LOG_IMPORT_VALIDATION = "Validating data for line %s ...";
  public static final String LOG_IMPORT_GET_DATA =
      "Retrieving existing data record from database ...";
  public static final String LOG_IMPORT_LINE_SUCCESS = "Row %s import is completed";
  public static final String LOG_IMPORT_LINE_FAILED = "Row %s import has failed: %s";
  public static final String LOG_IMPORT_COMPLETED =
      "The import process has been completed. Please check the success and failed output files for a detailed report";

  public static final String LOG_SCANNING_START = "Retrieving data from %s.%s table ...";
  public static final String LOG_CONVERTING = "Converting %s.%s data to %s ...";
  public static final String MISSING_CSV_HEADERS =
      "Valid headers are not present or missing in the provided CSV file";
  public static final String ERROR_MISSING_SOURCE_FIELD =
      "the data mapping source field '%s' for table '%s' is missing in the json data record";
  public static final String ABORT_TRANSACTION_STATUS =
      "Transaction aborted as part of batch transaction aborted";
}
