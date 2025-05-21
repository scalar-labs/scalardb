package com.scalar.db.common.error;

public enum CoreError implements ScalarDbError {

  //
  // Errors for the user error category
  //
  OPERATION_CHECK_ERROR_INDEX_ONLY_SINGLE_COLUMN_INDEX_SUPPORTED(
      Category.USER_ERROR,
      "0000",
      "Only a single-column index is supported. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_INDEX_NON_INDEXED_COLUMN_SPECIFIED(
      Category.USER_ERROR,
      "0001",
      "The column of the specified index key is not indexed. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_INDEX_INDEX_KEY_NOT_PROPERLY_SPECIFIED(
      Category.USER_ERROR,
      "0002",
      "The index key is not properly specified. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_INDEX_CLUSTERING_KEY_SPECIFIED(
      Category.USER_ERROR,
      "0003",
      "Clustering keys cannot be specified when using an index. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_INDEX_ORDERING_SPECIFIED(
      Category.USER_ERROR,
      "0004",
      "Orderings cannot be specified when using an index. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_LIMIT(
      Category.USER_ERROR, "0005", "The limit cannot be negative. Operation: %s", "", ""),
  OPERATION_CHECK_ERROR_CROSS_PARTITION_SCAN(
      Category.USER_ERROR, "0006", "Cross-partition scan is not enabled. Operation: %s", "", ""),
  OPERATION_CHECK_ERROR_CROSS_PARTITION_SCAN_ORDERING(
      Category.USER_ERROR,
      "0007",
      "Cross-partition scan ordering is not enabled. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_CROSS_PARTITION_SCAN_FILTERING(
      Category.USER_ERROR,
      "0008",
      "Cross-partition scan filtering is not enabled. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_PROJECTION(
      Category.USER_ERROR,
      "0009",
      "The specified projection is not found. Projection: %s, Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_CLUSTERING_KEY_BOUNDARY(
      Category.USER_ERROR,
      "0010",
      "The clustering key boundary is not properly specified. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_START_CLUSTERING_KEY(
      Category.USER_ERROR,
      "0011",
      "The start clustering key is not properly specified. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_END_CLUSTERING_KEY(
      Category.USER_ERROR,
      "0012",
      "The end clustering key is not properly specified. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_ORDERING_NOT_PROPERLY_SPECIFIED(
      Category.USER_ERROR, "0013", "Orderings are not properly specified. Operation: %s", "", ""),
  OPERATION_CHECK_ERROR_ORDERING_COLUMN_NOT_FOUND(
      Category.USER_ERROR,
      "0014",
      "The specified ordering column is not found. Ordering: %s, Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_CONDITION(
      Category.USER_ERROR,
      "0015",
      "The condition is not properly specified. Operation: %s",
      "",
      ""),
  TABLE_NOT_FOUND(Category.USER_ERROR, "0016", "The table does not exist. Table: %s", "", ""),
  OPERATION_CHECK_ERROR_INVALID_COLUMN(
      Category.USER_ERROR,
      "0017",
      "The column value is not properly specified. Column: %s, Operation: %s",
      "",
      ""),
  EMPTY_MUTATIONS_SPECIFIED(Category.USER_ERROR, "0018", "The mutations are empty", "", ""),
  OPERATION_CHECK_ERROR_MULTI_PARTITION_MUTATION(
      Category.USER_ERROR,
      "0019",
      "Mutations that span multiple partitions are not supported. Mutations: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_PARTITION_KEY(
      Category.USER_ERROR,
      "0020",
      "The partition key is not properly specified. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_CLUSTERING_KEY(
      Category.USER_ERROR,
      "0021",
      "The clustering key is not properly specified. Operation: %s",
      "",
      ""),
  AUTH_NOT_ENABLED(
      Category.USER_ERROR,
      "0022",
      "The authentication and authorization feature is not enabled. To use this feature, you must enable it. Note that this feature is supported only in the ScalarDB Enterprise edition",
      "",
      ""),
  CONDITION_BUILD_ERROR_CONDITION_NOT_ALLOWED_FOR_PUT_IF(
      Category.USER_ERROR,
      "0023",
      "This condition is not allowed for the PutIf operation. Condition: %s",
      "",
      ""),
  CONDITION_BUILD_ERROR_CONDITION_NOT_ALLOWED_FOR_DELETE_IF(
      Category.USER_ERROR,
      "0024",
      "This condition is not allowed for the DeleteIf operation. Condition: %s",
      "",
      ""),
  LIKE_CHECK_ERROR_OPERATOR_MUST_BE_LIKE_OR_NOT_LIKE(
      Category.USER_ERROR, "0025", "Operator must be LIKE or NOT_LIKE. Operator: %s", "", ""),
  LIKE_CHECK_ERROR_ESCAPE_CHARACTER_MUST_BE_STRING_OF_SINGLE_CHARACTER_OR_EMPTY_STRING(
      Category.USER_ERROR,
      "0026",
      "An escape character must be a string of a single character or an empty string",
      "",
      ""),
  LIKE_CHECK_ERROR_LIKE_PATTERN_MUST_NOT_BE_NULL(
      Category.USER_ERROR, "0027", "The LIKE pattern must not be null", "", ""),
  LIKE_CHECK_ERROR_LIKE_PATTERN_MUST_NOT_INCLUDE_ONLY_ESCAPE_CHARACTER(
      Category.USER_ERROR,
      "0028",
      "The LIKE pattern must not include only an escape character",
      "",
      ""),
  LIKE_CHECK_ERROR_LIKE_PATTERN_MUST_NOT_END_WITH_ESCAPE_CHARACTER(
      Category.USER_ERROR,
      "0029",
      "The LIKE pattern must not end with an escape character",
      "",
      ""),
  COLUMN_NOT_FOUND(Category.USER_ERROR, "0030", "The column %s does not exist", "", ""),
  GET_BUILD_ERROR_OPERATION_NOT_SUPPORTED_WHEN_GETTING_RECORDS_OF_DATABASE_WITHOUT_USING_INDEX(
      Category.USER_ERROR,
      "0031",
      "This operation is not supported when getting records of a database without using an index",
      "",
      ""),
  GET_BUILD_ERROR_OPERATION_NOT_SUPPORTED_WHEN_GETTING_RECORDS_OF_DATABASE_USING_INDEX(
      Category.USER_ERROR,
      "0032",
      "This operation is not supported when getting records of a database by using an index",
      "",
      ""),
  SCAN_BUILD_ERROR_OPERATION_NOT_SUPPORTED_WHEN_SCANNING_ALL_RECORDS_OF_DATABASE_OR_SCANNING_RECORDS_OF_DATABASE_USING_INDEX(
      Category.USER_ERROR,
      "0033",
      "This operation is not supported when scanning all the records of a database "
          + "or scanning records of a database by using an index",
      "",
      ""),
  SCAN_BUILD_ERROR_OPERATION_SUPPORTED_ONLY_WHEN_SCANNING_RECORDS_OF_DATABASE_USING_INDEX(
      Category.USER_ERROR,
      "0034",
      "This operation is supported only when scanning records of a database by using an index",
      "",
      ""),
  SCAN_BUILD_ERROR_OPERATION_NOT_SUPPORTED_WHEN_SCANNING_RECORDS_OF_DATABASE_USING_INDEX(
      Category.USER_ERROR,
      "0035",
      "This operation is not supported when scanning records of a database by using an index",
      "",
      ""),
  SCAN_BUILD_ERROR_OPERATION_SUPPORTED_ONLY_WHEN_NO_CONDITIONS_ARE_SPECIFIED(
      Category.USER_ERROR,
      "0037",
      "This operation is supported only when no conditions are specified. "
          + "If you want to modify a condition, please use clearConditions() to remove all existing conditions first",
      "",
      ""),
  TABLE_METADATA_BUILD_ERROR_NO_COLUMNS_SPECIFIED(
      Category.USER_ERROR, "0038", "One or more columns must be specified.", "", ""),
  TABLE_METADATA_BUILD_ERROR_NO_PARTITION_KEYS_SPECIFIED(
      Category.USER_ERROR, "0039", "One or more partition keys must be specified.", "", ""),
  TABLE_METADATA_BUILD_ERROR_PARTITION_KEY_COLUMN_DEFINITION_NOT_SPECIFIED(
      Category.USER_ERROR,
      "0040",
      "The column definition must be specified since %s is specified as a partition key",
      "",
      ""),
  TABLE_METADATA_BUILD_ERROR_CLUSTERING_KEY_COLUMN_DEFINITION_NOT_SPECIFIED(
      Category.USER_ERROR,
      "0041",
      "The column definition must be specified since %s is specified as a clustering key",
      "",
      ""),
  TRANSACTION_STATE_INSTANTIATION_ERROR_INVALID_ID(
      Category.USER_ERROR, "0042", "Invalid ID specified. ID: %d", "", ""),
  TRANSACTION_NOT_ACTIVE(
      Category.USER_ERROR, "0043", "The transaction is not active. Status: %s", "", ""),
  TRANSACTION_ALREADY_COMMITTED_OR_ROLLED_BACK(
      Category.USER_ERROR,
      "0044",
      "The transaction has already been committed or rolled back. Status: %s",
      "",
      ""),
  TRANSACTION_NOT_PREPARED(
      Category.USER_ERROR, "0045", "The transaction has not been prepared. Status: %s", "", ""),
  TRANSACTION_NOT_PREPARED_OR_VALIDATED(
      Category.USER_ERROR,
      "0046",
      "The transaction has not been prepared or validated. Status: %s",
      "",
      ""),
  TRANSACTION_ALREADY_EXISTS(Category.USER_ERROR, "0047", "The transaction already exists", "", ""),
  TRANSACTION_NOT_FOUND(
      Category.USER_ERROR,
      "0048",
      "A transaction associated with the specified transaction ID is not found. "
          + "The transaction might have expired",
      "",
      ""),
  SYSTEM_NAMESPACE_SPECIFIED(
      Category.USER_ERROR, "0049", "%s is the system namespace name", "", ""),
  NAMESPACE_ALREADY_EXISTS(
      Category.USER_ERROR, "0050", "The namespace already exists. Namespace: %s", "", ""),
  NAMESPACE_NOT_FOUND(
      Category.USER_ERROR, "0051", "The namespace does not exist. Namespace: %s", "", ""),
  TABLE_ALREADY_EXISTS(Category.USER_ERROR, "0052", "The table already exists. Table: %s", "", ""),
  NAMESPACE_NOT_EMPTY(
      Category.USER_ERROR,
      "0053",
      "The namespace is not empty. Namespace: %s; Tables in the namespace: %s",
      "",
      ""),
  COLUMN_NOT_FOUND2(
      Category.USER_ERROR, "0054", "The column does not exist. Table: %s; Column: %s", "", ""),
  INDEX_ALREADY_EXISTS(
      Category.USER_ERROR, "0055", "The index already exists. Table: %s; Column: %s", "", ""),
  INDEX_NOT_FOUND(
      Category.USER_ERROR, "0056", "The index does not exist. Table: %s; Column: %s", "", ""),
  COLUMN_ALREADY_EXISTS(
      Category.USER_ERROR, "0057", "The column already exists. Table: %s; Column: %s", "", ""),
  OPERATION_DOES_NOT_HAVE_TARGET_NAMESPACE_OR_TABLE_NAME(
      Category.USER_ERROR,
      "0058",
      "The operation does not have the target namespace or table name. Operation: %s",
      "",
      ""),
  CONFIG_UTILS_INVALID_NUMBER_FORMAT(
      Category.USER_ERROR,
      "0059",
      "The specified value of the property '%s' is not a number. Value: %s",
      "",
      ""),
  CONFIG_UTILS_INVALID_BOOLEAN_FORMAT(
      Category.USER_ERROR,
      "0060",
      "The specified value of the property '%s' is not a boolean. Value: %s",
      "",
      ""),
  CONFIG_UTILS_READING_FILE_FAILED(
      Category.USER_ERROR, "0061", "Reading the file failed. File: %s", "", ""),
  CROSS_PARTITION_SCAN_MUST_BE_ENABLED_TO_USE_CROSS_PARTITION_SCAN_WITH_FILTERING_OR_ORDERING(
      Category.USER_ERROR,
      "0062",
      "The property 'scalar.db.cross_partition_scan.enabled' must be set to true "
          + "to use cross-partition scan with filtering or ordering",
      "",
      ""),
  OUT_OF_RANGE_COLUMN_VALUE_FOR_BIGINT(
      Category.USER_ERROR,
      "0063",
      "This column value is out of range for BigInt. Value: %s",
      "",
      ""),
  KEY_BUILD_ERROR_UNSUPPORTED_TYPE(
      Category.USER_ERROR, "0064", "This type is not supported. Name: %s, Type: %s", "", ""),
  STORAGE_NOT_FOUND(Category.USER_ERROR, "0065", "Storage '%s' is not found", "", ""),
  TRANSACTION_MANAGER_NOT_FOUND(
      Category.USER_ERROR, "0066", "Transaction manager '%s' is not found", "", ""),
  GET_OPERATION_USED_FOR_NON_EXACT_MATCH_SELECTION(
      Category.USER_ERROR,
      "0068",
      "Please use scan() for non-exact match selection. Operation: %s",
      "",
      ""),
  CASSANDRA_IMPORT_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0069",
      "Import-related functionality is not supported in Cassandra",
      "",
      ""),
  CASSANDRA_NETWORK_STRATEGY_NOT_FOUND(
      Category.USER_ERROR, "0070", "The %s network strategy does not exist", "", ""),
  INVALID_CONTACT_PORT(
      Category.USER_ERROR,
      "0071",
      "The property 'scalar.db.contact_port' must be greater than or equal to zero",
      "",
      ""),
  COSMOS_CLUSTERING_KEY_BLOB_TYPE_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0073",
      "The BLOB type is not supported for clustering keys in Cosmos DB. Column: %s",
      "",
      ""),
  COSMOS_IMPORT_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0074",
      "Import-related functionality is not supported in Cosmos DB",
      "",
      ""),
  INVALID_CONTACT_POINTS(
      Category.USER_ERROR,
      "0075",
      "The property 'scalar.db.contact_points' must not be empty",
      "",
      ""),
  COSMOS_CONDITION_OPERATION_NOT_SUPPORTED_FOR_BLOB_TYPE(
      Category.USER_ERROR,
      "0076",
      "Cosmos DB supports only EQ, NE, IS_NULL, and IS_NOT_NULL operations for the BLOB type in conditions. Mutation: %s",
      "",
      ""),
  INVALID_CONSISTENCY_LEVEL(
      Category.USER_ERROR,
      "0077",
      "The specified consistency level is not supported. Consistency level: %s",
      "",
      ""),
  DYNAMO_ENCODER_0X00_BYTES_NOT_ACCEPTED_IN_BLOB_VALUES_IN_DESC_ORDER(
      Category.USER_ERROR,
      "0078",
      "0x00 bytes are not accepted in BLOB values in DESC order",
      "",
      ""),
  DYNAMO_ENCODER_CANNOT_ENCODE_TEXT_VALUE_CONTAINING_0X0000(
      Category.USER_ERROR, "0079", "Cannot encode a Text value that contains '\\u0000'", "", ""),
  DYNAMO_INDEX_COLUMN_CANNOT_BE_SET_TO_NULL_OR_EMPTY(
      Category.USER_ERROR,
      "0081",
      "An index column cannot be set to null or an empty value for Text or Blob in DynamoDB. Operation: %s",
      "",
      ""),
  DYNAMO_CONDITION_OPERATION_NOT_SUPPORTED_FOR_BOOLEAN_TYPE(
      Category.USER_ERROR,
      "0082",
      "DynamoDB supports only EQ, NE, IS_NULL, and IS_NOT_NULL operations for the BOOLEAN type in conditions. Mutation: %s",
      "",
      ""),
  MULTI_STORAGE_NESTED_MULTI_STORAGE_DEFINITION_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0083",
      "Nested multi-storage definitions are not supported. Storage: %s",
      "",
      ""),
  MULTI_STORAGE_STORAGE_NOT_FOUND(
      Category.USER_ERROR, "0084", "Storage not found. Storage: %s", "", ""),
  JDBC_NAMESPACE_NAME_NOT_ACCEPTABLE(
      Category.USER_ERROR, "0085", "The namespace name is not acceptable. Namespace: %s", "", ""),
  JDBC_TABLE_NAME_NOT_ACCEPTABLE(
      Category.USER_ERROR, "0086", "The table name is not acceptable. Table: %s", "", ""),
  JDBC_IMPORT_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0087",
      "Importing tables is not allowed in the RDB engine. RDB engine: %s",
      "",
      ""),
  JDBC_IMPORT_TABLE_WITHOUT_PRIMARY_KEY(
      Category.USER_ERROR, "0088", "The %s table must have a primary key", "", ""),
  JDBC_RDB_ENGINE_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0089",
      "The RDB engine is not supported. JDBC connection URL: %s",
      "",
      ""),
  JDBC_IMPORT_DATA_TYPE_WITH_SIZE_NOT_SUPPORTED(
      Category.USER_ERROR, "0090", "Data type %s(%d) is not supported: %s", "", ""),
  JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED(
      Category.USER_ERROR, "0091", "Data type %s is not supported: %s", "", ""),
  JDBC_TRANSACTION_GETTING_TRANSACTION_STATE_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0092",
      "Getting a transaction state is not supported in JDBC transactions",
      "",
      ""),
  JDBC_TRANSACTION_ROLLING_BACK_TRANSACTION_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0093",
      "Rolling back a transaction is not supported in JDBC transactions",
      "",
      ""),
  CONSENSUS_COMMIT_COORDINATOR_TABLES_ALREADY_EXIST(
      Category.USER_ERROR, "0094", "Coordinator tables already exist", "", ""),
  CONSENSUS_COMMIT_COORDINATOR_TABLES_NOT_FOUND(
      Category.USER_ERROR, "0095", "Coordinator tables do not exist", "", ""),
  CONSENSUS_COMMIT_COORDINATOR_NAMESPACE_SPECIFIED(
      Category.USER_ERROR,
      "0096",
      "The namespace %s is reserved. Any operations on this namespace are not allowed",
      "",
      ""),
  CONSENSUS_COMMIT_MUTATING_TRANSACTION_METADATA_COLUMNS_NOT_ALLOWED(
      Category.USER_ERROR,
      "0097",
      "Mutating transaction metadata columns is not allowed. Table: %s; Column: %s",
      "",
      ""),
  CONSENSUS_COMMIT_CONDITION_NOT_ALLOWED_ON_PUT(
      Category.USER_ERROR, "0098", "A %s condition is not allowed on Put operations", "", ""),
  CONSENSUS_COMMIT_CONDITION_NOT_ALLOWED_ON_DELETE(
      Category.USER_ERROR, "0099", "A %s condition is not allowed on Delete operations", "", ""),
  CONSENSUS_COMMIT_CONDITION_NOT_ALLOWED_TO_TARGET_TRANSACTION_METADATA_COLUMNS(
      Category.USER_ERROR,
      "0100",
      "The condition is not allowed to target transaction metadata columns. Column: %s",
      "",
      ""),
  CONSENSUS_COMMIT_COLUMN_RESERVED_AS_TRANSACTION_METADATA(
      Category.USER_ERROR, "0101", "The column '%s' is reserved as transaction metadata", "", ""),
  CONSENSUS_COMMIT_BEFORE_PREFIXED_COLUMN_FOR_NON_PRIMARY_KEY_RESERVED_AS_TRANSACTION_METADATA(
      Category.USER_ERROR,
      "0102",
      "Non-primary key columns with the 'before_' prefix, '%s', are reserved as transaction metadata",
      "",
      ""),
  CONSENSUS_COMMIT_PUT_CANNOT_HAVE_CONDITION_WHEN_TARGET_RECORD_UNREAD_AND_IMPLICIT_PRE_READ_DISABLED(
      Category.USER_ERROR,
      "0103",
      "Put cannot have a condition when the target record is unread and implicit pre-read is disabled."
          + " Please read the target record beforehand or enable implicit pre-read: %s",
      "",
      ""),
  CONSENSUS_COMMIT_WRITING_ALREADY_DELETED_DATA_NOT_ALLOWED(
      Category.USER_ERROR,
      "0104",
      "Writing data already-deleted by the same transaction is not allowed",
      "",
      ""),
  CONSENSUS_COMMIT_SCANNING_ALREADY_WRITTEN_OR_DELETED_DATA_NOT_ALLOWED(
      Category.USER_ERROR,
      "0106",
      "Scanning data already-written or already-deleted by the same transaction is not allowed",
      "",
      ""),
  CONSENSUS_COMMIT_TRANSACTION_NOT_VALIDATED_IN_SERIALIZABLE(
      Category.USER_ERROR,
      "0107",
      "The transaction is not validated."
          + " When using the SERIALIZABLE isolation level, you need to call validate()"
          + " before calling commit()",
      "",
      ""),
  DYNAMO_BATCH_SIZE_EXCEEDED(
      Category.USER_ERROR, "0108", "DynamoDB cannot batch more than 100 mutations at once", "", ""),
  SCHEMA_LOADER_ALTERING_PARTITION_KEYS_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0109",
      "The partition keys of the table %s.%s were modified, but altering partition keys is not supported",
      "",
      ""),
  SCHEMA_LOADER_ALTERING_CLUSTERING_KEYS_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0110",
      "The clustering keys of the table %s.%s were modified, but altering clustering keys is not supported",
      "",
      ""),
  SCHEMA_LOADER_ALTERING_CLUSTERING_ORDER_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0111",
      "The clustering ordering of the table %s.%s were modified, but altering clustering ordering is not supported",
      "",
      ""),
  SCHEMA_LOADER_DELETING_COLUMN_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0112",
      "The column %s of the table %s.%s has been deleted. Column deletion is not supported when altering a table",
      "",
      ""),
  SCHEMA_LOADER_ALTERING_COLUMN_DATA_TYPE_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0113",
      "The data type of the column %s of the table %s.%s was modified, but altering data types is not supported",
      "",
      ""),
  SCHEMA_LOADER_SPECIFYING_SCHEMA_FILE_REQUIRED_WHEN_USING_REPAIR_ALL(
      Category.USER_ERROR,
      "0114",
      "Specifying the '--schema-file' option is required when using the '--repair-all' option",
      "",
      ""),
  SCHEMA_LOADER_SPECIFYING_SCHEMA_FILE_REQUIRED_WHEN_USING_ALTER(
      Category.USER_ERROR,
      "0115",
      "Specifying the '--schema-file' option is required when using the '--alter' option",
      "",
      ""),
  SCHEMA_LOADER_SPECIFYING_SCHEMA_FILE_REQUIRED_WHEN_USING_IMPORT(
      Category.USER_ERROR,
      "0116",
      "Specifying the '--schema-file' option is required when using the '--import' option",
      "",
      ""),
  SCHEMA_LOADER_SPECIFYING_COORDINATOR_WITH_IMPORT_NOT_ALLOWED(
      Category.USER_ERROR,
      "0117",
      "Specifying the '--coordinator' option with the '--import' option is not allowed."
          + " Create Coordinator tables separately",
      "",
      ""),
  SCHEMA_LOADER_READING_CONFIG_FILE_FAILED(
      Category.USER_ERROR, "0118", "Reading the configuration file failed. File: %s", "", ""),
  SCHEMA_LOADER_READING_SCHEMA_FILE_FAILED(
      Category.USER_ERROR, "0119", "Reading the schema file failed. File: %s", "", ""),
  SCHEMA_LOADER_PARSING_SCHEMA_JSON_FAILED(
      Category.USER_ERROR, "0120", "Parsing the schema JSON failed. Details: %s", "", ""),
  SCHEMA_LOADER_PARSE_ERROR_TABLE_NAME_MUST_CONTAIN_NAMESPACE_AND_TABLE(
      Category.USER_ERROR,
      "0121",
      "The table name must contain the namespace and the table. Table: %s",
      "",
      ""),
  SCHEMA_LOADER_PARSE_ERROR_PARTITION_KEY_MUST_BE_SPECIFIED(
      Category.USER_ERROR, "0122", "The partition key must be specified. Table: %s", "", ""),
  SCHEMA_LOADER_PARSE_ERROR_INVALID_CLUSTERING_KEY_FORMAT(
      Category.USER_ERROR,
      "0123",
      "Invalid clustering-key format. The clustering key must be in the format of 'column_name' or 'column_name ASC/DESC'."
          + " Table: %s; Clustering key: %s",
      "",
      ""),
  SCHEMA_LOADER_PARSE_ERROR_COLUMNS_NOT_SPECIFIED(
      Category.USER_ERROR, "0124", "Columns must be specified. Table: %s", "", ""),
  SCHEMA_LOADER_PARSE_ERROR_INVALID_COLUMN_TYPE(
      Category.USER_ERROR, "0125", "Invalid column type. Table: %s; Column: %s; Type: %s", "", ""),
  OPERATION_CHECK_ERROR_UNSUPPORTED_MUTATION_TYPE(
      Category.USER_ERROR,
      "0126",
      "The mutation type is not supported. Only the Put or Delete type is supported. Mutation: %s",
      "",
      ""),
  CONDITION_BUILD_ERROR_CONDITION_NOT_ALLOWED_FOR_UPDATE_IF(
      Category.USER_ERROR,
      "0127",
      "This condition is not allowed for the UpdateIf operation. Condition: %s",
      "",
      ""),
  CASSANDRA_CROSS_PARTITION_SCAN_WITH_ORDERING_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0128",
      "Cross-partition scan with ordering is not supported in Cassandra",
      "",
      ""),
  COSMOS_CROSS_PARTITION_SCAN_WITH_ORDERING_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0129",
      "Cross-partition scan with ordering is not supported in Cosmos DB",
      "",
      ""),
  DYNAMO_CROSS_PARTITION_SCAN_WITH_ORDERING_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0130",
      "Cross-partition scan with ordering is not supported in DynamoDB",
      "",
      ""),
  DATA_LOADER_DIRECTORY_WRITE_ACCESS_NOT_ALLOWED(
      Category.USER_ERROR,
      "0131",
      "The directory '%s' does not have write permissions. Please ensure that the current user has write access to the directory.",
      "",
      ""),
  DATA_LOADER_DIRECTORY_CREATE_FAILED(
      Category.USER_ERROR,
      "0132",
      "Failed to create the directory '%s'. Please check if you have sufficient permissions and if there are any file system restrictions. Details: %s",
      "",
      ""),
  DATA_LOADER_MISSING_DIRECTORY_NOT_ALLOWED(
      Category.USER_ERROR, "0133", "Directory path cannot be null or empty.", "", ""),
  DATA_LOADER_MISSING_FILE_EXTENSION(
      Category.USER_ERROR,
      "0134",
      "No file extension was found on the provided file name %s.",
      "",
      ""),
  DATA_LOADER_INVALID_FILE_EXTENSION(
      Category.USER_ERROR,
      "0135",
      "Invalid file extension: %s. Allowed extensions are: %s",
      "",
      ""),
  SINGLE_CRUD_OPERATION_TRANSACTION_GETTING_TRANSACTION_STATE_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0136",
      "Getting a transaction state is not supported in single CRUD operation transactions",
      "",
      ""),
  SINGLE_CRUD_OPERATION_TRANSACTION_ROLLING_BACK_TRANSACTION_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0137",
      "Rolling back a transaction is not supported in single CRUD operation transactions",
      "",
      ""),
  SINGLE_CRUD_OPERATION_TRANSACTION_MULTIPLE_MUTATIONS_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0138",
      "Multiple mutations are not supported in single CRUD operation transactions",
      "",
      ""),
  SINGLE_CRUD_OPERATION_TRANSACTION_BEGINNING_TRANSACTION_NOT_ALLOWED(
      Category.USER_ERROR,
      "0139",
      "Beginning a transaction is not allowed in single CRUD operation transactions",
      "",
      ""),
  SINGLE_CRUD_OPERATION_TRANSACTION_RESUMING_TRANSACTION_NOT_ALLOWED(
      Category.USER_ERROR,
      "0140",
      "Resuming a transaction is not allowed in single CRUD operation transactions",
      "",
      ""),
  CONSENSUS_COMMIT_GROUP_COMMIT_WITH_TWO_PHASE_COMMIT_INTERFACE_NOT_ALLOWED(
      Category.USER_ERROR,
      "0141",
      "Using the group commit feature on the Coordinator table with a two-phase commit interface is not allowed",
      "",
      ""),
  GET_BUILD_ERROR_OPERATION_SUPPORTED_ONLY_WHEN_NO_CONDITIONS_ARE_SPECIFIED(
      Category.USER_ERROR,
      "0142",
      "This operation is supported only when no conditions are specified. "
          + "If you want to modify a condition, please use clearConditions() to remove all existing conditions first",
      "",
      ""),
  ENCRYPTION_NOT_ENABLED(
      Category.USER_ERROR,
      "0143",
      "The encryption feature is not enabled. To encrypt data at rest, you must enable this feature. Note that this feature is supported only in the ScalarDB Enterprise edition",
      "",
      ""),
  INVALID_VARIABLE_KEY_COLUMN_SIZE(
      Category.USER_ERROR,
      "0144",
      "The variable key column size must be greater than or equal to 64",
      "",
      ""),
  COSMOS_PRIMARY_KEY_CONTAINS_ILLEGAL_CHARACTER(
      Category.USER_ERROR,
      "0145",
      "The value of the column %s in the primary key contains an illegal character. "
          + "Primary-key columns must not contain any of the following characters in Cosmos DB: ':', '/', '\\', '#', '?'. Value: %s",
      "",
      ""),
  CONSENSUS_COMMIT_INSERTING_ALREADY_WRITTEN_DATA_NOT_ALLOWED(
      Category.USER_ERROR,
      "0146",
      "Inserting data already-written by the same transaction is not allowed",
      "",
      ""),
  CONSENSUS_COMMIT_DELETING_ALREADY_INSERTED_DATA_NOT_ALLOWED(
      Category.USER_ERROR,
      "0147",
      "Deleting data already-inserted by the same transaction is not allowed",
      "",
      ""),
  DATA_LOADER_INVALID_COLUMN_NON_EXISTENT(
      Category.USER_ERROR,
      "0148",
      "Invalid key: Column %s does not exist in the table %s in namespace %s.",
      "",
      ""),
  DATA_LOADER_INVALID_BASE64_ENCODING_FOR_COLUMN_VALUE(
      Category.USER_ERROR,
      "0149",
      "Invalid base64 encoding for blob value for column %s in table %s in namespace %s",
      "",
      ""),
  DATA_LOADER_INVALID_NUMBER_FORMAT_FOR_COLUMN_VALUE(
      Category.USER_ERROR,
      "0150",
      "Invalid number specified for column %s in table %s in namespace %s",
      "",
      ""),
  DATA_LOADER_ERROR_METHOD_NULL_ARGUMENT(
      Category.USER_ERROR, "0151", "Method null argument not allowed", "", ""),
  ABAC_NOT_ENABLED(
      Category.USER_ERROR,
      "0152",
      "The attribute-based access control feature is not enabled. To use this feature, you must enable it. Note that this feature is supported only in the ScalarDB Enterprise edition",
      "",
      ""),
  DATA_LOADER_CLUSTERING_KEY_NOT_FOUND(
      Category.USER_ERROR, "0153", "The provided clustering key %s was not found", "", ""),
  DATA_LOADER_INVALID_PROJECTION(
      Category.USER_ERROR, "0154", "The column '%s' was not found", "", ""),
  DATA_LOADER_INCOMPLETE_PARTITION_KEY(
      Category.USER_ERROR,
      "0155",
      "The provided partition key is incomplete. Required key: %s",
      "",
      ""),
  DATA_LOADER_CLUSTERING_KEY_ORDER_MISMATCH(
      Category.USER_ERROR,
      "0156",
      "The provided clustering key order does not match the table schema. Required order: %s",
      "",
      ""),
  DATA_LOADER_PARTITION_KEY_ORDER_MISMATCH(
      Category.USER_ERROR,
      "0157",
      "The provided partition key order does not match the table schema. Required order: %s",
      "",
      ""),
  OUT_OF_RANGE_COLUMN_VALUE_FOR_DATE(
      Category.USER_ERROR,
      "0158",
      "This DATE column value is out of the valid range. It must be between 1000-01-01 and 9999-12-12. Value: %s",
      "",
      ""),
  SUBMICROSECOND_PRECISION_NOT_SUPPORTED_FOR_TIME(
      Category.USER_ERROR,
      "0159",
      "This TIME column value precision cannot be shorter than one microsecond. Value: %s",
      "",
      ""),
  OUT_OF_RANGE_COLUMN_VALUE_FOR_TIMESTAMP(
      Category.USER_ERROR,
      "0160",
      "This TIMESTAMP column value is out of the valid range. It must be between 1000-01-01T00:00:00.000 and 9999-12-31T23:59:59.999. Value: %s",
      "",
      ""),
  SUBMILLISECOND_PRECISION_NOT_SUPPORTED_FOR_TIMESTAMP(
      Category.USER_ERROR,
      "0161",
      "This TIMESTAMP column value precision cannot be shorter than one millisecond. Value: %s",
      "",
      ""),
  OUT_OF_RANGE_COLUMN_VALUE_FOR_TIMESTAMPTZ(
      Category.USER_ERROR,
      "0162",
      "This TIMESTAMPTZ column value is out of the valid range. It must be between 1000-01-01T00:00:00.000Z to 9999-12-31T23:59:59.999Z. Value: %s",
      "",
      ""),
  SUBMILLISECOND_PRECISION_NOT_SUPPORTED_FOR_TIMESTAMPTZ(
      Category.USER_ERROR,
      "0163",
      "This TIMESTAMPTZ column value precision cannot be shorter than one millisecond. Value: %s",
      "",
      ""),
  JDBC_IMPORT_DATA_TYPE_OVERRIDE_NOT_SUPPORTED(
      Category.USER_ERROR,
      "0164",
      "The underlying-storage data type %s is not supported as the ScalarDB %s data type: %s",
      "",
      ""),
  DATA_LOADER_MISSING_NAMESPACE_OR_TABLE(
      Category.USER_ERROR, "0165", "Missing namespace or table: %s, %s", "", ""),
  DATA_LOADER_TABLE_METADATA_RETRIEVAL_FAILED(
      Category.USER_ERROR, "0166", "Failed to retrieve table metadata. Details: %s", "", ""),
  DATA_LOADER_DUPLICATE_DATA_MAPPINGS(
      Category.USER_ERROR,
      "0167",
      "Duplicate data mappings found for table '%s' in the control file",
      "",
      ""),
  DATA_LOADER_MISSING_COLUMN_MAPPING(
      Category.USER_ERROR,
      "0168",
      "No mapping found for column '%s' in table '%s' in the control file. Control file validation set at 'FULL'. All columns need to be mapped.",
      "",
      ""),
  DATA_LOADER_CONTROL_FILE_MISSING_DATA_MAPPINGS(
      Category.USER_ERROR, "0169", "The control file is missing data mappings", "", ""),
  DATA_LOADER_TARGET_COLUMN_NOT_FOUND(
      Category.USER_ERROR,
      "0170",
      "The target column '%s' for source field '%s' could not be found in table '%s'",
      "",
      ""),
  DATA_LOADER_MISSING_PARTITION_KEY(
      Category.USER_ERROR,
      "0171",
      "The required partition key '%s' is missing in the control file mapping for table '%s'",
      "",
      ""),
  DATA_LOADER_MISSING_CLUSTERING_KEY(
      Category.USER_ERROR,
      "0172",
      "The required clustering key '%s' is missing in the control file mapping for table '%s'",
      "",
      ""),
  DATA_LOADER_MULTIPLE_MAPPINGS_FOR_COLUMN_FOUND(
      Category.USER_ERROR,
      "0173",
      "Duplicated data mappings found for column '%s' in table '%s'",
      "",
      ""),
  DATA_LOADER_MISSING_CLUSTERING_KEY_COLUMN(
      Category.USER_ERROR,
      "0174",
      "Missing required field or column mapping for clustering key %s",
      "",
      ""),
  DATA_LOADER_MISSING_PARTITION_KEY_COLUMN(
      Category.USER_ERROR,
      "0175",
      "Missing required field or column mapping for partition key %s",
      "",
      ""),
  DATA_LOADER_MISSING_COLUMN(
      Category.USER_ERROR, "0176", "Missing field or column mapping for %s", "", ""),
  DATA_LOADER_VALUE_TO_STRING_CONVERSION_FAILED(
      Category.USER_ERROR,
      "0177",
      "Something went wrong while converting the ScalarDB values to strings. The table metadata and Value datatype probably do not match. Details: %s",
      "",
      ""),
  DATA_LOADER_FILE_FORMAT_NOT_SUPPORTED(
      Category.USER_ERROR, "0178", "The provided file format is not supported : %s", "", ""),
  DATA_LOADER_COULD_NOT_FIND_PARTITION_KEY(
      Category.USER_ERROR, "0179", "Could not find the partition key", "", ""),
  DATA_LOADER_UPSERT_INSERT_MISSING_COLUMNS(
      Category.USER_ERROR,
      "0180",
      "The source record needs to contain all fields if the UPSERT turns into an INSERT",
      "",
      ""),
  DATA_LOADER_DATA_ALREADY_EXISTS(Category.USER_ERROR, "0181", "Record already exists", "", ""),
  DATA_LOADER_DATA_NOT_FOUND(Category.USER_ERROR, "0182", "Record was not found", "", ""),
  DATA_LOADER_COULD_NOT_FIND_CLUSTERING_KEY(
      Category.USER_ERROR, "0183", "Could not find the clustering key", "", ""),
  DATA_LOADER_TABLE_METADATA_MISSING(
      Category.USER_ERROR, "0184", "No table metadata found", "", ""),
  DATA_LOADER_MISSING_SOURCE_FIELD(
      Category.USER_ERROR,
      "0185",
      "The data mapping source field '%s' for table '%s' is missing in the json data record",
      "",
      ""),
  DATA_LOADER_CSV_DATA_MISMATCH(
      Category.USER_ERROR, "0186", "The CSV row: %s does not match header: %s.", "", ""),
  DATA_LOADER_JSON_CONTENT_START_ERROR(
      Category.USER_ERROR, "0187", "Expected JSON file content to be an array", "", ""),
  REPLICATION_NOT_ENABLED(
      Category.USER_ERROR,
      "0188",
      // TODO: Update the message once the licence type is determined.
      "The replication feature is not enabled. To use this feature, you must enable it",
      "",
      ""),
  DATA_LOADER_IMPORT_TARGET_MISSING(
      Category.USER_ERROR,
      "0189",
      "Missing option: either '--namespace' and'--table' or '--control-file' options must be specified.",
      "",
      ""),
  DATA_LOADER_MISSING_IMPORT_FILE(
      Category.USER_ERROR,
      "0190",
      "The file '%s' specified by the argument '%s' does not exist.",
      "",
      ""),
  DATA_LOADER_LOG_DIRECTORY_WRITE_ACCESS_DENIED(
      Category.USER_ERROR, "0191", "Cannot write to the log directory: %s", "", ""),
  DATA_LOADER_LOG_DIRECTORY_CREATION_FAILED(
      Category.USER_ERROR, "0192", "Failed to create the log directory: %s", "", ""),
  DATA_LOADER_INVALID_CONTROL_FILE(
      Category.USER_ERROR, "0193", "Failed to parse the control file: %s", "", ""),
  DATA_LOADER_DIRECTORY_WRITE_ACCESS(
      Category.USER_ERROR,
      "0194",
      "No permission to create or write files in the directory: %s",
      "",
      ""),
  DATA_LOADER_DIRECTORY_CREATION_FAILED(
      Category.USER_ERROR, "0195", "Failed to create the directory: %s", "", ""),
  DATA_LOADER_PATH_IS_NOT_A_DIRECTORY(
      Category.USER_ERROR, "0196", "Path exists but is not a directory: %s", "", ""),
  DATA_LOADER_FILE_PATH_IS_BLANK(
      Category.USER_ERROR, "0197", "File path must not be blank.", "", ""),
  DATA_LOADER_FILE_NOT_FOUND(Category.USER_ERROR, "0198", "File not found: %s", "", ""),
  DATA_LOADER_INVALID_DATE_TIME_FOR_COLUMN_VALUE(
      Category.USER_ERROR,
      "0199",
      "Invalid date time value specified for column %s in table %s in namespace %s.",
      "",
      ""),
  DATA_LOADER_NULL_OR_EMPTY_KEY_VALUE_INPUT(
      Category.USER_ERROR, "0200", "Key-value cannot be null or empty", "", ""),
  DATA_LOADER_INVALID_KEY_VALUE_INPUT(
      Category.USER_ERROR, "0201", "Invalid key-value format: %s", "", ""),
  DATA_LOADER_INVALID_COLUMN_ORDER_FORMAT(
      Category.USER_ERROR, "0202", "Invalid column order format: %s", "", ""),

  DATA_LOADER_SPLIT_INPUT_VALUE_NULL(Category.USER_ERROR, "0203", "Value must not be null", "", ""),

  DATA_LOADER_SPLIT_INPUT_DELIMITER_NULL(
      Category.USER_ERROR, "0204", "Delimiter must not be null", "", ""),
  DATA_LOADER_CONFIG_FILE_PATH_BLANK(
      Category.USER_ERROR, "0205", "Config file path must not be blank", "", ""),

  //
  // Errors for the concurrency error category
  //
  NO_MUTATION_APPLIED(Category.CONCURRENCY_ERROR, "0000", "No mutation was applied", "", ""),
  CASSANDRA_LOGGING_FAILED_IN_BATCH(
      Category.CONCURRENCY_ERROR, "0001", "Logging failed in the batch", "", ""),
  CASSANDRA_OPERATION_FAILED_IN_BATCH(
      Category.CONCURRENCY_ERROR, "0002", "The operation failed in the batch with type %s", "", ""),
  CASSANDRA_ERROR_OCCURRED_IN_BATCH(
      Category.CONCURRENCY_ERROR, "0003", "An error occurred in the batch. Details: %s", "", ""),
  CASSANDRA_WRITE_TIMEOUT_IN_PAXOS_PHASE_IN_MUTATION(
      Category.CONCURRENCY_ERROR, "0004", "A Paxos phase in the CAS operation failed", "", ""),
  CASSANDRA_WRITE_TIMEOUT_IN_LEARN_PHASE_IN_MUTATION(
      Category.CONCURRENCY_ERROR, "0005", "The learn phase in the CAS operation failed", "", ""),
  CASSANDRA_WRITE_TIMEOUT_SIMPLE_WRITE_OPERATION_FAILED_IN_MUTATION(
      Category.CONCURRENCY_ERROR, "0006", "A simple write operation failed", "", ""),
  CASSANDRA_ERROR_OCCURRED_IN_MUTATION(
      Category.CONCURRENCY_ERROR, "0007", "An error occurred in the mutation. Details: %s", "", ""),
  COSMOS_RETRY_WITH_ERROR_OCCURRED_IN_MUTATION(
      Category.CONCURRENCY_ERROR,
      "0008",
      "A RetryWith error occurred in the mutation. Details: %s",
      "",
      ""),
  DYNAMO_TRANSACTION_CONFLICT_OCCURRED_IN_MUTATION(
      Category.CONCURRENCY_ERROR,
      "0009",
      "A transaction conflict occurred in the mutation. Details: %s",
      "",
      ""),
  JDBC_TRANSACTION_CONFLICT_OCCURRED_IN_MUTATION(
      Category.CONCURRENCY_ERROR,
      "0010",
      "A transaction conflict occurred in the mutation. Details: %s",
      "",
      ""),
  JDBC_TRANSACTION_CONFLICT_OCCURRED(
      Category.CONCURRENCY_ERROR,
      "0011",
      "A conflict occurred. Please try restarting the transaction. Details: %s",
      "",
      ""),
  JDBC_TRANSACTION_CONDITION_NOT_SATISFIED(
      Category.CONCURRENCY_ERROR,
      "0012",
      "The %s condition of the %s operation is not satisfied. Targeting column(s): %s",
      "",
      ""),
  CONSENSUS_COMMIT_PREPARING_RECORD_EXISTS(
      Category.CONCURRENCY_ERROR, "0013", "The record being prepared already exists", "", ""),
  CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_PREPARING_RECORDS(
      Category.CONCURRENCY_ERROR, "0014", "A conflict occurred when preparing records", "", ""),
  CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_COMMITTING_STATE(
      Category.CONCURRENCY_ERROR,
      "0015",
      "The committing state in the coordinator failed. The transaction has been aborted",
      "",
      ""),
  CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHILE_IMPLICIT_PRE_READ(
      Category.CONCURRENCY_ERROR, "0016", "A conflict occurred during implicit pre-read", "", ""),
  CONSENSUS_COMMIT_READ_UNCOMMITTED_RECORD(
      Category.CONCURRENCY_ERROR, "0017", "This record needs to be recovered", "", ""),
  CONSENSUS_COMMIT_CONDITION_NOT_SATISFIED_BECAUSE_RECORD_NOT_EXISTS(
      Category.CONCURRENCY_ERROR,
      "0018",
      "The record does not exist, so the %s condition is not satisfied",
      "",
      ""),
  CONSENSUS_COMMIT_CONDITION_NOT_SATISFIED_BECAUSE_RECORD_EXISTS(
      Category.CONCURRENCY_ERROR,
      "0019",
      "The record exists, so the %s condition is not satisfied",
      "",
      ""),
  CONSENSUS_COMMIT_CONDITION_NOT_SATISFIED(
      Category.CONCURRENCY_ERROR,
      "0020",
      "The condition on the column '%s' is not satisfied",
      "",
      ""),
  CONSENSUS_COMMIT_ANTI_DEPENDENCY_FOUND(
      Category.CONCURRENCY_ERROR,
      "0022",
      "An anti-dependency was found. The transaction has been aborted",
      "",
      ""),
  JDBC_TRANSACTION_CONFLICT_OCCURRED_IN_INSERT(
      Category.CONCURRENCY_ERROR,
      "0023",
      "A transaction conflict occurred in the Insert operation",
      "",
      ""),
  SINGLE_CRUD_OPERATION_TRANSACTION_CONDITION_NOT_SATISFIED(
      Category.CONCURRENCY_ERROR,
      "0024",
      "The %s condition of the %s operation is not satisfied. Targeting column(s): %s",
      "",
      ""),
  SINGLE_CRUD_OPERATION_TRANSACTION_CONFLICT_OCCURRED_IN_INSERT(
      Category.CONCURRENCY_ERROR,
      "0025",
      "A transaction conflict occurred in the Insert operation",
      "",
      ""),

  //
  // Errors for the internal error category
  //
  CREATING_NAMESPACE_FAILED(
      Category.INTERNAL_ERROR, "0000", "Creating the namespace failed. Namespace: %s", "", ""),
  DROPPING_NAMESPACE_FAILED(
      Category.INTERNAL_ERROR, "0001", "Dropping the namespace failed. Namespace: %s", "", ""),
  CREATING_TABLE_FAILED(
      Category.INTERNAL_ERROR, "0002", "Creating the table failed. Table: %s", "", ""),
  DROPPING_TABLE_FAILED(
      Category.INTERNAL_ERROR, "0003", "Dropping the table failed. Table: %s", "", ""),
  TRUNCATING_TABLE_FAILED(
      Category.INTERNAL_ERROR, "0004", "Truncating the table failed. Table: %s", "", ""),
  CREATING_INDEX_FAILED(
      Category.INTERNAL_ERROR, "0005", "Creating the index failed. Table: %s, Column: %s", "", ""),
  DROPPING_INDEX_FAILED(
      Category.INTERNAL_ERROR, "0006", "Dropping the index failed. Table: %s, Column: %s", "", ""),
  GETTING_TABLE_METADATA_FAILED(
      Category.INTERNAL_ERROR, "0007", "Getting the table metadata failed. Table: %s", "", ""),
  GETTING_TABLE_NAMES_IN_NAMESPACE_FAILED(
      Category.INTERNAL_ERROR,
      "0008",
      "Getting the table names in the namespace failed. Namespace: %s",
      "",
      ""),
  CHECKING_NAMESPACE_EXISTENCE_FAILED(
      Category.INTERNAL_ERROR,
      "0009",
      "Checking the namespace existence failed. Namespace: %s",
      "",
      ""),
  CHECKING_TABLE_EXISTENCE_FAILED(
      Category.INTERNAL_ERROR, "0010", "Checking the table existence failed. Table: %s", "", ""),
  CHECKING_INDEX_EXISTENCE_FAILED(
      Category.INTERNAL_ERROR,
      "0011",
      "Checking the index existence failed. Table: %s; Column: %s",
      "",
      ""),
  REPAIRING_NAMESPACE_FAILED(
      Category.INTERNAL_ERROR, "0012", "Repairing the namespace failed. Namespace: %s", "", ""),
  REPAIRING_TABLE_FAILED(
      Category.INTERNAL_ERROR, "0013", "Repairing the table failed. Table: %s", "", ""),
  ADDING_NEW_COLUMN_TO_TABLE_FAILED(
      Category.INTERNAL_ERROR,
      "0014",
      "Adding a new column to the table failed. Table: %s; Column: %s; ColumnType: %s",
      "",
      ""),
  GETTING_NAMESPACE_NAMES_FAILED(
      Category.INTERNAL_ERROR, "0015", "Getting the namespace names failed", "", ""),
  GETTING_IMPORT_TABLE_METADATA_FAILED(
      Category.INTERNAL_ERROR,
      "0016",
      "Getting the table metadata of the table being imported failed. Table: %s",
      "",
      ""),
  IMPORTING_TABLE_FAILED(
      Category.INTERNAL_ERROR, "0017", "Importing the table failed. Table: %s", "", ""),
  ADDING_RAW_COLUMN_TO_TABLE_FAILED(
      Category.INTERNAL_ERROR,
      "0018",
      "Adding the raw column to the table failed. Table: %s; Column: %s; ColumnType: %s",
      "",
      ""),
  UPGRADING_SCALAR_DB_ENV_FAILED(
      Category.INTERNAL_ERROR, "0019", "Upgrading the ScalarDB environment failed", "", ""),
  CASSANDRA_WRITE_TIMEOUT_WITH_OTHER_WRITE_TYPE_IN_MUTATION(
      Category.INTERNAL_ERROR,
      "0020",
      "Something wrong because WriteType is neither CAS nor SIMPLE",
      "",
      ""),
  CASSANDRA_ERROR_OCCURRED_IN_SELECTION(
      Category.INTERNAL_ERROR, "0021", "An error occurred in the selection. Details: %s", "", ""),
  COSMOS_ERROR_OCCURRED_IN_MUTATION(
      Category.INTERNAL_ERROR, "0022", "An error occurred in the mutation. Details: %s", "", ""),
  COSMOS_ERROR_OCCURRED_IN_SELECTION(
      Category.INTERNAL_ERROR, "0023", "An error occurred in the selection. Details: %s", "", ""),
  DYNAMO_ERROR_OCCURRED_IN_MUTATION(
      Category.INTERNAL_ERROR, "0024", "An error occurred in the mutation. Details: %s", "", ""),
  DYNAMO_ERROR_OCCURRED_IN_SELECTION(
      Category.INTERNAL_ERROR, "0025", "An error occurred in the selection. Details: %s", "", ""),
  JDBC_ERROR_OCCURRED_IN_MUTATION(
      Category.INTERNAL_ERROR, "0026", "An error occurred in the mutation. Details: %s", "", ""),
  JDBC_ERROR_OCCURRED_IN_SELECTION(
      Category.INTERNAL_ERROR, "0027", "An error occurred in the selection. Details: %s", "", ""),
  JDBC_FETCHING_NEXT_RESULT_FAILED(
      Category.INTERNAL_ERROR, "0028", "Fetching the next result failed", "", ""),
  JDBC_TRANSACTION_ROLLING_BACK_TRANSACTION_FAILED(
      Category.INTERNAL_ERROR, "0029", "Rolling back the transaction failed. Details: %s", "", ""),
  JDBC_TRANSACTION_COMMITTING_TRANSACTION_FAILED(
      Category.INTERNAL_ERROR, "0030", "Committing the transaction failed. Details: %s", "", ""),
  JDBC_TRANSACTION_GET_OPERATION_FAILED(
      Category.INTERNAL_ERROR, "0031", "The Get operation failed. Details: %s", "", ""),
  JDBC_TRANSACTION_SCAN_OPERATION_FAILED(
      Category.INTERNAL_ERROR, "0032", "The Scan operation failed. Details: %s", "", ""),
  JDBC_TRANSACTION_PUT_OPERATION_FAILED(
      Category.INTERNAL_ERROR, "0033", "The Put operation failed. Details: %s", "", ""),
  JDBC_TRANSACTION_DELETE_OPERATION_FAILED(
      Category.INTERNAL_ERROR, "0034", "The Delete operation failed. Details: %s", "", ""),
  JDBC_TRANSACTION_BEGINNING_TRANSACTION_FAILED(
      Category.INTERNAL_ERROR, "0035", "Beginning a transaction failed. Details: %s", "", ""),
  CONSENSUS_COMMIT_PREPARING_RECORDS_FAILED(
      Category.INTERNAL_ERROR, "0036", "Preparing records failed", "", ""),
  CONSENSUS_COMMIT_VALIDATION_FAILED(Category.INTERNAL_ERROR, "0037", "Validation failed", "", ""),
  CONSENSUS_COMMIT_EXECUTING_IMPLICIT_PRE_READ_FAILED(
      Category.INTERNAL_ERROR, "0038", "Executing implicit pre-read failed", "", ""),
  CONSENSUS_COMMIT_READING_RECORD_FROM_STORAGE_FAILED(
      Category.INTERNAL_ERROR,
      "0039",
      "Reading a record from the underlying storage failed",
      "",
      ""),
  CONSENSUS_COMMIT_SCANNING_RECORDS_FROM_STORAGE_FAILED(
      Category.INTERNAL_ERROR,
      "0040",
      "Scanning records from the underlying storage failed",
      "",
      ""),
  CONSENSUS_COMMIT_ROLLBACK_FAILED_BECAUSE_TRANSACTION_ALREADY_COMMITTED(
      Category.INTERNAL_ERROR,
      "0041",
      "Rollback failed because the transaction has already been committed",
      "",
      ""),
  CONSENSUS_COMMIT_ROLLBACK_FAILED(Category.INTERNAL_ERROR, "0042", "Rollback failed", "", ""),
  JDBC_TRANSACTION_INSERT_OPERATION_FAILED(
      Category.INTERNAL_ERROR, "0043", "The Insert operation failed. Details: %s", "", ""),
  JDBC_TRANSACTION_UPSERT_OPERATION_FAILED(
      Category.INTERNAL_ERROR, "0044", "The Upsert operation failed. Details: %s", "", ""),
  JDBC_TRANSACTION_UPDATE_OPERATION_FAILED(
      Category.INTERNAL_ERROR, "0045", "The Update operation failed. Details: %s", "", ""),
  HANDLING_BEFORE_PREPARATION_SNAPSHOT_HOOK_FAILED(
      Category.INTERNAL_ERROR,
      "0046",
      "Handling the before-preparation snapshot hook failed. Details: %s",
      "",
      ""),
  DATA_LOADER_ERROR_CRUD_EXCEPTION(
      Category.INTERNAL_ERROR,
      "0047",
      "Something went wrong while trying to save the data. Details: %s",
      "",
      ""),
  DATA_LOADER_ERROR_SCAN(
      Category.INTERNAL_ERROR,
      "0048",
      "Something went wrong while scanning. Are you sure you are running in the correct transaction mode? Details: %s",
      "",
      ""),
  DATA_LOADER_CSV_FILE_READ_FAILED(
      Category.INTERNAL_ERROR, "0049", "Failed to read CSV file. Details: %s.", "", ""),
  DATA_LOADER_CSV_FILE_HEADER_READ_FAILED(
      Category.INTERNAL_ERROR, "0050", "Failed to CSV read header line. Details: %s.", "", ""),
  DATA_LOADER_DATA_CHUNK_PROCESS_FAILED(
      Category.INTERNAL_ERROR,
      "0051",
      "Data chunk processing was interrupted. Details: %s",
      "",
      ""),
  DATA_LOADER_JSON_FILE_READ_FAILED(
      Category.INTERNAL_ERROR, "0052", "Failed to read JSON file. Details: %s.", "", ""),
  DATA_LOADER_JSONLINES_FILE_READ_FAILED(
      Category.INTERNAL_ERROR, "0053", "Failed to read JSON Lines file. Details: %s.", "", ""),

  //
  // Errors for the unknown transaction status error category
  //
  JDBC_TRANSACTION_UNKNOWN_TRANSACTION_STATUS(
      Category.UNKNOWN_TRANSACTION_STATUS_ERROR,
      "0000",
      "Rolling back the transaction failed. Details: %s",
      "",
      ""),
  CONSENSUS_COMMIT_COMMITTING_STATE_FAILED_WITH_NO_MUTATION_EXCEPTION_BUT_COORDINATOR_STATUS_DOES_NOT_EXIST(
      Category.UNKNOWN_TRANSACTION_STATUS_ERROR,
      "0001",
      "Committing state failed with NoMutationException, but the coordinator status does not exist",
      "",
      ""),
  CONSENSUS_COMMIT_CANNOT_GET_STATE(
      Category.UNKNOWN_TRANSACTION_STATUS_ERROR, "0002", "The state cannot be retrieved", "", ""),
  CONSENSUS_COMMIT_UNKNOWN_COORDINATOR_STATUS(
      Category.UNKNOWN_TRANSACTION_STATUS_ERROR,
      "0003",
      "The coordinator status is unknown",
      "",
      ""),
  CONSENSUS_COMMIT_ABORTING_STATE_FAILED_WITH_NO_MUTATION_EXCEPTION_BUT_COORDINATOR_STATUS_DOES_NOT_EXIST(
      Category.UNKNOWN_TRANSACTION_STATUS_ERROR,
      "0004",
      "Aborting state failed with NoMutationException, but the coordinator status does not exist",
      "",
      ""),
  ;

  private static final String COMPONENT_NAME = "DB-CORE";

  private final Category category;
  private final String id;
  private final String message;
  private final String cause;
  private final String solution;

  CoreError(Category category, String id, String message, String cause, String solution) {
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
