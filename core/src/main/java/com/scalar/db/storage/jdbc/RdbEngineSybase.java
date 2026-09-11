package com.scalar.db.storage.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.LikeExpression;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.CoreError;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.SelectWithTop;
import com.scalar.db.storage.jdbc.query.UpdateThenInsertQuery;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link RdbEngineStrategy} implementation for SAP ASE (Adaptive Server Enterprise, formerly
 * Sybase ASE). This is a proof of concept, verified against a live ASE 16.0 SP03 PL02 server over
 * jConnect 16.0; see docs/sap-ase-poc.md for what the integration suite does and does not cover.
 *
 * <p>It is aimed at running ScalarDB over an existing ASE, including in front of tables that are
 * already there, so it asks the server for as little as possible.
 *
 * <p>Namespace: a namespace is an ASE object owner, so a ScalarDB table is {@code owner.table}, the
 * same shape as a SQL Server schema. ASE differs in that an owner is a database user, and a
 * database user needs a server-level login, so <b>ScalarDB never creates or drops one</b>: creating
 * a namespace only checks that the user exists, and dropping a namespace does nothing. A database
 * administrator creates the users, and ScalarDB runs with no privilege beyond creating tables.
 *
 * <p>Nullability: ASE columns are NOT NULL unless the database sets {@code allow nulls by default},
 * the reverse of what ScalarDB expects, so every column definition here is explicit. Keys carry NOT
 * NULL through {@link #getDataTypeForKey}, and everything else carries NULL through {@link
 * #getNullableColumnClause}.
 *
 * <p>Character set: TEXT is stored in a {@code univarchar}, which holds Unicode regardless of the
 * character set the server was built with. A {@code varchar} holds the server's own set, so on an
 * iso_1 server it cannot store non-Latin text at all.
 *
 * <p>Truncation: ASE silently truncates a value that exceeds its column, so the connection turns on
 * {@code STRING_RTRUNCATION}, which makes that an error instead, and {@code BLOB} maps to the
 * unbounded {@code image} rather than to a sized {@code varbinary}.
 *
 * <p>Types: ASE {@code bit} cannot be null, so {@code BOOLEAN} is stored in a {@code tinyint}. ASE
 * has no time zone aware type, so {@code TIMESTAMPTZ} is stored in UTC in a {@code bigdatetime}.
 * Note that the ASE type named {@code timestamp} is a row version, not a point in time; it is
 * neither written nor importable here.
 *
 * <p>Locking: tables that ScalarDB creates use {@code LOCK DATAROWS}. The ASE default is allpages
 * locking, whose page level locks would make concurrent Consensus Commit transactions contend and
 * deadlock on rows they never touched. An imported table keeps whatever locking scheme it has.
 *
 * <p>Error codes: the numbers below come from the ASE documentation. They are the first thing to
 * confirm against a live server, since misreading a deadlock as an unknown failure would turn a
 * retriable conflict into a transaction failure.
 */
class RdbEngineSybase extends AbstractRdbEngine {

  private static final Logger logger = LoggerFactory.getLogger(RdbEngineSybase.class);

  /**
   * The size, in characters, of a variable length key column. ASE limits an index row to 600 bytes
   * on a server with the default 2 KB page size, and a ScalarDB key can span several columns. A
   * TEXT key is a {@code univarchar} at two bytes per character, so 128 characters is 256 bytes.
   * Making it configurable, as MySQL and Oracle do with {@code
   * scalar.db.jdbc.mysql.variable_key_column_size}, is left out of the proof of concept.
   */
  private static final int KEY_COLUMN_SIZE = 128;

  /**
   * The size, in characters, of a variable length non-key column. TEXT is stored in a {@code
   * univarchar}, which holds two bytes per character, so 900 characters is 1800 bytes and fits
   * inside the 1964 byte row limit of a server with the default 2 KB page size. A server built with
   * a larger page size can hold more.
   */
  private static final int VARIABLE_COLUMN_SIZE = 900;

  /** The lowest error number ASE reserves for user-defined messages. */
  private static final int USER_DEFINED_ERROR_NUMBER = 20000;

  /**
   * Raised by {@link #renameIndexSqls} when the index to rename is not there. It has to be distinct
   * from {@link #USER_DEFINED_ERROR_NUMBER} because {@link #isUndefinedIndexError} treats it as a
   * missing index and the caller falls back to another name on it.
   */
  private static final int USER_DEFINED_INDEX_NOT_FOUND_ERROR_NUMBER = 20001;

  // Error 2714: There is already an object named '%.*s' in the database.
  private static final int ERROR_OBJECT_ALREADY_EXISTS = 2714;
  // Error 2601: Attempt to insert duplicate key row in object '%.*s' with unique index '%.*s'.
  private static final int ERROR_DUPLICATE_KEY_IN_INDEX = 2601;
  // Error 2615: Attempt to insert duplicate row in table '%.*s' with unique clustered index.
  private static final int ERROR_DUPLICATE_ROW_IN_TABLE = 2615;
  // Error 2627: Attempt to insert duplicate key row violating a unique constraint.
  private static final int ERROR_UNIQUE_CONSTRAINT_VIOLATION = 2627;
  // Error 1205: Your server command was deadlocked with another process and has been chosen as
  // the deadlock victim. Rerun your command.
  private static final int ERROR_DEADLOCK = 1205;
  // Error 2727: Cannot find index '%.*s'.
  private static final int ERROR_INDEX_NOT_FOUND = 2727;
  // Error 3701: Cannot drop the index '%.*s', because it does not exist in the system catalogs.
  private static final int ERROR_CANNOT_DROP_MISSING_OBJECT = 3701;
  // Error 1913: An index named '%.*s' already exists on table '%.*s'.
  private static final int ERROR_INDEX_ALREADY_EXISTS = 1913;
  // Error 12205: Could not acquire a lock within the specified wait period. Aborting the
  // transaction. Raised only because LOCK_WAIT_SECONDS is set; ASE waits forever by default.
  private static final int ERROR_LOCK_WAIT_TIMEOUT = 12205;

  /**
   * How long a statement waits for a lock before ASE aborts it, from {@link
   * JdbcConfig#SYBASE_LOCK_WAIT_SECONDS}. The ASE default is to wait forever, which turns
   * contention into a hang that Consensus Commit cannot retry, because a blocked statement never
   * returns. A bounded wait turns it into error 12205, which {@link #isConflict(SQLException)}
   * reports as a conflict for the caller to retry. A negative value leaves ASE's own behavior
   * alone.
   */
  private final int lockWaitSeconds;

  /** The text form the driver renders for a bigdatetime, with the fraction optional. */
  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("yyyy-MM-dd HH:mm:ss")
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
          .toFormatter();

  private final RdbEngineTimeTypeSybase timeTypeEngine;

  RdbEngineSybase(JdbcConfig config) {
    this(config.getSybaseLockWaitSeconds());
  }

  @VisibleForTesting
  RdbEngineSybase() {
    this(JdbcConfig.DEFAULT_SYBASE_LOCK_WAIT_SECONDS);
  }

  private RdbEngineSybase(int lockWaitSeconds) {
    this.lockWaitSeconds = lockWaitSeconds;
    timeTypeEngine = new RdbEngineTimeTypeSybase();
  }

  @Override
  public boolean isDuplicateTableError(SQLException e) {
    return e.getErrorCode() == ERROR_OBJECT_ALREADY_EXISTS;
  }

  @Override
  public boolean isDuplicateKeyError(SQLException e) {
    return e.getErrorCode() == ERROR_DUPLICATE_KEY_IN_INDEX
        || e.getErrorCode() == ERROR_DUPLICATE_ROW_IN_TABLE
        || e.getErrorCode() == ERROR_UNIQUE_CONSTRAINT_VIOLATION;
  }

  @Override
  public boolean isConflict(SQLException e) {
    if (e.getErrorCode() == ERROR_DEADLOCK || e.getErrorCode() == ERROR_LOCK_WAIT_TIMEOUT) {
      return true;
    }
    // A duplicate key is a conflict here, not a permanent error. The upsert is an UPDATE followed
    // by an INSERT that only runs when the UPDATE matched nothing, so a row that is there and
    // committed is updated and never reaches the INSERT. A duplicate key from that INSERT therefore
    // means another transaction inserted the same key in between, which the caller should retry.
    // A PutIfNotExists that wants to see the duplicate is handled in ConditionalMutationQuery
    // before this is consulted.
    return isDuplicateKeyError(e);
  }

  @Override
  public boolean isUndefinedIndexError(SQLException e) {
    return e.getErrorCode() == ERROR_INDEX_NOT_FOUND
        || e.getErrorCode() == ERROR_CANNOT_DROP_MISSING_OBJECT
        || e.getErrorCode() == USER_DEFINED_INDEX_NOT_FOUND_ERROR_NUMBER;
  }

  @Override
  public boolean isDuplicateIndexError(SQLException e) {
    return e.getErrorCode() == ERROR_INDEX_ALREADY_EXISTS
        || e.getErrorCode() == ERROR_OBJECT_ALREADY_EXISTS;
  }

  @Override
  public boolean isDuplicateSchemaError(SQLException e) {
    // An owner is never created, so this error cannot happen
    return false;
  }

  @Override
  public String getDataTypeForEngine(DataType scalarDbDataType) {
    switch (scalarDbDataType) {
      case BOOLEAN:
        // Not "bit": an ASE bit column cannot hold null, and every non-key ScalarDB column can
        return "TINYINT";
      case INT:
        return "INT";
      case BIGINT:
        return "BIGINT";
      case FLOAT:
        return "REAL";
      case DOUBLE:
        return "DOUBLE PRECISION";
      case TEXT:
        // Not varchar: a varchar holds the server's own character set, which on an iso_1 server
        // cannot represent non-Latin text at all ("Error converting characters into server's
        // character set"). A univarchar holds Unicode whatever the server was built with.
        return "UNIVARCHAR(" + VARIABLE_COLUMN_SIZE + ")";
      case BLOB:
        // Not VARBINARY: ASE silently truncates a value that exceeds the column, with no error and
        // no warning, which corrupts anything longer than the limit. Consensus Commit's coordinator
        // stores its serialized write set in a BLOB, so a transaction with several writes produced
        // a truncated protobuf that failed to parse on read. An image column holds up to 2 GB and
        // is stored off-row, which also keeps the row within ASE's row size limit.
        return "IMAGE";
      case DATE:
        return "DATE";
      case TIME:
        // Not "time", whose resolution is a millisecond, while ScalarDB TIME is a microsecond
        return "BIGTIME";
      case TIMESTAMP:
      case TIMESTAMPTZ:
        // Not "datetime", whose resolution is 1/300 of a second
        return "BIGDATETIME";
      default:
        throw new AssertionError();
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Never returns null, so that a key column never falls back to the type for a regular column,
   * which would leave it nullable and make ASE reject the primary key.
   */
  @Override
  public String getDataTypeForKey(DataType dataType) {
    return getKeyColumnType(dataType) + " NOT NULL";
  }

  /**
   * {@inheritDoc}
   *
   * <p>Unlike a key column, an indexed column stays nullable; the caller appends {@link
   * #getNullableColumnClause()}. Returns null for the types that need no change to be indexed.
   */
  @Override
  @Nullable
  public String getDataTypeForSecondaryIndex(DataType dataType) {
    switch (dataType) {
      case TEXT:
        return getKeyColumnType(dataType);
      case BLOB:
        // A BLOB is an image column, which ASE cannot index, and it cannot be modified back to
        // image afterwards either: "You cannot modify column to TEXT/IMAGE/UNITEXT type". Oracle
        // and Db2 refuse a secondary index on a BLOB for their own reasons.
        throw new UnsupportedOperationException(
            CoreError.JDBC_SYBASE_INDEX_ON_BLOB_COLUMN_NOT_SUPPORTED.buildMessage());
      case BOOLEAN:
        // An imported bit column cannot be indexed: "Can't create index on a column of BIT data
        // type". Widening it to tinyint, which is how this engine stores BOOLEAN anyway, makes it
        // indexable.
        return "TINYINT";
      default:
        return null;
    }
  }

  private String getKeyColumnType(DataType dataType) {
    switch (dataType) {
      case TEXT:
        return "UNIVARCHAR(" + KEY_COLUMN_SIZE + ")";
      case BLOB:
        return "VARBINARY(" + KEY_COLUMN_SIZE + ")";
      default:
        return getDataTypeForEngine(dataType);
    }
  }

  @Override
  public String getNullableColumnClause() {
    // ASE columns are NOT NULL unless the database sets "allow nulls by default", which this engine
    // does not require, so a nullable column has to say so
    return " NULL";
  }

  @Override
  DataType getDataTypeForScalarDbInternal(
      JDBCType type,
      String typeName,
      int columnSize,
      int digits,
      String columnDescription,
      @Nullable DataType overrideDataType) {
    switch (type) {
      case BIT:
        return DataType.BOOLEAN;
      case TINYINT:
      case SMALLINT:
        logger.info(
            "Data type larger than that of underlying database is assigned: {} ({} to INT)",
            columnDescription,
            typeName);
        return DataType.INT;
      case INTEGER:
        if (isUnsigned(typeName)) {
          logger.info(
              "Data type larger than that of underlying database is assigned: {} ({} to BIGINT)",
              columnDescription,
              typeName);
          return DataType.BIGINT;
        }
        return DataType.INT;
      case BIGINT:
        if (isUnsigned(typeName)) {
          // An unsigned bigint does not fit in the signed ScalarDB BIGINT
          throw new IllegalArgumentException(
              CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                  typeName, columnDescription));
        }
        return DataType.BIGINT;
      case NUMERIC:
      case DECIMAL:
        // ScalarDB has no decimal type. Without a scale the value is an integer, so it maps exactly
        // as long as it fits: BIGINT holds 18 digits and INT holds 9. jConnect reports the
        // precision as the column size and the scale as the digits.
        if (digits == 0) {
          if (columnSize <= 9) {
            return DataType.INT;
          }
          if (columnSize <= 18) {
            logger.info(
                "Data type larger than that of underlying database is assigned: {} ({} to BIGINT)",
                columnDescription,
                typeName);
            return DataType.BIGINT;
          }
          throw new IllegalArgumentException(
              CoreError.JDBC_SYBASE_DECIMAL_PRECISION_NOT_SUPPORTED.buildMessage(
                  columnDescription, typeName, columnSize, digits));
        }
        // With a scale, only DOUBLE is close, and it is not exact past 15 significant digits, so
        // the caller has to ask for it rather than have the loss chosen for them. ASE rounds a
        // double back to the column's scale on write.
        if (overrideDataType == DataType.DOUBLE) {
          return DataType.DOUBLE;
        }
        throw new IllegalArgumentException(
            CoreError.JDBC_SYBASE_DECIMAL_SCALE_NOT_SUPPORTED.buildMessage(
                columnDescription, typeName, columnSize, digits));
      case REAL:
        return DataType.FLOAT;
      case FLOAT:
      case DOUBLE:
        return DataType.DOUBLE;
      case CHAR:
      case NCHAR:
      case VARCHAR:
      case NVARCHAR:
      case LONGVARCHAR:
      case LONGNVARCHAR:
        logger.info(
            "Data type larger than that of underlying database is assigned: {} ({} to TEXT)",
            columnDescription,
            typeName);
        return DataType.TEXT;
      case BINARY:
      case VARBINARY:
      case LONGVARBINARY:
        if (typeName.equalsIgnoreCase("timestamp")) {
          // The ASE type named timestamp is a row version that the server maintains, not a point in
          // time, and it cannot be written by a client
          throw new IllegalArgumentException(
              CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                  typeName, columnDescription));
        }
        logger.info(
            "Data type larger than that of underlying database is assigned: {} ({} to BLOB)",
            columnDescription,
            typeName);
        return DataType.BLOB;
      case DATE:
        return DataType.DATE;
      case TIME:
        return DataType.TIME;
      case TIMESTAMP:
        // datetime and smalldatetime
        return timestampType(overrideDataType);
      case OTHER:
        // jConnect reports bigdatetime as 11 and bigtime as 10, neither of which java.sql.Types
        // defines, so both arrive here rather than as TIMESTAMP and TIME
        if (typeName.equalsIgnoreCase("bigdatetime")) {
          return timestampType(overrideDataType);
        }
        if (typeName.equalsIgnoreCase("bigtime")) {
          return DataType.TIME;
        }
        throw new IllegalArgumentException(
            CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                typeName, columnDescription));
      default:
        // Includes decimal, numeric, money and smallmoney, which have no ScalarDB counterpart
        throw new IllegalArgumentException(
            CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                typeName, columnDescription));
    }
  }

  private static DataType timestampType(@Nullable DataType overrideDataType) {
    if (overrideDataType == DataType.TIMESTAMPTZ) {
      // ASE has no time zone aware type. A column that already holds UTC can be imported as
      // TIMESTAMPTZ, which is how this engine writes one.
      return DataType.TIMESTAMPTZ;
    }
    return DataType.TIMESTAMP;
  }

  private static boolean isUnsigned(String typeName) {
    return typeName.toLowerCase().startsWith("unsigned");
  }

  @Override
  public int getSqlTypes(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return Types.TINYINT;
      case INT:
        return Types.INTEGER;
      case BIGINT:
        return Types.BIGINT;
      case FLOAT:
        return Types.REAL;
      case DOUBLE:
        return Types.DOUBLE;
      case TEXT:
        return Types.VARCHAR;
      case BLOB:
        return Types.LONGVARBINARY;
      case DATE:
        return Types.DATE;
      case TIME:
        return Types.TIME;
      case TIMESTAMP:
      case TIMESTAMPTZ:
        return Types.TIMESTAMP;
      default:
        throw new AssertionError();
    }
  }

  @Override
  public String getTextType(int charLength, boolean isKey) {
    // Used for the columns of the ScalarDB metadata tables. A key column has to say NOT NULL; the
    // caller appends the nullable clause to the others where they are nullable.
    return String.format("UNIVARCHAR(%s)", charLength) + (isKey ? " NOT NULL" : "");
  }

  @Override
  public String computeBooleanValue(boolean value) {
    return value ? "1" : "0";
  }

  /**
   * {@inheritDoc}
   *
   * <p>ScalarDB does not create the owner: an ASE owner is a database user, and a database user
   * needs a server-level login, which is not something an application should be able to create. The
   * returned statement only checks that a database administrator has created it, so that a missing
   * one is reported here rather than as a confusing failure at the first {@code CREATE TABLE}.
   */
  @Override
  public String[] createSchemaSqls(String fullSchema) {
    return new String[] {
      "IF NOT EXISTS (SELECT 1 FROM sysusers WHERE name = "
          + quoteStringLiteral(fullSchema)
          + ") RAISERROR "
          + USER_DEFINED_ERROR_NUMBER
          + " "
          + quoteStringLiteral(
              CoreError.JDBC_SYBASE_NAMESPACE_USER_NOT_FOUND.buildMessage(fullSchema))
    };
  }

  @Override
  public String[] createSchemaIfNotExistsSqls(String fullSchema) {
    // The statement is a check rather than a creation, so it is already idempotent
    return createSchemaSqls(fullSchema);
  }

  private static String quoteStringLiteral(String value) {
    // Single quotes, not double: quoted identifiers are on, so a double quoted string would be read
    // as an identifier
    return "'" + value.replace("'", "''") + "'";
  }

  @Override
  public String createTableInternalPrimaryKeyClause(
      boolean hasDescClusteringOrder, TableMetadata metadata) {
    String keys;
    if (hasDescClusteringOrder) {
      keys =
          Stream.concat(
                  metadata.getPartitionKeyNames().stream().map(c -> enclose(c) + " ASC"),
                  metadata.getClusteringKeyNames().stream()
                      .map(c -> enclose(c) + " " + metadata.getClusteringOrder(c)))
              .collect(Collectors.joining(","));
    } else {
      keys =
          Stream.concat(
                  metadata.getPartitionKeyNames().stream(),
                  metadata.getClusteringKeyNames().stream())
              .map(this::enclose)
              .collect(Collectors.joining(","));
    }
    // LOCK DATAROWS replaces the allpages locking that ASE uses by default. Page level locks would
    // block transactions that touch unrelated rows sharing a page, which Consensus Commit does
    // constantly.
    return "PRIMARY KEY (" + keys + ")) LOCK DATAROWS";
  }

  @Override
  public String[] createTableInternalSqlsAfterCreateTable(
      boolean hasDifferentClusteringOrders,
      String schema,
      String table,
      TableMetadata metadata,
      boolean ifNotExists) {
    // do nothing
    return new String[0];
  }

  @Override
  public String tryAddIfNotExistsToCreateTableSql(String createTableSql) {
    // ASE has no IF NOT EXISTS clause, so the duplicate object error is handled instead
    return createTableSql;
  }

  @Override
  public String deleteMetadataSchemaSql(String metadataSchema) {
    // Do nothing. ScalarDB does not drop the ASE user that owns its metadata tables.
    return null;
  }

  @Override
  public String dropNamespaceSql(String namespace) {
    // Do nothing. ScalarDB does not drop an ASE user.
    return null;
  }

  @Override
  public String[] dropColumnSql(String namespace, String table, String columnName) {
    // ASE spells this without the COLUMN keyword
    return new String[] {
      "ALTER TABLE " + encloseFullTableName(namespace, table) + " DROP " + enclose(columnName)
    };
  }

  @Override
  public String renameColumnSql(
      String namespace,
      String table,
      String oldColumnName,
      String newColumnName,
      String columnType) {
    return renameAsOwner(namespace, table + "." + oldColumnName, newColumnName, "column");
  }

  @Override
  public String renameTableSql(String namespace, String oldTableName, String newTableName) {
    return renameAsOwner(namespace, oldTableName, newTableName, null);
  }

  /**
   * Builds a rename batch for {@code sp_rename}, which has two properties that shape this. It
   * resolves the object under the <em>current user</em> and rejects an owner-qualified name
   * ("'n1.t1.v1' is invalid", "'n1' is not a valid object name"), so the rename runs as the
   * namespace's owner with {@code setuser}. It also rejects quoted identifiers ("You do not own a
   * table, column, index or partition of that name"), so the names are passed bare: a name that
   * would need quoting cannot be renamed on ASE.
   *
   * <p>Both of those failures come back as messages with a non-zero return status rather than as
   * errors, so a wrong statement here would silently do nothing.
   */
  private String renameAsOwner(
      String namespace, String objectName, String newName, @Nullable String objectType) {
    StringBuilder sql =
        new StringBuilder("setuser ")
            .append(quoteStringLiteral(namespace))
            .append("\nexec sp_rename ")
            .append(quoteStringLiteral(objectName))
            .append(", ")
            .append(quoteStringLiteral(newName));
    if (objectType != null) {
      sql.append(", ").append(quoteStringLiteral(objectType));
    }
    return sql.append("\nsetuser").toString();
  }

  @Override
  public String[] alterColumnTypeSql(
      String namespace, String table, String columnName, String columnType) {
    // ASE spells this MODIFY rather than ALTER COLUMN
    return new String[] {
      "ALTER TABLE "
          + encloseFullTableName(namespace, table)
          + " MODIFY "
          + enclose(columnName)
          + " "
          + columnType
    };
  }

  @Override
  public String internalTableExistsCheckSql() {
    return "SELECT 1 FROM sysobjects o JOIN sysusers u ON o.uid = u.uid"
        + " WHERE u.name = ? AND o.name = ? AND o.type = 'U'";
  }

  @Override
  public String getTableNamesInNamespaceSql() {
    return "SELECT o.name FROM sysobjects o JOIN sysusers u ON o.uid = u.uid"
        + " WHERE u.name = ? AND o.type = 'U'";
  }

  /**
   * {@inheritDoc}
   *
   * <p>The index name is not enclosed. ASE takes the quotes literally in an index name: {@code
   * create index "i" ...} makes an index whose name is {@code "i"}, quotes included, which nothing
   * can then drop or rename. ScalarDB generates index names that need no quoting.
   */
  @Override
  public String createIndexSql(
      String schema, String table, String indexName, String indexedColumn) {
    return "CREATE INDEX "
        + indexName
        + " ON "
        + encloseFullTableName(schema, table)
        + " ("
        + enclose(indexedColumn)
        + ")";
  }

  /**
   * {@inheritDoc}
   *
   * <p>ASE names the table and the index together rather than with an ON clause, and it reads a
   * three-part name as {@code database.owner.object}: qualifying the table with its owner gets
   * "DROP INDEX does not allow specifying the database name as a prefix to the object name". So the
   * drop runs as the owner, exactly as a rename does, and the index name is not enclosed for the
   * reason given on {@link #createIndexSql}.
   */
  @Override
  public String dropIndexSql(String schema, String table, String indexName) {
    return "setuser "
        + quoteStringLiteral(schema)
        + "\nDROP INDEX "
        + enclose(table)
        + "."
        + indexName
        + "\nsetuser";
  }

  @Override
  public boolean requiresExplicitDropIndexBeforeDropColumn() {
    // ASE refuses to drop a column that an index still references
    return true;
  }

  @Override
  public String[] renameIndexSqls(
      String schema, String table, String column, String oldIndexName, String newIndexName) {
    // sp_rename reports a name it cannot resolve as a message with a non-zero return status rather
    // than as an error, so on its own it would rename nothing and still look like it worked. The
    // caller renames an index by its shortened name and falls back to the original long name when
    // the first attempt reports the index missing, and that fallback only runs if this throws. So
    // the missing index is turned into a real error ahead of the rename.
    return new String[] {
      "IF NOT EXISTS (SELECT 1 FROM sysindexes WHERE id = object_id("
          + quoteStringLiteral(schema + "." + table)
          + ") AND name = "
          + quoteStringLiteral(oldIndexName)
          + ") RAISERROR "
          + USER_DEFINED_INDEX_NOT_FOUND_ERROR_NUMBER
          + " "
          + quoteStringLiteral("The index " + oldIndexName + " does not exist"),
      renameAsOwner(schema, table + "." + oldIndexName, newIndexName, "index")
    };
  }

  @Override
  public String tryAddIfNotExistsToCreateIndexSql(String createIndexSql) {
    // ASE has no IF NOT EXISTS clause, so the duplicate index error is handled instead
    return createIndexSql;
  }

  @Override
  public String enclose(String name) {
    // Requires quoted identifiers, which getConnectionInitSql() turns on for every connection
    return "\"" + name + "\"";
  }

  @Override
  public SelectQuery buildSelectWithLimitQuery(SelectQuery.Builder builder, int limit) {
    return new SelectWithTop(builder, limit);
  }

  @Override
  public UpsertQuery buildUpsertQuery(UpsertQuery.Builder builder) {
    // Not MERGE, although ASE 16 has it and it works on a table ScalarDB created. ASE compiles the
    // WHEN NOT MATCHED THEN INSERT branch even when the row is there, and rejects the whole
    // statement with error 233 if that insert would leave a NOT NULL column without a value. An
    // upsert only carries the columns being written, so on a table that has NOT NULL columns of its
    // own -- which is to say most tables ScalarDB did not create -- the MERGE fails on a row it
    // would only ever have updated. See UpdateThenInsertQuery.
    return new UpdateThenInsertQuery(builder);
  }

  @Override
  public String getDriverClassName() {
    // Returned as a name rather than as a class literal on purpose: jConnect ships with SAP ASE
    // under the SAP license and is not on Maven Central, so it cannot be a build dependency. Put
    // jconn4.jar on the classpath. jTDS is not used, since it predates the bigdatetime and bigtime
    // types this engine writes.
    return "com.sybase.jdbc4.jdbc.SybDriver";
  }

  @Override
  public String getPattern(LikeExpression likeExpression) {
    String escape = likeExpression.getEscape();
    String pattern = likeExpression.getTextValue();
    if (escape.isEmpty()) {
      // As in Transact-SQL generally, "[" and "]" are pattern characters and have to be escaped
      // even when the user asked for no escaping, so an implicit escape character is added. It is
      // "\", which then has to be escaped as well to keep the user's original intention.
      return pattern.replaceAll("[\\[\\]\\\\]", "\\\\$0");
    }
    return pattern.replaceAll(
        "[\\[\\]]", String.format("%s$0", escape.equals("\\") ? "\\\\" : escape));
  }

  @Override
  public String getEscape(LikeExpression likeExpression) {
    String escape = likeExpression.getEscape();
    return escape.isEmpty() ? "\\" : escape;
  }

  @Override
  public DateColumn parseDateColumn(ResultSet resultSet, String columnName) throws SQLException {
    // Read as text rather than through java.sql.Date, which applies the Julian to Gregorian
    // transition and shifts a date before October 15, 1582 by ten days: 1582-10-05 comes back from
    // getDate() as 1582-10-15, while getString() returns the stored value. The driver renders a
    // date as yyyy-MM-dd. The same does not apply to bigdatetime and bigtime, which round trip
    // correctly through getTimestamp().
    String date = resultSet.getString(columnName);
    return DateColumn.of(columnName, date == null ? null : LocalDate.parse(date.trim()));
  }

  @Override
  public TimeColumn parseTimeColumn(ResultSet resultSet, String columnName) throws SQLException {
    // Read as text, as with every other temporal type here; java.sql.Time would also lose the
    // microseconds that a bigtime column holds
    String time = resultSet.getString(columnName);
    return TimeColumn.ofStrict(
        columnName,
        time == null ? null : LocalTime.parse(time.trim()).truncatedTo(ChronoUnit.MICROS));
  }

  @Override
  public TimestampColumn parseTimestampColumn(ResultSet resultSet, String columnName)
      throws SQLException {
    // Truncated, not rejected: ScalarDB TIMESTAMP holds milliseconds while an ASE bigdatetime holds
    // microseconds. A table ScalarDB created only ever holds milliseconds, but an imported column
    // can hold more, and the alternative is for the read to fail.
    LocalDateTime timestamp = parseTimestampText(resultSet.getString(columnName));
    return TimestampColumn.ofStrict(
        columnName, timestamp == null ? null : timestamp.truncatedTo(ChronoUnit.MILLIS));
  }

  /**
   * Parses the text form the driver renders for a bigdatetime, {@code yyyy-MM-dd HH:mm:ss.SSSSSS}.
   * Text is used rather than {@code java.sql.Timestamp} because that shifts a value inside the
   * Julian to Gregorian transition: 1582-10-05 comes back as 1582-10-15.
   */
  @Nullable
  private static LocalDateTime parseTimestampText(@Nullable String timestamp) {
    return timestamp == null ? null : LocalDateTime.parse(timestamp.trim(), TIMESTAMP_FORMATTER);
  }

  @Override
  public TimestampTZColumn parseTimestampTZColumn(ResultSet resultSet, String columnName)
      throws SQLException {
    LocalDateTime timestamp = parseTimestampText(resultSet.getString(columnName));
    if (timestamp == null) {
      return TimestampTZColumn.ofNull(columnName);
    }
    // The column holds UTC, as written by RdbEngineTimeTypeSybase. Truncated for the same reason
    // as in parseTimestampColumn.
    return TimestampTZColumn.ofStrict(
        columnName, timestamp.truncatedTo(ChronoUnit.MILLIS).toInstant(ZoneOffset.UTC));
  }

  @Override
  public Map<String, String> getConnectionProperties(JdbcConfig config) {
    // An upsert is two statements in one prepared statement, which ASE can only run as a language
    // batch. Dynamic prepare would send it as a single prepared statement, which ASE rejects. This
    // is the jConnect default, but it is set explicitly because the upsert silently depends on it.
    return ImmutableMap.of("DYNAMIC_PREPARE", "false");
  }

  @Override
  public String getConnectionInitSql() {
    // QUOTED_IDENTIFIER is needed for the double quoted identifiers that enclose() emits. LOCK WAIT
    // bounds how long a statement blocks on a lock; see lockWaitSeconds for why waiting forever,
    // which is the ASE default, is worse than failing.
    // STRING_RTRUNCATION turns silent truncation into error 9502. Without it ASE quietly shortens
    // any value that exceeds its column, losing data with no error and no warning.
    String sql = "SET QUOTED_IDENTIFIER ON\nSET STRING_RTRUNCATION ON";
    if (lockWaitSeconds >= 0) {
      sql += "\nSET LOCK WAIT " + lockWaitSeconds;
    }
    return sql;
  }

  @Override
  public RdbEngineTimeTypeStrategy<String, String, String, String> getTimeTypeStrategy() {
    return timeTypeEngine;
  }

  @Override
  public void setConnectionToReadOnly(Connection connection, boolean readOnly) {
    // Do nothing. ASE has no read-only session mode, and jConnect rejects the request.
  }

  @Override
  public void throwIfAlterColumnTypeNotSupported(DataType from, DataType to) {
    if (from == DataType.BLOB && to == DataType.TEXT) {
      // A BLOB is an image column, and ASE refuses "Explicit conversion from datatype 'IMAGE' to
      // 'VARCHAR'"
      throw new UnsupportedOperationException(
          CoreError.JDBC_SYBASE_UNSUPPORTED_COLUMN_TYPE_CONVERSION.buildMessage(
              from.toString(), to.toString()));
    }
  }

  @Override
  public int getMinimumIsolationLevelForConsistentVirtualTableRead() {
    // A virtual table read joins the data table to the transaction metadata table, and Consensus
    // Commit needs the pair to come from the same instant. Level 1 cannot give that: ASE releases
    // the shared lock on a row "when the row qualification completes", so a writer can commit
    // between the two halves of the join and the reader sees data from one version with metadata
    // from another.
    //
    // Level 2 is enough, on ASE's own terms. "Performance and Tuning Series: Locking and
    // Concurrency Control" for 16.0 says level 2 "holds shared locks until the transaction
    // completes", so once the reader has the data row a writer cannot change it, and a writer
    // always updates the data table before the metadata table in one transaction (see
    // JdbcDatabase.dividePutForSourceTables), so it cannot reach the metadata either. Verified
    // against a server: at level 1 a second connection updates a row the reader is holding, at
    // level 2 it blocks.
    //
    // An existing table is usually allpages locked, which does not support level 2, but that is
    // safe rather than a hole: "If transaction level 2 is set in a session, and an allpages-locked
    // table is included in a query, isolation level 3 is also applied on the allpages-locked
    // tables." So such a table is read at level 3 whatever this returns.
    return Connection.TRANSACTION_REPEATABLE_READ;
  }
}
