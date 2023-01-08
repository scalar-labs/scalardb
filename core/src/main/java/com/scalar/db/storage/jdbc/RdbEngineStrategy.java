package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcAdmin.METADATA_SCHEMA;
import static com.scalar.db.storage.jdbc.JdbcAdmin.METADATA_TABLE;
import static com.scalar.db.storage.jdbc.query.QueryUtils.enclosedFullTableName;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.query.QueryUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class RdbEngineStrategy {

  static RdbEngineStrategy create(JdbcConfig config) {
    switch (config.getRdbEngine()) {
      case MYSQL:
        return new RdbEngineMysql();
      case POSTGRESQL:
        return new RdbEnginePostgresql();
      case ORACLE:
        return new RdbEngineOracle();
      case SQL_SERVER:
        return new RdbEngineSqlServer();
      default:
        assert false;
        return null;
    }
  }

  abstract protected RdbEngine getRdbEngine();

  protected abstract String getDataTypeForEngine(DataType dataType);

  protected abstract void createNamespaceExecute(Connection connection, String fullNamespace)
      throws SQLException;

  protected abstract String createTableInternalPrimaryKeyClause(
      boolean hasDescClusteringOrder, TableMetadata metadata);

  protected abstract void createTableInternalExecuteAfterCreateTable(
      boolean hasDescClusteringOrder,
      Connection connection,
      String schema,
      String table,
      TableMetadata metadata)
      throws SQLException;

  protected String enclose(String name) {
    return QueryUtils.enclose(name, getRdbEngine());
  }
}
