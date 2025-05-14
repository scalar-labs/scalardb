package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.RdbEngine.DB2;
import static com.scalar.db.storage.jdbc.RdbEngine.MYSQL;
import static com.scalar.db.storage.jdbc.RdbEngine.ORACLE;
import static com.scalar.db.storage.jdbc.RdbEngine.POSTGRESQL;
import static com.scalar.db.storage.jdbc.RdbEngine.SQL_SERVER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.scalar.db.io.DataType;
import java.sql.JDBCType;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class RdbEngineTest {
  private static final Map<RdbEngine, Map<Column, DataType>> DATA_TYPE_MAP = new HashMap<>();

  @BeforeAll
  public static void beforeAll() {
    prepareDataTypeMap();
  }

  @ParameterizedTest
  @EnumSource(
      value = RdbEngine.class,
      names = {"SQLITE"},
      mode = EnumSource.Mode.EXCLUDE)
  public void getDataTypeForScalarDbTest(RdbEngine rdbEngineType) {
    RdbEngineStrategy rdbEngine = RdbEngine.createRdbEngineStrategy(rdbEngineType);

    DATA_TYPE_MAP
        .get(rdbEngineType)
        .forEach(
            (given, expected) -> {
              String description =
                  String.format(
                      "database engine specific test failed: "
                          + "%s, JDBCType = %s, type name = %s, column size = %d, digits = %dp, overrideDataType = %s",
                      rdbEngineType,
                      given.type,
                      given.typeName,
                      given.columnSize,
                      given.digits,
                      given.overrideDataType);
              if (expected != null) {
                if (given.overrideDataType != null) {
                  DataType actualWithAllowedOverride =
                      rdbEngine.getDataTypeForScalarDb(
                          given.type,
                          given.typeName,
                          given.columnSize,
                          given.digits,
                          "",
                          given.overrideDataType);
                  assertThat(actualWithAllowedOverride).as(description).isEqualTo(expected);
                } else {
                  DataType actualWithoutOverride =
                      rdbEngine.getDataTypeForScalarDb(
                          given.type, given.typeName, given.columnSize, given.digits, "", null);
                  assertThat(actualWithoutOverride).as(description).isEqualTo(expected);

                  // Overriding with the default type mapping should works as well
                  DataType actualWithOverrideSameAsDefault =
                      rdbEngine.getDataTypeForScalarDb(
                          given.type, given.typeName, given.columnSize, given.digits, "", expected);
                  assertThat(actualWithOverrideSameAsDefault).as(description).isEqualTo(expected);
                }
              } else {
                Throwable thrown =
                    catchThrowable(
                        () ->
                            rdbEngine.getDataTypeForScalarDb(
                                given.type,
                                given.typeName,
                                given.columnSize,
                                given.digits,
                                "",
                                given.overrideDataType));
                assertThat(thrown).as(description).isInstanceOf(IllegalArgumentException.class);
              }
            });
  }

  private static void prepareDataTypeMap() {
    // init
    for (RdbEngine rdbEngine : RdbEngine.values()) {
      DATA_TYPE_MAP.put(rdbEngine, new HashMap<>());
    }

    // BOOLEAN
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.BIT, "BIT", 1, 0), DataType.BOOLEAN);
    DATA_TYPE_MAP.get(POSTGRESQL).put(new Column(JDBCType.BIT, "bool", 1, 0), DataType.BOOLEAN);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.BIT, "bit", 1, 0), DataType.BOOLEAN);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.BOOLEAN, "BOOLEAN"), DataType.BOOLEAN);

    // INT
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.TINYINT, "TINYINT"), DataType.INT);
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.SMALLINT, "SMALLINT"), DataType.INT);
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.INTEGER, "INT"), DataType.INT);
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.INTEGER, "INT UNSIGNED"), DataType.BIGINT);
    DATA_TYPE_MAP.get(POSTGRESQL).put(new Column(JDBCType.SMALLINT, "int2"), DataType.INT);
    DATA_TYPE_MAP.get(POSTGRESQL).put(new Column(JDBCType.SMALLINT, "smallserial"), null);
    DATA_TYPE_MAP.get(POSTGRESQL).put(new Column(JDBCType.INTEGER, "int4"), DataType.INT);
    DATA_TYPE_MAP.get(POSTGRESQL).put(new Column(JDBCType.INTEGER, "serial"), null);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.INTEGER, "INT"), null);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.SMALLINT, "smallint"), DataType.INT);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.INTEGER, "int"), DataType.INT);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.SMALLINT, "SMALLINT"), DataType.INT);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.INTEGER, "INT"), DataType.INT);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.BIGINT, "BIGINT"), DataType.BIGINT);

    // BIGINT
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.BIGINT, "BIGINT"), DataType.BIGINT);
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.BIGINT, "BIGINT UNSIGNED"), null);
    DATA_TYPE_MAP.get(POSTGRESQL).put(new Column(JDBCType.BIGINT, "bigint"), DataType.BIGINT);
    DATA_TYPE_MAP.get(POSTGRESQL).put(new Column(JDBCType.BIGINT, "bigserial"), null);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.BIGINT, "BIGINT"), null);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.BIGINT, "bigint"), DataType.BIGINT);

    // FLOAT
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.REAL, "FLOAT"), DataType.FLOAT);
    DATA_TYPE_MAP.get(POSTGRESQL).put(new Column(JDBCType.REAL, "float4"), DataType.FLOAT);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.REAL, "BINARY_FLOAT"), DataType.FLOAT);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.REAL, "real"), DataType.FLOAT);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.REAL, "REAL", 24, 0), DataType.FLOAT);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.REAL, "REAL", 10, 0), DataType.FLOAT);

    // DOUBLE
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.DOUBLE, "DOUBLE"), DataType.DOUBLE);
    DATA_TYPE_MAP.get(POSTGRESQL).put(new Column(JDBCType.DOUBLE, "float8"), DataType.DOUBLE);
    DATA_TYPE_MAP.get(POSTGRESQL).put(new Column(JDBCType.DOUBLE, "money"), null);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.DOUBLE, "BINARY_DOUBLE"), DataType.DOUBLE);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.FLOAT, "FLOAT", 53, 0), DataType.DOUBLE);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.FLOAT, "FLOAT", 54, 0), null);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.DOUBLE, "real"), DataType.DOUBLE);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.DOUBLE, "DOUBLE", 53, 0), DataType.DOUBLE);

    // TEXT
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.CHAR, "CHAR"), DataType.TEXT);
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.CHAR, "ENUM"), null);
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.CHAR, "SET"), null);
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.VARCHAR, "VARCHAR"), DataType.TEXT);
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.VARCHAR, "TINYTEXT"), DataType.TEXT);
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.LONGVARCHAR, "TEXT"), DataType.TEXT);
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.LONGVARCHAR, "JSON"), null);
    DATA_TYPE_MAP.get(POSTGRESQL).put(new Column(JDBCType.CHAR, "bpchar"), DataType.TEXT);
    DATA_TYPE_MAP.get(POSTGRESQL).put(new Column(JDBCType.VARCHAR, "varchar"), DataType.TEXT);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.CHAR, "CHAR"), DataType.TEXT);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.CLOB, "CLOB"), DataType.TEXT);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.VARCHAR, "VARCHAR2"), DataType.TEXT);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.NCHAR, "NCHAR"), DataType.TEXT);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.NCLOB, "NCLOB"), DataType.TEXT);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.NVARCHAR, "NVARCHAR"), DataType.TEXT);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.LONGVARCHAR, "LONG"), DataType.TEXT);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.CHAR, "char"), DataType.TEXT);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.CHAR, "uniqueidentifier"), null);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.VARCHAR, "varchar"), DataType.TEXT);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.NCHAR, "nchar"), DataType.TEXT);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.NVARCHAR, "nvarchar"), DataType.TEXT);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.LONGVARCHAR, "text"), DataType.TEXT);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.LONGNVARCHAR, "ntext"), DataType.TEXT);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.LONGNVARCHAR, "xml"), null);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.CHAR, "CHAR", 3, 0), DataType.TEXT);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.VARCHAR, "VARCHAR", 512, 0), DataType.TEXT);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.CLOB, "CLOB", 1048576, 0), DataType.TEXT);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.CHAR, "GRAPHIC", 3, 0), DataType.TEXT);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.VARCHAR, "VARGRAPHIC", 512, 0), DataType.TEXT);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.CLOB, "DCLOB", 512, 10), DataType.TEXT);

    // BLOB
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.BINARY, "BINARY"), DataType.BLOB);
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.BINARY, "GEOMETRY"), null);
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.LONGVARBINARY, "BLOB"), DataType.BLOB);
    DATA_TYPE_MAP.get(POSTGRESQL).put(new Column(JDBCType.BINARY, "bytea"), DataType.BLOB);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.BLOB, "BLOB"), DataType.BLOB);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.VARBINARY, "RAW"), DataType.BLOB);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.LONGVARBINARY, "LONG RAW"), DataType.BLOB);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.BINARY, "binary"), DataType.BLOB);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.BINARY, "timestamp"), null);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.VARBINARY, "varbinary"), DataType.BLOB);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.BINARY, "BINARY", 5, 0), DataType.BLOB);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.BINARY, "VARBINARY", 512, 0), DataType.BLOB);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.BLOB, "BLOB", 1024, 0), DataType.BLOB);
    DATA_TYPE_MAP
        .get(DB2)
        .put(new Column(JDBCType.BINARY, "CHAR () FOR BIT DATA", 5, 0), DataType.BLOB);
    DATA_TYPE_MAP
        .get(DB2)
        .put(new Column(JDBCType.BINARY, "VARCHAR () FOR BIT DATA", 512, 0), DataType.BLOB);

    // NUMERIC/DECIMAL
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.NUMERIC, "NUMERIC"), null);
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.DECIMAL, "DECIMAL"), null);
    DATA_TYPE_MAP.get(POSTGRESQL).put(new Column(JDBCType.NUMERIC, "numeric"), null);
    DATA_TYPE_MAP.get(POSTGRESQL).put(new Column(JDBCType.DECIMAL, "decimal"), null);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.NUMERIC, "NUMBER", 15, 0), DataType.BIGINT);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.NUMERIC, "NUMBER", 15, 2), DataType.DOUBLE);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.NUMERIC, "NUMBER", 16, 0), null);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.NUMERIC, "numeric"), null);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.DECIMAL, "decimal"), null);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.DECIMAL, "DECIMAL"), null);

    // DATE/TIME/TIMESTAMP
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.DATE, "DATE"), DataType.DATE);
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.DATE, "YEAR"), null);
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.TIME, "TIME"), DataType.TIME);
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.TIMESTAMP, "DATETIME"), DataType.TIMESTAMP);
    DATA_TYPE_MAP
        .get(MYSQL)
        .put(
            new Column(JDBCType.TIMESTAMP, "DATETIME", DataType.TIMESTAMPTZ), DataType.TIMESTAMPTZ);
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.TIMESTAMP, "TIMESTAMP"), DataType.TIMESTAMPTZ);
    DATA_TYPE_MAP
        .get(MYSQL)
        .put(new Column(JDBCType.TIMESTAMP, "TIMESTAMP", DataType.TIMESTAMP), null);
    DATA_TYPE_MAP.get(POSTGRESQL).put(new Column(JDBCType.DATE, "date"), DataType.DATE);
    DATA_TYPE_MAP.get(POSTGRESQL).put(new Column(JDBCType.TIME, "time"), DataType.TIME);
    DATA_TYPE_MAP.get(POSTGRESQL).put(new Column(JDBCType.TIME, "timetz"), null);
    DATA_TYPE_MAP
        .get(POSTGRESQL)
        .put(new Column(JDBCType.TIMESTAMP, "timestamp"), DataType.TIMESTAMP);
    DATA_TYPE_MAP
        .get(POSTGRESQL)
        .put(new Column(JDBCType.TIMESTAMP, "timestamptz"), DataType.TIMESTAMPTZ);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.TIMESTAMP, "DATE"), DataType.DATE);
    DATA_TYPE_MAP
        .get(ORACLE)
        .put(new Column(JDBCType.TIMESTAMP, "DATE", DataType.TIME), DataType.TIME);
    DATA_TYPE_MAP
        .get(ORACLE)
        .put(new Column(JDBCType.TIMESTAMP, "DATE", DataType.TIMESTAMP), DataType.TIMESTAMP);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.TIMESTAMP, "TIMESTAMP"), DataType.TIMESTAMP);
    DATA_TYPE_MAP
        .get(ORACLE)
        .put(new Column(JDBCType.TIMESTAMP, "TIMESTAMP", DataType.TIME), DataType.TIME);
    DATA_TYPE_MAP
        .get(ORACLE)
        .put(new Column(JDBCType.OTHER, "TIMESTAMP WITH TIME ZONE"), DataType.TIMESTAMPTZ);
    DATA_TYPE_MAP
        .get(ORACLE)
        .put(new Column(JDBCType.OTHER, "TIMESTAMP(3) WITH TIME ZONE"), DataType.TIMESTAMPTZ);
    DATA_TYPE_MAP
        .get(ORACLE)
        .put(new Column(JDBCType.OTHER, "TIMESTAMP WITH LOCAL TIME ZONE"), DataType.TIMESTAMPTZ);
    DATA_TYPE_MAP
        .get(ORACLE)
        .put(new Column(JDBCType.OTHER, "TIMESTAMP(1) WITH LOCAL TIME ZONE"), DataType.TIMESTAMPTZ);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.DATE, "date"), DataType.DATE);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.TIME, "time"), DataType.TIME);
    DATA_TYPE_MAP
        .get(SQL_SERVER)
        .put(new Column(JDBCType.TIMESTAMP, "datetime"), DataType.TIMESTAMP);
    DATA_TYPE_MAP
        .get(SQL_SERVER)
        .put(new Column(JDBCType.TIMESTAMP, "datetime2"), DataType.TIMESTAMP);
    DATA_TYPE_MAP
        .get(SQL_SERVER)
        .put(new Column(JDBCType.TIMESTAMP, "smalldatetime"), DataType.TIMESTAMP);
    DATA_TYPE_MAP
        .get(SQL_SERVER)
        .put(new Column(JDBCType.OTHER, "datetimeoffset"), DataType.TIMESTAMPTZ);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.DATE, "DATE"), DataType.DATE);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.TIME, "TIME"), DataType.TIME);
    DATA_TYPE_MAP
        .get(DB2)
        .put(new Column(JDBCType.TIMESTAMP, "TIMESTAMP", DataType.TIME), DataType.TIME);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.TIMESTAMP, "TIMESTAMP"), DataType.TIMESTAMP);
    DATA_TYPE_MAP
        .get(DB2)
        .put(
            new Column(JDBCType.TIMESTAMP, "TIMESTAMP", DataType.TIMESTAMPTZ),
            DataType.TIMESTAMPTZ);

    // Other unsupported data types
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.BIT, "BIT", 8, 0), null);
    DATA_TYPE_MAP.get(MYSQL).put(new Column(JDBCType.OTHER, ""), null);
    DATA_TYPE_MAP.get(POSTGRESQL).put(new Column(JDBCType.BIT, "bit", 8, 0), null);
    DATA_TYPE_MAP.get(POSTGRESQL).put(new Column(JDBCType.SQLXML, ""), null);
    DATA_TYPE_MAP.get(POSTGRESQL).put(new Column(JDBCType.OTHER, ""), null);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.OTHER, ""), null);
    DATA_TYPE_MAP.get(ORACLE).put(new Column(JDBCType.ROWID, "ROWID"), null);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.BIT, "BIT", 8, 0), null);
    DATA_TYPE_MAP.get(SQL_SERVER).put(new Column(JDBCType.OTHER, ""), null);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.OTHER, "DECFLOAT"), null);
    DATA_TYPE_MAP.get(DB2).put(new Column(JDBCType.OTHER, "XML"), null);
  }

  @Immutable
  private static class Column {
    final JDBCType type;
    final String typeName;
    final int columnSize;
    final int digits;
    @Nullable final DataType overrideDataType;

    Column(
        JDBCType type,
        String typeName,
        int columnSize,
        int digits,
        @Nullable DataType overrideDataType) {
      this.type = type;
      this.typeName = typeName;
      this.columnSize = columnSize;
      this.digits = digits;
      this.overrideDataType = overrideDataType;
    }

    Column(JDBCType type, String typeName, @Nullable DataType overrideDataType) {
      this(type, typeName, 0, 0, overrideDataType);
    }

    Column(JDBCType type, String typeName, int columnSize, int digits) {
      this(type, typeName, columnSize, digits, null);
    }

    Column(JDBCType type, String typeName) {
      this(type, typeName, 0, 0, null);
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof Column)) {
        return false;
      }
      Column other = (Column) o;
      return (type.equals(other.type)
          && typeName.equals(other.typeName)
          && columnSize == other.columnSize
          && digits == other.digits);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, typeName, columnSize, digits, overrideDataType);
    }

    @Override
    public String toString() {
      return String.format(
          "ColumnMetadata { JDBCType: %s typeName: %s columnSize: %d digits: %s }",
          type, typeName, columnSize, digits);
    }
  }
}
