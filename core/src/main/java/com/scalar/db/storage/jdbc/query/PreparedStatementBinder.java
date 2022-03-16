package com.scalar.db.storage.jdbc.query;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.ColumnVisitor;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import com.scalar.db.storage.jdbc.RdbEngine;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class PreparedStatementBinder implements ColumnVisitor {

  private final PreparedStatement preparedStatement;
  private final TableMetadata tableMetadata;
  private final RdbEngine rdbEngine;

  private int index = 1;
  private SQLException sqlException;

  public PreparedStatementBinder(
      PreparedStatement preparedStatement, TableMetadata tableMetadata, RdbEngine rdbEngine) {
    this.preparedStatement = preparedStatement;
    this.tableMetadata = tableMetadata;
    this.rdbEngine = rdbEngine;
  }

  public void throwSQLExceptionIfOccurred() throws SQLException {
    if (sqlException != null) {
      throw sqlException;
    }
  }

  @Override
  public void visit(BooleanColumn column) {
    try {
      if (column.hasNullValue()) {
        preparedStatement.setNull(index++, getSqlType(column.getName()));
      } else {
        preparedStatement.setBoolean(index++, column.getBooleanValue());
      }
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  @Override
  public void visit(IntColumn column) {
    try {
      if (column.hasNullValue()) {
        preparedStatement.setNull(index++, getSqlType(column.getName()));
      } else {
        preparedStatement.setInt(index++, column.getIntValue());
      }
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  @Override
  public void visit(BigIntColumn column) {
    try {
      if (column.hasNullValue()) {
        preparedStatement.setNull(index++, getSqlType(column.getName()));
      } else {
        preparedStatement.setLong(index++, column.getBigIntValue());
      }
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  @Override
  public void visit(FloatColumn column) {
    try {
      if (column.hasNullValue()) {
        preparedStatement.setNull(index++, getSqlType(column.getName()));
      } else {
        preparedStatement.setFloat(index++, column.getFloatValue());
      }
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  @Override
  public void visit(DoubleColumn column) {
    try {
      if (column.hasNullValue()) {
        preparedStatement.setNull(index++, getSqlType(column.getName()));
      } else {
        preparedStatement.setDouble(index++, column.getDoubleValue());
      }
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  @Override
  public void visit(TextColumn column) {
    try {
      if (column.hasNullValue()) {
        preparedStatement.setNull(index++, getSqlType(column.getName()));
      } else {
        preparedStatement.setString(index++, column.getTextValue());
      }
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  @Override
  public void visit(BlobColumn column) {
    try {
      if (column.hasNullValue()) {
        preparedStatement.setNull(index++, getSqlType(column.getName()));
      } else {
        preparedStatement.setBytes(index++, column.getBlobValueAsBytes());
      }
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  private int getSqlType(String name) {
    switch (tableMetadata.getColumnDataType(name)) {
      case BOOLEAN:
        if (rdbEngine == RdbEngine.ORACLE) {
          return Types.BIT;
        }
        return Types.BOOLEAN;
      case INT:
        return Types.INTEGER;
      case BIGINT:
        return Types.BIGINT;
      case FLOAT:
        return Types.FLOAT;
      case DOUBLE:
        return Types.DOUBLE;
      case TEXT:
        return Types.VARCHAR;
      case BLOB:
        if (rdbEngine == RdbEngine.POSTGRESQL) {
          return Types.VARBINARY;
        }
        return Types.BLOB;
      default:
        throw new AssertionError();
    }
  }
}
