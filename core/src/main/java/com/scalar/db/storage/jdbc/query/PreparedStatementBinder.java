package com.scalar.db.storage.jdbc.query;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.ValueVisitor;
import com.scalar.db.storage.jdbc.RdbEngine;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class PreparedStatementBinder implements ValueVisitor {

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
  public void visit(BooleanValue value) {
    try {
      preparedStatement.setBoolean(index++, value.get());
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  @Override
  public void visit(IntValue value) {
    try {
      preparedStatement.setInt(index++, value.get());
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  @Override
  public void visit(BigIntValue value) {
    try {
      preparedStatement.setLong(index++, value.get());
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  @Override
  public void visit(FloatValue value) {
    try {
      preparedStatement.setFloat(index++, value.get());
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  @Override
  public void visit(DoubleValue value) {
    try {
      preparedStatement.setDouble(index++, value.get());
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  @Override
  public void visit(TextValue value) {
    try {
      preparedStatement.setString(index++, value.get().orElse(null));
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  @Override
  public void visit(BlobValue value) {
    try {
      preparedStatement.setBytes(index++, value.get().orElse(null));
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  public void bindNullValue(String name) {
    try {
      preparedStatement.setNull(index++, getSqlType(name));
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
