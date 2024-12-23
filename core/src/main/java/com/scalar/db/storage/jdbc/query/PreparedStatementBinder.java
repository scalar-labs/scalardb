package com.scalar.db.storage.jdbc.query;

import com.scalar.db.api.LikeExpression;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.ColumnVisitor;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class PreparedStatementBinder implements ColumnVisitor {

  private final PreparedStatement preparedStatement;
  private final TableMetadata tableMetadata;
  private final RdbEngineStrategy rdbEngine;

  private int index = 1;
  private SQLException sqlException;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public PreparedStatementBinder(
      PreparedStatement preparedStatement,
      TableMetadata tableMetadata,
      RdbEngineStrategy rdbEngine) {
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

  @Override
  public void visit(DateColumn column) {
    try {
      if (column.hasNullValue()) {
        preparedStatement.setNull(index++, getSqlType(column.getName()));
      } else {
        preparedStatement.setObject(index++, rdbEngine.encodeDate(column));
      }
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  @Override
  public void visit(TimeColumn column) {
    try {
      if (column.hasNullValue()) {
        preparedStatement.setNull(index++, getSqlType(column.getName()));
      } else {
        preparedStatement.setObject(index++, rdbEngine.encodeTime(column));
      }
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  @Override
  public void visit(TimestampColumn column) {
    try {
      if (column.hasNullValue()) {
        preparedStatement.setNull(index++, getSqlType(column.getName()));
      } else {
        preparedStatement.setObject(index++, rdbEngine.encodeTimestamp(column));
      }
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  @Override
  public void visit(TimestampTZColumn column) {
    try {
      if (column.hasNullValue()) {
        preparedStatement.setNull(index++, getSqlType(column.getName()));
      } else {
        preparedStatement.setObject(index++, rdbEngine.encodeTimestampTZ(column));
      }
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  public void bindLikeClause(LikeExpression likeExpression) {
    try {
      String pattern = rdbEngine.getPattern(likeExpression);
      String escape = rdbEngine.getEscape(likeExpression);
      preparedStatement.setString(index++, pattern);
      if (escape != null) {
        preparedStatement.setString(index++, escape);
      }
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  private int getSqlType(String name) {
    DataType dataType = tableMetadata.getColumnDataType(name);
    return rdbEngine.getSqlTypes(dataType);
  }
}
