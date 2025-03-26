package com.scalar.db.storage.objectstorage;

import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import java.util.Comparator;

public class ColumnComparator implements Comparator<Column<?>> {
  private final DataType dataType;

  public ColumnComparator(DataType dataType) {
    this.dataType = dataType;
  }

  @Override
  public int compare(Column<?> o1, Column<?> o2) {
    if (o1.getDataType() != dataType || o2.getDataType() != dataType) {
      throw new IllegalArgumentException("The columns are not of the specified data type.");
    }
    int cmp;
    switch (dataType) {
      case BOOLEAN:
        cmp = ((BooleanColumn) o1).compareTo((BooleanColumn) o2);
        break;
      case INT:
        cmp = ((IntColumn) o1).compareTo((IntColumn) o2);
        break;
      case BIGINT:
        cmp = ((BigIntColumn) o1).compareTo((BigIntColumn) o2);
        break;
      case FLOAT:
        cmp = ((FloatColumn) o1).compareTo((FloatColumn) o2);
        break;
      case DOUBLE:
        cmp = ((DoubleColumn) o1).compareTo((DoubleColumn) o2);
        break;
      case TEXT:
        cmp = ((TextColumn) o1).compareTo((TextColumn) o2);
        break;
      case BLOB:
        cmp = ((BlobColumn) o1).compareTo((BlobColumn) o2);
        break;
      case DATE:
        cmp = ((DateColumn) o1).compareTo((DateColumn) o2);
        break;
      case TIME:
        cmp = ((TimeColumn) o1).compareTo((TimeColumn) o2);
        break;
      case TIMESTAMP:
        cmp = ((TimestampColumn) o1).compareTo((TimestampColumn) o2);
        break;
      case TIMESTAMPTZ:
        cmp = ((TimestampTZColumn) o1).compareTo((TimestampTZColumn) o2);
        break;
      default:
        throw new AssertionError();
    }
    return cmp;
  }
}
