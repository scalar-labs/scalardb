package com.scalar.db.storage.cosmos;

import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.ColumnVisitor;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A visitor class to make a concatenated key string for the partition key
 *
 * @author Yuji Ito
 */
@NotThreadSafe
public class ConcatenationVisitor implements ColumnVisitor {
  private final List<String> columns;

  public ConcatenationVisitor() {
    columns = new ArrayList<>();
  }

  public String build() {
    // TODO What if the string or blob column includes `:`?
    return String.join(":", columns);
  }

  /**
   * Sets the specified {@code BooleanColumn} to the key string
   *
   * @param column a {@code BooleanColumn} to be set
   */
  @Override
  public void visit(BooleanColumn column) {
    columns.add(String.valueOf(column.getBooleanValue()));
  }

  /**
   * Sets the specified {@code IntColumn} to the key string
   *
   * @param column a {@code IntColumn} to be set
   */
  @Override
  public void visit(IntColumn column) {
    columns.add(String.valueOf(column.getIntValue()));
  }

  /**
   * Sets the specified {@code BigIntColumn} to the key string
   *
   * @param column a {@code BigIntColumn} to be set
   */
  @Override
  public void visit(BigIntColumn column) {
    columns.add(String.valueOf(column.getBigIntValue()));
  }

  /**
   * Sets the specified {@code FloatColumn} to the key string
   *
   * @param column a {@code FloatColumn} to be set
   */
  @Override
  public void visit(FloatColumn column) {
    columns.add(String.valueOf(column.getFloatValue()));
  }

  /**
   * Sets the specified {@code DoubleColumn} to the key string
   *
   * @param column a {@code DoubleColumn} to be set
   */
  @Override
  public void visit(DoubleColumn column) {
    columns.add(String.valueOf(column.getDoubleValue()));
  }

  /**
   * Sets the specified {@code TextColumn} to the key string
   *
   * @param column a {@code TextColumn} to be set
   */
  @Override
  public void visit(TextColumn column) {
    column.getValue().ifPresent(columns::add);
  }

  /**
   * Sets the specified {@code BlobColumn} to the key string
   *
   * @param column a {@code BlobColumn} to be set
   */
  @Override
  public void visit(BlobColumn column) {
    if (!column.hasNullValue()) {
      // Use Base64 encoding
      columns.add(
          Base64.getUrlEncoder().withoutPadding().encodeToString(column.getBlobValueAsBytes()));
    }
  }
}
