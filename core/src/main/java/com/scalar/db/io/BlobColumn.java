package com.scalar.db.io;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import com.google.common.primitives.UnsignedBytes;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/** A {@code Column} for an BLOB type. */
@Immutable
public class BlobColumn implements Column<ByteBuffer> {

  private final String name;
  @Nullable private final byte[] value;

  private BlobColumn(String name, @Nullable ByteBuffer value) {
    this.name = Objects.requireNonNull(name);
    if (value == null) {
      this.value = null;
    } else {
      this.value = new byte[value.remaining()];
      value.get(this.value);
    }
  }

  private BlobColumn(String name, @Nullable byte[] value) {
    this.name = name;
    if (value == null) {
      this.value = null;
    } else {
      this.value = new byte[value.length];
      System.arraycopy(value, 0, this.value, 0, value.length);
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Optional<ByteBuffer> getValue() {
    return Optional.ofNullable(getBlobValueAsByteBuffer());
  }

  @Override
  @Nullable
  public ByteBuffer getBlobValueAsByteBuffer() {
    if (value == null) {
      return null;
    }
    return ByteBuffer.wrap(value);
  }

  @Override
  @Nullable
  public byte[] getBlobValueAsBytes() {
    if (value == null) {
      return null;
    }
    byte[] ret = new byte[value.length];
    System.arraycopy(value, 0, ret, 0, value.length);
    return ret;
  }

  @Override
  public BlobColumn copyWith(String name) {
    return new BlobColumn(name, value);
  }

  @Override
  public DataType getDataType() {
    return DataType.BLOB;
  }

  @Override
  public boolean hasNullValue() {
    return value == null;
  }

  @Override
  @Nullable
  public Object getValueAsObject() {
    return getBlobValueAsByteBuffer();
  }

  @Override
  public int compareTo(Column<ByteBuffer> o) {
    return ComparisonChain.start()
        .compare(getName(), o.getName())
        .compareTrueFirst(hasNullValue(), o.hasNullValue())
        .compare(
            getBlobValueAsBytes(),
            o.getBlobValueAsBytes(),
            Comparator.nullsFirst(UnsignedBytes.lexicographicalComparator()))
        .result();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BlobColumn)) {
      return false;
    }
    BlobColumn that = (BlobColumn) o;
    return Objects.equals(name, that.name) && Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(name);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  @Override
  public void accept(ColumnVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("name", name).add("value", value).toString();
  }

  /**
   * Returns a Blob column instance with the specified column name and value.
   *
   * @param columnName a column name
   * @param value a column value
   * @return a Blob column instance with the specified column name and value
   */
  public static BlobColumn of(String columnName, @Nullable ByteBuffer value) {
    return new BlobColumn(columnName, value);
  }

  /**
   * Returns a Blob column instance with the specified column name and value.
   *
   * @param columnName a column name
   * @param value a column value
   * @return a Blob column instance with the specified column name and value
   */
  public static BlobColumn of(String columnName, @Nullable byte[] value) {
    return new BlobColumn(columnName, value);
  }

  /**
   * Returns a Blob column instance with the specified column name and a null value.
   *
   * @param columnName a column name
   * @return a Blob column instance with the specified column name and a null value
   */
  public static BlobColumn ofNull(String columnName) {
    return new BlobColumn(columnName, (ByteBuffer) null);
  }
}
