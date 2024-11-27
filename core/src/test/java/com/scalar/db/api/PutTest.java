package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.Column;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.util.ScalarDbUtils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class PutTest {
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";

  private Put preparePut() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Put(partitionKey, clusteringKey);
  }

  @Test
  public void getPartitionKey_ProperKeyGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    Key expected = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    Put put = new Put(expected, clusteringKey);

    // Act
    Key actual = put.getPartitionKey();

    // Assert
    assertThat((Iterable<? extends Value<?>>) expected).isEqualTo(actual);
  }

  @Test
  public void getClusteringKey_ProperKeyGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key expected = new Key(ANY_NAME_1, ANY_TEXT_2);
    Put put = new Put(partitionKey, expected);

    // Act
    Optional<Key> actual = put.getClusteringKey();

    // Assert
    assertThat(actual).isEqualTo(Optional.of(expected));
  }

  @Test
  public void getClusteringKey_ClusteringKeyNotGivenInConstructor_ShouldReturnNull() {
    // Arrange
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Put put = new Put(partitionKey);

    // Act
    Optional<Key> actual = put.getClusteringKey();

    // Assert
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void withValue_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange
    Put put = preparePut();
    TextColumn column2 = TextColumn.of(ANY_NAME_2, ANY_TEXT_2);
    TextColumn column1 = TextColumn.of(ANY_NAME_1, ANY_TEXT_1);

    // Act
    put.withValue(column1).withValue(column2);

    // Assert
    assertThat(put.getValues())
        .isEqualTo(
            ImmutableMap.of(
                column1.getName(),
                ScalarDbUtils.toValue(column1),
                column2.getName(),
                ScalarDbUtils.toValue(column2)));

    assertThat(put.getColumns())
        .isEqualTo(ImmutableMap.of(column1.getName(), column1, column2.getName(), column2));

    assertThat(put.getContainedColumnNames()).isEqualTo(ImmutableSet.of(ANY_NAME_1, ANY_NAME_2));

    assertThat(put.containsColumn(ANY_NAME_1)).isTrue();
    assertThat(put.isNullValue(ANY_NAME_1)).isFalse();
    assertThat(put.getTextValue(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    assertThat(put.getValueAsObject(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);

    assertThat(put.containsColumn(ANY_NAME_2)).isTrue();
    assertThat(put.isNullValue(ANY_NAME_2)).isFalse();
    assertThat(put.getTextValue(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    assertThat(put.getValueAsObject(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
  }

  @Test
  public void withValue_ProperValuesGiven_ShouldReturnWhatsSet() {
    // Arrange
    Put put = preparePut();

    // Act
    put.withValue("val1", true)
        .withValue("val2", 5678)
        .withValue("val3", 1234L)
        .withValue("val4", 4.56f)
        .withValue("val5", 1.23)
        .withValue("val6", "string_value")
        .withValue("val7", "blob_value".getBytes(StandardCharsets.UTF_8))
        .withValue("val8", ByteBuffer.wrap("blob_value2".getBytes(StandardCharsets.UTF_8)));

    // Assert
    Map<String, Value<?>> values = put.getValues();
    assertThat(values.size()).isEqualTo(8);
    assertThat(values.get("val1")).isEqualTo(new BooleanValue("val1", true));
    assertThat(values.get("val2")).isEqualTo(new IntValue("val2", 5678));
    assertThat(values.get("val3")).isEqualTo(new BigIntValue("val3", 1234L));
    assertThat(values.get("val4")).isEqualTo(new FloatValue("val4", 4.56f));
    assertThat(values.get("val5")).isEqualTo(new DoubleValue("val5", 1.23));
    assertThat(values.get("val6")).isEqualTo(new TextValue("val6", "string_value"));
    assertThat(values.get("val7"))
        .isEqualTo(new BlobValue("val7", "blob_value".getBytes(StandardCharsets.UTF_8)));
    assertThat(values.get("val8"))
        .isEqualTo(new BlobValue("val8", "blob_value2".getBytes(StandardCharsets.UTF_8)));

    Map<String, Column<?>> columns = put.getColumns();
    assertThat(columns.size()).isEqualTo(8);
    assertThat(columns.get("val1")).isEqualTo(BooleanColumn.of("val1", true));
    assertThat(columns.get("val2")).isEqualTo(IntColumn.of("val2", 5678));
    assertThat(columns.get("val3")).isEqualTo(BigIntColumn.of("val3", 1234L));
    assertThat(columns.get("val4")).isEqualTo(FloatColumn.of("val4", 4.56f));
    assertThat(columns.get("val5")).isEqualTo(DoubleColumn.of("val5", 1.23));
    assertThat(columns.get("val6")).isEqualTo(TextColumn.of("val6", "string_value"));
    assertThat(columns.get("val7"))
        .isEqualTo(BlobColumn.of("val7", "blob_value".getBytes(StandardCharsets.UTF_8)));
    assertThat(columns.get("val8"))
        .isEqualTo(BlobColumn.of("val8", "blob_value2".getBytes(StandardCharsets.UTF_8)));

    assertThat(put.getContainedColumnNames())
        .isEqualTo(ImmutableSet.of("val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8"));

    assertThat(put.containsColumn("val1")).isTrue();
    assertThat(put.isNullValue("val1")).isFalse();
    assertThat(put.getBooleanValue("val1")).isTrue();
    assertThat(put.getValueAsObject("val1")).isEqualTo(true);

    assertThat(put.containsColumn("val2")).isTrue();
    assertThat(put.isNullValue("val2")).isFalse();
    assertThat(put.getIntValue("val2")).isEqualTo(5678);
    assertThat(put.getValueAsObject("val2")).isEqualTo(5678);

    assertThat(put.containsColumn("val3")).isTrue();
    assertThat(put.isNullValue("val3")).isFalse();
    assertThat(put.getBigIntValue("val3")).isEqualTo(1234L);
    assertThat(put.getValueAsObject("val3")).isEqualTo(1234L);

    assertThat(put.containsColumn("val4")).isTrue();
    assertThat(put.isNullValue("val4")).isFalse();
    assertThat(put.getFloatValue("val4")).isEqualTo(4.56f);
    assertThat(put.getValueAsObject("val4")).isEqualTo(4.56f);

    assertThat(put.containsColumn("val5")).isTrue();
    assertThat(put.isNullValue("val5")).isFalse();
    assertThat(put.getDoubleValue("val5")).isEqualTo(1.23);
    assertThat(put.getValueAsObject("val5")).isEqualTo(1.23);

    assertThat(put.containsColumn("val6")).isTrue();
    assertThat(put.isNullValue("val6")).isFalse();
    assertThat(put.getTextValue("val6")).isEqualTo("string_value");
    assertThat(put.getValueAsObject("val6")).isEqualTo("string_value");

    assertThat(put.containsColumn("val7")).isTrue();
    assertThat(put.isNullValue("val7")).isFalse();
    assertThat(put.getBlobValue("val7"))
        .isEqualTo(ByteBuffer.wrap("blob_value".getBytes(StandardCharsets.UTF_8)));
    assertThat(put.getBlobValueAsByteBuffer("val7"))
        .isEqualTo(ByteBuffer.wrap("blob_value".getBytes(StandardCharsets.UTF_8)));
    assertThat(put.getBlobValueAsBytes("val7"))
        .isEqualTo("blob_value".getBytes(StandardCharsets.UTF_8));
    assertThat(put.getValueAsObject("val7"))
        .isEqualTo(ByteBuffer.wrap("blob_value".getBytes(StandardCharsets.UTF_8)));

    assertThat(put.containsColumn("val8")).isTrue();
    assertThat(put.isNullValue("val8")).isFalse();
    assertThat(put.getBlobValue("val8"))
        .isEqualTo(ByteBuffer.wrap("blob_value2".getBytes(StandardCharsets.UTF_8)));
    assertThat(put.getBlobValueAsByteBuffer("val8"))
        .isEqualTo(ByteBuffer.wrap("blob_value2".getBytes(StandardCharsets.UTF_8)));
    assertThat(put.getBlobValueAsBytes("val8"))
        .isEqualTo("blob_value2".getBytes(StandardCharsets.UTF_8));
    assertThat(put.getValueAsObject("val8"))
        .isEqualTo(ByteBuffer.wrap("blob_value2".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void withXXXValue_ProperValuesGiven_ShouldReturnWhatsSet() {
    // Arrange
    Put put = preparePut();

    // Act
    put.withBooleanValue("val1", true)
        .withIntValue("val2", 5678)
        .withBigIntValue("val3", 1234L)
        .withFloatValue("val4", 4.56f)
        .withDoubleValue("val5", 1.23)
        .withTextValue("val6", "string_value")
        .withBlobValue("val7", "blob_value".getBytes(StandardCharsets.UTF_8))
        .withBlobValue("val8", ByteBuffer.wrap("blob_value2".getBytes(StandardCharsets.UTF_8)))
        .withBooleanValue("val9", null)
        .withIntValue("val10", null)
        .withBigIntValue("val11", null)
        .withFloatValue("val12", null)
        .withDoubleValue("val13", null)
        .withTextValue("val14", null)
        .withBlobValue("val15", (ByteBuffer) null)
        .withBlobValue("val16", (byte[]) null);

    // Assert
    Map<String, Value<?>> values = put.getValues();
    assertThat(values.size()).isEqualTo(16);
    assertThat(values.get("val1")).isEqualTo(new BooleanValue("val1", true));
    assertThat(values.get("val2")).isEqualTo(new IntValue("val2", 5678));
    assertThat(values.get("val3")).isEqualTo(new BigIntValue("val3", 1234L));
    assertThat(values.get("val4")).isEqualTo(new FloatValue("val4", 4.56f));
    assertThat(values.get("val5")).isEqualTo(new DoubleValue("val5", 1.23));
    assertThat(values.get("val6")).isEqualTo(new TextValue("val6", "string_value"));
    assertThat(values.get("val7"))
        .isEqualTo(new BlobValue("val7", "blob_value".getBytes(StandardCharsets.UTF_8)));
    assertThat(values.get("val8"))
        .isEqualTo(new BlobValue("val8", "blob_value2".getBytes(StandardCharsets.UTF_8)));
    assertThat(values).containsKey("val9");
    assertThat(values.get("val9")).isNull();
    assertThat(values).containsKey("val10");
    assertThat(values.get("val10")).isNull();
    assertThat(values).containsKey("val11");
    assertThat(values.get("val11")).isNull();
    assertThat(values).containsKey("val12");
    assertThat(values.get("val12")).isNull();
    assertThat(values).containsKey("val13");
    assertThat(values.get("val13")).isNull();
    assertThat(values).containsKey("val14");
    assertThat(values.get("val14")).isEqualTo(new TextValue("val14", (String) null));
    assertThat(values).containsKey("val15");
    assertThat(values.get("val15")).isEqualTo(new BlobValue("val15", (ByteBuffer) null));
    assertThat(values).containsKey("val16");
    assertThat(values.get("val16")).isEqualTo(new BlobValue("val16", (byte[]) null));

    Map<String, Column<?>> columns = put.getColumns();
    assertThat(columns.size()).isEqualTo(16);
    assertThat(columns.get("val1")).isEqualTo(BooleanColumn.of("val1", true));
    assertThat(columns.get("val2")).isEqualTo(IntColumn.of("val2", 5678));
    assertThat(columns.get("val3")).isEqualTo(BigIntColumn.of("val3", 1234L));
    assertThat(columns.get("val4")).isEqualTo(FloatColumn.of("val4", 4.56f));
    assertThat(columns.get("val5")).isEqualTo(DoubleColumn.of("val5", 1.23));
    assertThat(columns.get("val6")).isEqualTo(TextColumn.of("val6", "string_value"));
    assertThat(columns.get("val7"))
        .isEqualTo(BlobColumn.of("val7", "blob_value".getBytes(StandardCharsets.UTF_8)));
    assertThat(columns.get("val8"))
        .isEqualTo(BlobColumn.of("val8", "blob_value2".getBytes(StandardCharsets.UTF_8)));
    assertThat(columns.get("val9")).isEqualTo(BooleanColumn.ofNull("val9"));
    assertThat(columns.get("val10")).isEqualTo(IntColumn.ofNull("val10"));
    assertThat(columns.get("val11")).isEqualTo(BigIntColumn.ofNull("val11"));
    assertThat(columns.get("val12")).isEqualTo(FloatColumn.ofNull("val12"));
    assertThat(columns.get("val13")).isEqualTo(DoubleColumn.ofNull("val13"));
    assertThat(columns.get("val14")).isEqualTo(TextColumn.ofNull("val14"));
    assertThat(columns.get("val15")).isEqualTo(BlobColumn.ofNull("val15"));
    assertThat(columns.get("val16")).isEqualTo(BlobColumn.ofNull("val16"));

    assertThat(put.getContainedColumnNames())
        .isEqualTo(
            ImmutableSet.of(
                "val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8", "val9", "val10",
                "val11", "val12", "val13", "val14", "val15", "val16"));

    assertThat(put.containsColumn("val1")).isTrue();
    assertThat(put.isNullValue("val1")).isFalse();
    assertThat(put.getBooleanValue("val1")).isTrue();
    assertThat(put.getValueAsObject("val1")).isEqualTo(true);

    assertThat(put.containsColumn("val2")).isTrue();
    assertThat(put.isNullValue("val2")).isFalse();
    assertThat(put.getIntValue("val2")).isEqualTo(5678);
    assertThat(put.getValueAsObject("val2")).isEqualTo(5678);

    assertThat(put.containsColumn("val3")).isTrue();
    assertThat(put.isNullValue("val3")).isFalse();
    assertThat(put.getBigIntValue("val3")).isEqualTo(1234L);
    assertThat(put.getValueAsObject("val3")).isEqualTo(1234L);

    assertThat(put.containsColumn("val4")).isTrue();
    assertThat(put.isNullValue("val4")).isFalse();
    assertThat(put.getFloatValue("val4")).isEqualTo(4.56f);
    assertThat(put.getValueAsObject("val4")).isEqualTo(4.56f);

    assertThat(put.containsColumn("val5")).isTrue();
    assertThat(put.isNullValue("val5")).isFalse();
    assertThat(put.getDoubleValue("val5")).isEqualTo(1.23);
    assertThat(put.getValueAsObject("val5")).isEqualTo(1.23);

    assertThat(put.containsColumn("val6")).isTrue();
    assertThat(put.isNullValue("val6")).isFalse();
    assertThat(put.getTextValue("val6")).isEqualTo("string_value");
    assertThat(put.getValueAsObject("val6")).isEqualTo("string_value");

    assertThat(put.containsColumn("val7")).isTrue();
    assertThat(put.isNullValue("val7")).isFalse();
    assertThat(put.getBlobValue("val7"))
        .isEqualTo(ByteBuffer.wrap("blob_value".getBytes(StandardCharsets.UTF_8)));
    assertThat(put.getBlobValueAsByteBuffer("val7"))
        .isEqualTo(ByteBuffer.wrap("blob_value".getBytes(StandardCharsets.UTF_8)));
    assertThat(put.getBlobValueAsBytes("val7"))
        .isEqualTo("blob_value".getBytes(StandardCharsets.UTF_8));
    assertThat(put.getValueAsObject("val7"))
        .isEqualTo(ByteBuffer.wrap("blob_value".getBytes(StandardCharsets.UTF_8)));

    assertThat(put.containsColumn("val8")).isTrue();
    assertThat(put.isNullValue("val8")).isFalse();
    assertThat(put.getBlobValue("val8"))
        .isEqualTo(ByteBuffer.wrap("blob_value2".getBytes(StandardCharsets.UTF_8)));
    assertThat(put.getBlobValueAsByteBuffer("val8"))
        .isEqualTo(ByteBuffer.wrap("blob_value2".getBytes(StandardCharsets.UTF_8)));
    assertThat(put.getBlobValueAsBytes("val8"))
        .isEqualTo("blob_value2".getBytes(StandardCharsets.UTF_8));
    assertThat(put.getValueAsObject("val8"))
        .isEqualTo(ByteBuffer.wrap("blob_value2".getBytes(StandardCharsets.UTF_8)));

    assertThat(put.containsColumn("val9")).isTrue();
    assertThat(put.isNullValue("val9")).isTrue();
    assertThat(put.getBooleanValue("val9")).isFalse();
    assertThat(put.getValueAsObject("val9")).isNull();

    assertThat(put.containsColumn("val10")).isTrue();
    assertThat(put.isNullValue("val10")).isTrue();
    assertThat(put.getIntValue("val10")).isEqualTo(0);
    assertThat(put.getValueAsObject("val10")).isNull();

    assertThat(put.containsColumn("val11")).isTrue();
    assertThat(put.isNullValue("val11")).isTrue();
    assertThat(put.getBigIntValue("val11")).isEqualTo(0L);
    assertThat(put.getValueAsObject("val11")).isNull();

    assertThat(put.containsColumn("val12")).isTrue();
    assertThat(put.isNullValue("val12")).isTrue();
    assertThat(put.getFloatValue("val12")).isEqualTo(0.0F);
    assertThat(put.getValueAsObject("val12")).isNull();

    assertThat(put.containsColumn("val13")).isTrue();
    assertThat(put.isNullValue("val13")).isTrue();
    assertThat(put.getDoubleValue("val13")).isEqualTo(0.0);
    assertThat(put.getValueAsObject("val13")).isNull();

    assertThat(put.containsColumn("val14")).isTrue();
    assertThat(put.isNullValue("val14")).isTrue();
    assertThat(put.getTextValue("val14")).isNull();
    assertThat(put.getValueAsObject("val14")).isNull();

    assertThat(put.containsColumn("val15")).isTrue();
    assertThat(put.isNullValue("val15")).isTrue();
    assertThat(put.getBlobValue("val15")).isNull();
    assertThat(put.getValueAsObject("val15")).isNull();

    assertThat(put.containsColumn("val16")).isTrue();
    assertThat(put.isNullValue("val16")).isTrue();
    assertThat(put.getBlobValue("val16")).isNull();
    assertThat(put.getValueAsObject("val16")).isNull();
  }

  @Test
  public void withValues_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange
    Put put = preparePut();
    TextValue value1 = new TextValue(ANY_NAME_1, ANY_TEXT_1);
    TextValue value2 = new TextValue(ANY_NAME_2, ANY_TEXT_2);

    // Act
    put.withValues(Arrays.asList(value1, value2));

    // Assert
    assertThat(put.getValues())
        .isEqualTo(ImmutableMap.of(value1.getName(), value1, value2.getName(), value2));

    assertThat(put.getColumns())
        .isEqualTo(
            ImmutableMap.of(
                value1.getName(),
                ScalarDbUtils.toColumn(value1),
                value2.getName(),
                ScalarDbUtils.toColumn(value2)));

    assertThat(put.getContainedColumnNames()).isEqualTo(ImmutableSet.of(ANY_NAME_1, ANY_NAME_2));

    assertThat(put.containsColumn(ANY_NAME_1)).isTrue();
    assertThat(put.isNullValue(ANY_NAME_1)).isFalse();
    assertThat(put.getTextValue(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    assertThat(put.getValueAsObject(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);

    assertThat(put.containsColumn(ANY_NAME_2)).isTrue();
    assertThat(put.isNullValue(ANY_NAME_2)).isFalse();
    assertThat(put.getTextValue(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    assertThat(put.getValueAsObject(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
  }

  @Test
  public void getColumns_TryToModifyReturned_ShouldThrowException() {
    // Arrange
    Put put = preparePut();
    TextColumn column1 = TextColumn.of(ANY_NAME_1, ANY_TEXT_1);
    TextColumn column2 = TextColumn.of(ANY_NAME_2, ANY_TEXT_2);
    put.withValue(column1).withValue(column2);

    // Act Assert
    Map<String, Column<?>> values = put.getColumns();
    assertThatThrownBy(() -> values.put(ANY_NAME_3, TextColumn.of(ANY_NAME_3, ANY_TEXT_3)))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void withConsistency_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange
    Put put = preparePut();
    Consistency expected = Consistency.EVENTUAL;

    // Act
    put.withConsistency(expected);

    // Assert
    assertThat(expected).isEqualTo(put.getConsistency());
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(() -> new Put((Key) null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void constructor_PutGiven_ShouldCopyProperly() {
    // Arrange
    Put put =
        preparePut()
            .withValue("c1", 1)
            .withCondition(new PutIfExists())
            .withConsistency(Consistency.EVENTUAL)
            .forNamespace("n1")
            .forTable("t1");

    // Act
    Put actual = new Put(put);

    // Assert
    assertThat(actual).isEqualTo(put);
  }

  @Test
  public void equals_SameInstanceGiven_ShouldReturnTrue() {
    // Arrange
    Put put = preparePut();

    // Act
    @SuppressWarnings("SelfEquals")
    boolean ret = put.equals(put);

    // Assert
    assertThat(ret).isTrue();
  }

  @Test
  public void equals_SamePutGiven_ShouldReturnTrue() {
    // Arrange
    Put put = preparePut();
    Put another = preparePut();

    // Act
    boolean ret = put.equals(another);

    // Assert
    assertThat(ret).isTrue();
    assertThat(put.hashCode()).isEqualTo(another.hashCode());
  }

  @Test
  public void equals_SamePutWithPutIfExistsGiven_ShouldReturnTrue() {
    // Arrange
    Put put = preparePut().withCondition(new PutIfExists());
    Put another = preparePut().withCondition(new PutIfExists());

    // Act
    boolean ret = put.equals(another);

    // Assert
    assertThat(ret).isTrue();
    assertThat(put.hashCode()).isEqualTo(another.hashCode());
  }

  @Test
  public void equals_SamePutWithPutIfNotExistsGiven_ShouldReturnTrue() {
    // Arrange
    Put put = preparePut().withCondition(new PutIfNotExists());
    Put another = preparePut().withCondition(new PutIfNotExists());

    // Act
    boolean ret = put.equals(another);

    // Assert
    assertThat(ret).isTrue();
    assertThat(put.hashCode()).isEqualTo(another.hashCode());
  }

  @Test
  public void equals_PutWithDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    Put put = preparePut();
    put.withValue(ANY_NAME_3, ANY_TEXT_3);
    Put another = preparePut();
    another.withValue(ANY_NAME_3, ANY_TEXT_4);

    // Act
    boolean ret = put.equals(another);

    // Assert
    assertThat(ret).isFalse();
    assertThat(put.hashCode()).isNotEqualTo(another.hashCode());
  }

  @Test
  public void equals_PutWithDifferentConditionsGiven_ShouldReturnFalse() {
    // Arrange
    Put put = preparePut();
    put.withCondition(new PutIfExists());
    Put another = preparePut();
    another.withCondition(new PutIfNotExists());

    // Act
    boolean ret = put.equals(another);

    // Assert
    assertThat(ret).isFalse();
    assertThat(put.hashCode()).isNotEqualTo(another.hashCode());
  }

  @Test
  public void getAttribute_ShouldReturnProperValues() {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("pk", "pv"))
            .attribute("a1", "v1")
            .attributes(ImmutableMap.of("a2", "v2", "a3", "v3"))
            .build();

    // Act Assert
    assertThat(put.getAttribute("a1")).hasValue("v1");
    assertThat(put.getAttribute("a2")).hasValue("v2");
    assertThat(put.getAttribute("a3")).hasValue("v3");
    assertThat(put.getAttributes()).isEqualTo(ImmutableMap.of("a1", "v1", "a2", "v2", "a3", "v3"));
  }
}
