package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class ObjectStorageDataSerializerFactoryTest {

  @Test
  void create_CborWithNone_ShouldReturnCborDataSerializer() {
    ObjectStorageDataSerializer serializer =
        ObjectStorageDataSerializerFactory.create(
            ObjectStorageFormat.CBOR, ObjectStorageCompression.NONE);
    assertThat(serializer).isInstanceOf(CborDataSerializer.class);
  }

  @Test
  void create_ForyWithNone_ShouldReturnForyDataSerializer() {
    ObjectStorageDataSerializer serializer =
        ObjectStorageDataSerializerFactory.create(
            ObjectStorageFormat.FORY, ObjectStorageCompression.NONE);
    assertThat(serializer).isInstanceOf(ForyDataSerializer.class);
  }

  @Test
  void create_IonWithNone_ShouldReturnIonDataSerializer() {
    ObjectStorageDataSerializer serializer =
        ObjectStorageDataSerializerFactory.create(
            ObjectStorageFormat.ION, ObjectStorageCompression.NONE);
    assertThat(serializer).isInstanceOf(IonDataSerializer.class);
  }

  @Test
  void create_JsonWithNone_ShouldReturnJsonDataSerializer() {
    ObjectStorageDataSerializer serializer =
        ObjectStorageDataSerializerFactory.create(
            ObjectStorageFormat.JSON, ObjectStorageCompression.NONE);
    assertThat(serializer).isInstanceOf(JsonDataSerializer.class);
  }

  @Test
  void create_SmileWithNone_ShouldReturnSmileDataSerializer() {
    ObjectStorageDataSerializer serializer =
        ObjectStorageDataSerializerFactory.create(
            ObjectStorageFormat.SMILE, ObjectStorageCompression.NONE);
    assertThat(serializer).isInstanceOf(SmileDataSerializer.class);
  }

  @Test
  void create_CborWithGzip_ShouldReturnGzipCompressingSerializer() {
    ObjectStorageDataSerializer serializer =
        ObjectStorageDataSerializerFactory.create(
            ObjectStorageFormat.CBOR, ObjectStorageCompression.GZIP);
    assertThat(serializer).isInstanceOf(GzipCompressingSerializer.class);
  }

  @Test
  void create_CborWithZstd_ShouldReturnZstdCompressingSerializer() {
    ObjectStorageDataSerializer serializer =
        ObjectStorageDataSerializerFactory.create(
            ObjectStorageFormat.CBOR, ObjectStorageCompression.ZSTD);
    assertThat(serializer).isInstanceOf(ZstdCompressingSerializer.class);
  }
}
