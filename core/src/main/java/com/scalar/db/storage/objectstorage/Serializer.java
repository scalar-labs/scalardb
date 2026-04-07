package com.scalar.db.storage.objectstorage;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.esotericsoftware.kryo.kryo5.objenesis.strategy.StdInstantiatorStrategy;
import com.esotericsoftware.kryo.kryo5.util.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.kryo5.util.Pool;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class Serializer {
  public static final int MAX_STRING_LENGTH_ALLOWED = Integer.MAX_VALUE;

  private static final Pool<Kryo> kryoPool =
      new Pool<Kryo>(true, false) {
        @Override
        protected Kryo create() {
          Kryo kryo = new Kryo();
          kryo.setRegistrationRequired(false);
          kryo.setInstantiatorStrategy(
              new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
          return kryo;
        }
      };

  @SuppressWarnings("unchecked")
  public static <T> T deserialize(byte[] data) {
    Kryo kryo = kryoPool.obtain();
    try (InflaterInputStream iis = new InflaterInputStream(new ByteArrayInputStream(data));
        Input input = new Input(iis)) {
      return (T) kryo.readClassAndObject(input);
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize the object.", e);
    } finally {
      kryoPool.free(kryo);
    }
  }

  public static <T> byte[] serialize(T object) {
    Kryo kryo = kryoPool.obtain();
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (DeflaterOutputStream dos = new DeflaterOutputStream(baos);
          Output output = new Output(dos)) {
        kryo.writeClassAndObject(output, object);
      }
      return baos.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize the object.", e);
    } finally {
      kryoPool.free(kryo);
    }
  }
}
