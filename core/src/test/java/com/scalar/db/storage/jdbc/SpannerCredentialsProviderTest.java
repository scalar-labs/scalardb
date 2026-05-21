package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.google.auth.Credentials;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class SpannerCredentialsProviderTest {

  @AfterEach
  void tearDown() {
    SpannerCredentialsProvider.clear();
  }

  @Test
  void getCredentials_AfterRegister_ReturnsRegistered() throws Exception {
    Credentials credentials = mock(Credentials.class);
    String className = SpannerCredentialsProvider.register(credentials, key("a"));

    SpannerCredentialsProvider provider = instantiate(className);

    assertThat(provider.getCredentials()).isSameAs(credentials);
  }

  @Test
  void register_FirstCredentials_AssignsSlot0() {
    String className = SpannerCredentialsProvider.register(mock(Credentials.class), key("a"));

    assertThat(className).isEqualTo(SpannerCredentialsProvider.Slot0.class.getName());
  }

  @Test
  void register_SameIdentityKeyTwice_ReturnsSameSlot() throws Exception {
    Credentials first = mock(Credentials.class);
    Credentials second = mock(Credentials.class); // Different instance, same identity bytes.
    byte[] identity = key("a");

    String firstSlot = SpannerCredentialsProvider.register(first, identity);
    String secondSlot = SpannerCredentialsProvider.register(second, identity);

    assertThat(secondSlot).isEqualTo(firstSlot);
    // The slot keeps the first registered credentials — re-registering doesn't overwrite.
    assertThat(instantiate(firstSlot).getCredentials()).isSameAs(first);
  }

  @Test
  void register_DifferentIdentityKeys_AssignsDifferentSlots() throws Exception {
    Credentials a = mock(Credentials.class);
    Credentials b = mock(Credentials.class);

    String classA = SpannerCredentialsProvider.register(a, key("a"));
    String classB = SpannerCredentialsProvider.register(b, key("b"));

    assertThat(classA).isEqualTo(SpannerCredentialsProvider.Slot0.class.getName());
    assertThat(classB).isEqualTo(SpannerCredentialsProvider.Slot1.class.getName());
    assertThat(instantiate(classA).getCredentials()).isSameAs(a);
    assertThat(instantiate(classB).getCredentials()).isSameAs(b);
  }

  @Test
  void register_FillsAllFiveSlots_ReturnsDistinctSlotClasses() throws Exception {
    Credentials c0 = mock(Credentials.class);
    Credentials c1 = mock(Credentials.class);
    Credentials c2 = mock(Credentials.class);
    Credentials c3 = mock(Credentials.class);
    Credentials c4 = mock(Credentials.class);

    String n0 = SpannerCredentialsProvider.register(c0, key("0"));
    String n1 = SpannerCredentialsProvider.register(c1, key("1"));
    String n2 = SpannerCredentialsProvider.register(c2, key("2"));
    String n3 = SpannerCredentialsProvider.register(c3, key("3"));
    String n4 = SpannerCredentialsProvider.register(c4, key("4"));

    assertThat(n0).isEqualTo(SpannerCredentialsProvider.Slot0.class.getName());
    assertThat(n1).isEqualTo(SpannerCredentialsProvider.Slot1.class.getName());
    assertThat(n2).isEqualTo(SpannerCredentialsProvider.Slot2.class.getName());
    assertThat(n3).isEqualTo(SpannerCredentialsProvider.Slot3.class.getName());
    assertThat(n4).isEqualTo(SpannerCredentialsProvider.Slot4.class.getName());
    assertThat(instantiate(n0).getCredentials()).isSameAs(c0);
    assertThat(instantiate(n1).getCredentials()).isSameAs(c1);
    assertThat(instantiate(n2).getCredentials()).isSameAs(c2);
    assertThat(instantiate(n3).getCredentials()).isSameAs(c3);
    assertThat(instantiate(n4).getCredentials()).isSameAs(c4);
  }

  @Test
  void register_SixthDistinctIdentityKey_ThrowsIllegalStateException() {
    SpannerCredentialsProvider.register(mock(Credentials.class), key("0"));
    SpannerCredentialsProvider.register(mock(Credentials.class), key("1"));
    SpannerCredentialsProvider.register(mock(Credentials.class), key("2"));
    SpannerCredentialsProvider.register(mock(Credentials.class), key("3"));
    SpannerCredentialsProvider.register(mock(Credentials.class), key("4"));

    assertThatThrownBy(() -> SpannerCredentialsProvider.register(mock(Credentials.class), key("5")))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void register_AlreadyRegisteredIdentityAfterSlotsFull_ReturnsExistingSlot() {
    String firstSlot = SpannerCredentialsProvider.register(mock(Credentials.class), key("0"));
    SpannerCredentialsProvider.register(mock(Credentials.class), key("1"));
    SpannerCredentialsProvider.register(mock(Credentials.class), key("2"));
    SpannerCredentialsProvider.register(mock(Credentials.class), key("3"));
    SpannerCredentialsProvider.register(mock(Credentials.class), key("4"));

    String again = SpannerCredentialsProvider.register(mock(Credentials.class), key("0"));

    assertThat(again).isEqualTo(firstSlot);
  }

  @Test
  void register_IdentityKeyIsDefensivelyCopied() throws Exception {
    byte[] identity = key("a");
    Credentials credentials = mock(Credentials.class);

    String className = SpannerCredentialsProvider.register(credentials, identity);

    // Mutating the caller's array after registration must not affect lookup.
    identity[0] = (byte) (identity[0] ^ 0x01);
    String lookupWithMutated = SpannerCredentialsProvider.register(credentials, identity);
    assertThat(lookupWithMutated).isNotEqualTo(className);
    // And the original identity bytes should still find the original slot.
    String lookupWithOriginal = SpannerCredentialsProvider.register(credentials, key("a"));
    assertThat(lookupWithOriginal).isEqualTo(className);
  }

  @Test
  void getCredentials_WithoutRegister_ThrowsIllegalStateException() {
    SpannerCredentialsProvider.clear();

    assertThatThrownBy(() -> new SpannerCredentialsProvider.Slot0().getCredentials())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void register_NullCredentials_ThrowsNullPointerException() {
    assertThatThrownBy(() -> SpannerCredentialsProvider.register(null, key("a")))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void register_NullIdentityKey_ThrowsNullPointerException() {
    assertThatThrownBy(() -> SpannerCredentialsProvider.register(mock(Credentials.class), null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void clear_AfterRegister_ResetsAllSlots() {
    SpannerCredentialsProvider.register(mock(Credentials.class), key("0"));
    SpannerCredentialsProvider.register(mock(Credentials.class), key("1"));

    SpannerCredentialsProvider.clear();

    // After clear, the next registration should land in Slot0 again.
    String className = SpannerCredentialsProvider.register(mock(Credentials.class), key("0"));
    assertThat(className).isEqualTo(SpannerCredentialsProvider.Slot0.class.getName());
  }

  private static byte[] key(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Mirrors how the Spanner JDBC driver instantiates the credentials provider — by FQCN with a
   * no-arg constructor. Verifies that the slot subclasses are reachable that way.
   */
  private static SpannerCredentialsProvider instantiate(String className) throws Exception {
    return Class.forName(className)
        .asSubclass(SpannerCredentialsProvider.class)
        .getDeclaredConstructor()
        .newInstance();
  }
}
