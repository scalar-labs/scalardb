package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.google.auth.Credentials;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class SpannerCredentialsProviderTest {

  @AfterEach
  void tearDown() {
    SpannerCredentialsProvider.clear();
  }

  @Test
  void getCredentials_AfterRegister_ReturnsRegistered() {
    Credentials credentials = mock(Credentials.class);
    SpannerCredentialsProvider.register(credentials);

    SpannerCredentialsProvider provider = new SpannerCredentialsProvider();

    assertThat(provider.getCredentials()).isSameAs(credentials);
  }

  @Test
  void register_SameCredentialsTwice_IsIdempotent() {
    Credentials credentials = mock(Credentials.class);
    SpannerCredentialsProvider.register(credentials);
    SpannerCredentialsProvider.register(credentials);

    assertThat(new SpannerCredentialsProvider().getCredentials()).isSameAs(credentials);
  }

  @Test
  void register_DifferentCredentials_ThrowsIllegalStateException() {
    Credentials first = mock(Credentials.class);
    Credentials second = mock(Credentials.class);
    SpannerCredentialsProvider.register(first);

    assertThatThrownBy(() -> SpannerCredentialsProvider.register(second))
        .isInstanceOf(IllegalStateException.class);
    assertThat(new SpannerCredentialsProvider().getCredentials()).isSameAs(first);
  }

  @Test
  void getCredentials_WithoutRegister_ThrowsIllegalStateException() {
    SpannerCredentialsProvider.clear();

    assertThatThrownBy(() -> new SpannerCredentialsProvider().getCredentials())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void register_NullCredentials_ThrowsNullPointerException() {
    assertThatThrownBy(() -> SpannerCredentialsProvider.register(null))
        .isInstanceOf(NullPointerException.class);
  }
}
