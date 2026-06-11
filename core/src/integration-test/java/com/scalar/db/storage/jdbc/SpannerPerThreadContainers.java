package com.scalar.db.storage.jdbc;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation indicating that the {@link SpannerEmulatorExtension} should NOT start a
 * per-class container for this test class. The class is responsible for starting per-thread
 * containers via {@link SpannerEmulatorContainerSupport#startContainers(String, int)}.
 *
 * <p>When this annotation is present, {@link SpannerEmulatorExtension#beforeAll} skips the
 * per-class container start, and {@link SpannerEmulatorExtension#afterAll} stops all containers
 * registered via {@link SpannerEmulatorContainerSupport#startContainers(String, int)}.
 *
 * <p>{@code @Retention(RetentionPolicy.RUNTIME)} is mandatory — without it, {@code
 * isAnnotationPresent} returns {@code false} at runtime and the extension silently starts an extra
 * container.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface SpannerPerThreadContainers {}
