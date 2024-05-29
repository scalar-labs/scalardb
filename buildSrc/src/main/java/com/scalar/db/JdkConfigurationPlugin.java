package com.scalar.db;

import java.util.Objects;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.testing.Test;
import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaToolchainService;
import org.gradle.jvm.toolchain.JvmVendorSpec;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plugin configuring all tasks and integration test tasks to use a given JDK defined by the
 * following Gradle properties:
 *
 * <ul>
 *   <li>`javaVersion` : configure all Java related tasks to use the given JDK version. The default
 *       java version 8.
 *   <li>`javaVendor` : configure all Java related tasks to use the given JDK vendor. The default
 *       java vendor is Adoptium (also known as Temurin)
 *   <li>`integrationTestJavaRuntimeVersion` : configure all test tasks name starting with
 *       "integrationTest" to run with the given JDK version. The default Java runtime version is 8.
 *   <li>`integrationTestJavaRuntimeVendor` : configure all test tasks name starting with
 *       "integrationTest" to run with the given JDK vendor. The default Java runtime vendor is
 *       Adoptium (also known as Temurin)
 * </ul>
 *
 * <p>Usage example using the CLI:
 *
 * <p>1. To use JDK 11 (amazon) for all Java tasks including integration tests
 *
 * <pre><code>
 *   gradle integrationTestJdbc -PjavaVersion=11 -PjavaVendor=amazon
 * </code></pre>
 *
 * 2. To use JDK 11 (amazon) for all Java tasks while having integration test use JDK 17 (microsoft)
 *
 * <pre><code>
 *   gradle integrationTestJdbc -PjavaVersion=11 -PjavaVendor=amazon -PintegrationTestJavaRuntimeVersion=17 -PintegrationTestJavaRuntimeVendor=microsoft
 * </code></pre>
 */
public class JdkConfigurationPlugin implements Plugin<Project> {

  private static final Logger logger = LoggerFactory.getLogger(JdkConfigurationPlugin.class);
  // JDK 8 (temurin) is used as the default compiler
  private static final JavaLanguageVersion DEFAULT_JAVA_VERSION = JavaLanguageVersion.of(8);
  private static final JvmVendorSpec DEFAULT_JAVA_VENDOR = JvmVendorSpec.ADOPTIUM;

  private static final String JAVA_VERSION_PROP = "javaVersion";
  private static final String JAVA_VENDOR_PROP = "javaVendor";
  private static final String INTEGRATION_TEST_JAVA_RUNTIME_VERSION_PROP =
      "integrationTestJavaRuntimeVersion";
  private static final String INTEGRATION_TEST_JAVA_RUNTIME_VENDOR_PROP =
      "integrationTestJavaRuntimeVendor";

  private JavaLanguageVersion javaVersion = DEFAULT_JAVA_VERSION;
  private JvmVendorSpec javaVendor = DEFAULT_JAVA_VENDOR;
  @Nullable private JavaLanguageVersion integrationTestJavaVersion;
  @Nullable private JvmVendorSpec integrationTestJavaVendor;

  @Override
  public void apply(@NotNull Project project) {
    parseIntegrationTestInputProperties(project);
    configureJdkForAllJavaTasks(project);
    configureJdkForIntegrationTestTasks(project);
  }

  private void configureJdkForAllJavaTasks(Project project) {
    JavaPluginExtension javaPlugin = project.getExtensions().getByType(JavaPluginExtension.class);
    javaPlugin.getToolchain().getLanguageVersion().set(javaVersion);
    javaPlugin.getToolchain().getVendor().set(javaVendor);
    logger.debug("Configure JDK {} ({}) for Java tasks", javaVersion, javaVendor);
  }

  private void configureJdkForIntegrationTestTasks(Project project) {
    if (integrationTestJavaVersion == null) {
      return;
    }
    project
        .getTasks()
        .withType(Test.class)
        .matching(testTask -> testTask.getName().startsWith("integrationTest"))
        .configureEach(
            integrationTestTask ->
                integrationTestTask
                    .getJavaLauncher()
                    .set(
                        getJavaToolchainService(project)
                            .launcherFor(
                                config -> {
                                  config.getLanguageVersion().set(integrationTestJavaVersion);
                                  logger.debug(
                                      "Configure task '{}' to use JDK version {}",
                                      integrationTestTask.getName(),
                                      integrationTestJavaVersion);
                                  if (integrationTestJavaVendor != null) {
                                    config.getVendor().set(integrationTestJavaVendor);
                                    logger.debug(
                                        "Configure task '{}' to use {} JDK vendor",
                                        integrationTestTask.getName(),
                                        integrationTestJavaVendor);
                                  }
                                })));
  }

  private void parseIntegrationTestInputProperties(Project project) {
    parseVersionInputProperty(project, JAVA_VERSION_PROP, (version) -> javaVersion = version);
    parseVendorInputProperty(project, JAVA_VENDOR_PROP, (vendor) -> javaVendor = vendor);
    parseVersionInputProperty(
        project,
        INTEGRATION_TEST_JAVA_RUNTIME_VERSION_PROP,
        (version) -> integrationTestJavaVersion = version);
    parseVendorInputProperty(
        project,
        INTEGRATION_TEST_JAVA_RUNTIME_VENDOR_PROP,
        (vendor) -> integrationTestJavaVendor = vendor);
  }

  private void parseVersionInputProperty(
      Project project, String property, Consumer<JavaLanguageVersion> attributeSetter) {
    if (project.hasProperty(property)) {
      attributeSetter.accept(JavaLanguageVersion.of(Objects.toString(project.property(property))));
    }
  }

  private void parseVendorInputProperty(
      Project project, String property, Consumer<JvmVendorSpec> attributeSetter) {
    if (!project.getRootProject().hasProperty(property)) {
      return;
    }
    String propertyValue = Objects.toString(project.property(property));
    switch (propertyValue) {
      case "corretto":
        attributeSetter.accept(JvmVendorSpec.AMAZON);
        break;
      case "temurin":
        attributeSetter.accept(JvmVendorSpec.ADOPTIUM);
        break;
      default:
        attributeSetter.accept(JvmVendorSpec.matching(propertyValue));
    }
  }

  private @NotNull JavaToolchainService getJavaToolchainService(Project project) {
    return project.getExtensions().getByType(JavaToolchainService.class);
  }
}
