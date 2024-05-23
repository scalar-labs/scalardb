package com.scalar.db;

import java.util.Objects;
import javax.annotation.Nullable;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.api.tasks.testing.Test;
import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaToolchainService;
import org.gradle.jvm.toolchain.JvmVendorSpec;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plugin configuring compilation tasks to use JDK 8 (temurin) and integration test tasks to use a
 * given SDK defined by the following Gradle properties:
 *
 * <ul>
 *   <li>`integrationTestJavaVersion` : configure all tasks name starting with "integrationTest" to
 *       run with the given SDK version
 *   <li>`integrationTestJavaVendor` : configure all tasks name starting with "integrationTest" to
 *       run with the given SDK vendor
 * </ul>
 *
 * Usage example using the CLI :
 *
 * <pre><code>
 *   gradle integrationTestJdbc -PintegrationTestJavaVersion=11 -PintegrationTestJavaVendor=amazon
 * </code></pre>
 */
public class JdkConfigurationPlugin implements Plugin<Project> {
  private static final Logger logger = LoggerFactory.getLogger(JdkConfigurationPlugin.class);

  private static final JavaLanguageVersion COMPILATION_JAVA_VERSION = JavaLanguageVersion.of(8);
  private static final JvmVendorSpec COMPILATION_JAVA_VENDOR = JvmVendorSpec.ADOPTIUM;
  private static final String INTEGRATION_TEST_JAVA_VERSION_PROP = "integrationTestJavaVersion";
  private static final String INTEGRATION_TEST_JAVA_VENDOR_PROP = "integrationTestJavaVendor";
  @Nullable private JavaLanguageVersion integrationTestJavaVersion;
  @Nullable private JvmVendorSpec integrationTestJavaVendor;

  @Override
  public void apply(@NotNull Project project) {
    parseIntegrationTestInputProperties(project);
    configureJdkForCompilationTasks(project);
    configureJdkForIntegrationTestTasks(project);
  }

  private void configureJdkForCompilationTasks(Project project) {
    project
        .getTasks()
        .withType(JavaCompile.class)
        .configureEach(
            javaCompileTask ->
                javaCompileTask
                    .getJavaCompiler()
                    .set(
                        getJavaToolchainService(project)
                            .compilerFor(
                                config -> {
                                  config.getLanguageVersion().set(COMPILATION_JAVA_VERSION);
                                  config.getVendor().set(COMPILATION_JAVA_VENDOR);
                                  logger.debug(
                                      "Configure task '{}' to use JDK {} ({})",
                                      javaCompileTask.getName(),
                                      COMPILATION_JAVA_VERSION,
                                      COMPILATION_JAVA_VENDOR);
                                })));
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

  private static @NotNull JavaToolchainService getJavaToolchainService(Project project) {
    return project.getExtensions().getByType(JavaToolchainService.class);
  }

  private void parseIntegrationTestInputProperties(Project project) {
    parseIntegrationTestVersionInputProperty(project);
    parseIntegrationTestVendorInputProperty(project);
    if (integrationTestJavaVersion == null && integrationTestJavaVendor != null) {
      throw new IllegalArgumentException(
          "When configuring the JDK vendor to run integration test, you also need to set the JDK version using the \""
              + INTEGRATION_TEST_JAVA_VERSION_PROP
              + "\" project property.");
    }
  }

  private void parseIntegrationTestVersionInputProperty(Project project) {
    if (project.hasProperty(INTEGRATION_TEST_JAVA_VERSION_PROP)) {
      integrationTestJavaVersion =
          JavaLanguageVersion.of(
              Objects.toString(project.property(INTEGRATION_TEST_JAVA_VERSION_PROP)));
    }
  }

  private void parseIntegrationTestVendorInputProperty(Project project) {
    if (!project.getRootProject().hasProperty(INTEGRATION_TEST_JAVA_VENDOR_PROP)) {
      return;
    }
    String propertyValue = Objects.toString(project.property(INTEGRATION_TEST_JAVA_VENDOR_PROP));
    switch (propertyValue) {
      case "corretto":
        integrationTestJavaVendor = JvmVendorSpec.AMAZON;
        break;
      case "temurin":
        integrationTestJavaVendor = JvmVendorSpec.ADOPTIUM;
        break;
      default:
        integrationTestJavaVendor = JvmVendorSpec.matching(propertyValue);
    }
  }
}
