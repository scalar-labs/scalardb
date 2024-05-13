package com.scalar.db;

import java.util.Objects;
import java.util.function.Consumer;
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
 * Plugin configuring compilation and integration test tasks to use a given SDK defined by the
 * following Gradle properties:
 *
 * <ul>
 *   <li>`compilationJavaVendor` : configure the SDK vendor used by compilation tasks
 *   <li>`integrationTestJavaVersion` : configure all tasks name starting with "integrationTest" to
 *       run with the given SDK version
 *   <li>`integrationTestJavaVendor` : configure all tasks name starting with "integrationTest" to
 *       run with the given SDK vendor
 * </ul>
 *
 * Usage example using the CLI :
 *
 * <pre><code>
 *   gradle integrationTestJdbc -PcompilationJavaVendor=amazon -PintegrationTestJavaVersion=11 -PintegrationJavaVendor=temurin
 * </code></pre>
 */
public class JdkConfigurationPlugin implements Plugin<Project> {
  private static final Logger logger = LoggerFactory.getLogger(JdkConfigurationPlugin.class);

  private static final JavaLanguageVersion COMPILATION_JAVA_VERSION = JavaLanguageVersion.of(8);
  @Nullable private JvmVendorSpec compilationJavaVendor;
  @Nullable private JavaLanguageVersion integrationTestJavaVersion;
  @Nullable private JvmVendorSpec integrationTestJavaVendor;

  @Override
  public void apply(@NotNull Project project) {
    parseInputParameters(project);
    configureJdkForCompilationTasks(project);
    configureJdkForIntegrationTestTasks(project);
  }

  private void configureJdkForCompilationTasks(Project project) {
    project
        .getTasks()
        .withType(JavaCompile.class)
        .configureEach(
            javaCompileTask -> {
              javaCompileTask
                  .getJavaCompiler()
                  .set(
                      getJavaToolchainService(project)
                          .compilerFor(
                              config -> {
                                config.getLanguageVersion().set(COMPILATION_JAVA_VERSION);
                                logVersion(javaCompileTask.getName(), COMPILATION_JAVA_VERSION);
                                if (compilationJavaVendor != null) {
                                  config.getVendor().set(compilationJavaVendor);
                                  logVendor(javaCompileTask.getName(), compilationJavaVendor);
                                }
                              }));
            });
  }

  private void configureJdkForIntegrationTestTasks(Project project) {
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
                                  if (integrationTestJavaVersion == null) {
                                    return;
                                  }
                                  config.getLanguageVersion().set(integrationTestJavaVersion);
                                  logVersion(
                                      integrationTestTask.getName(), integrationTestJavaVersion);
                                  if (integrationTestJavaVendor != null) {
                                    config.getVendor().set(integrationTestJavaVendor);
                                    logVendor(
                                        integrationTestTask.getName(), integrationTestJavaVendor);
                                  }
                                })));
  }

  private static @NotNull JavaToolchainService getJavaToolchainService(Project project) {
    return project.getExtensions().getByType(JavaToolchainService.class);
  }

  private void parseInputParameters(Project project) {
    parseVendor(project, "compilationJavaVendor", (vendor) -> compilationJavaVendor = vendor);
    parseVendor(
        project, "integrationTestJavaVendor", (vendor) -> integrationTestJavaVendor = vendor);
    if (project.hasProperty("integrationTestJavaVersion")) {
      integrationTestJavaVersion =
          JavaLanguageVersion.of(Objects.toString(project.property("integrationTestJavaVersion")));
    }
    if (integrationTestJavaVersion == null && integrationTestJavaVendor != null) {
      throw new IllegalArgumentException(
          "When configuring the JDK vendor to run integration test, you also need to set the JDK version using the \"integrationTestJavaVersion\" project property.");
    }
  }

  private void parseVendor(
      Project project, String parameterName, Consumer<JvmVendorSpec> attributeSetter) {
    if (!project.getRootProject().hasProperty(parameterName)) {
      return;
    }
    String propertyValue = Objects.toString(project.property(parameterName));
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

  private void logVersion(String taskName, Object version) {
    logger.debug("Configure task '{}' to use JDK version {}", taskName, version);
  }

  private void logVendor(String taskName, JvmVendorSpec vendor) {
    logger.debug("Configure task '{}' to use {} JDK vendor", taskName, vendor);
  }
}
