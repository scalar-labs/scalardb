> [!CAUTION]
> 
> This documentation has been moved to the centralized ScalarDB documentation repository, [docs-internal-scalardb](https://github.com/scalar-labs/docs-internal-scalardb). Please update this documentation in that repository instead.
> 
> To view the ScalarDB documentation, visit [ScalarDB Documentation](https://scalardb.scalar-labs.com/docs/).

# Add ScalarDB to Your Build

The ScalarDB library is available on the [Maven Central Repository](https://mvnrepository.com/artifact/com.scalar-labs/scalardb). You can add the library as a build dependency to your application by using Gradle or Maven.

## Configure your application based on your build tool

Select your build tool, and follow the instructions to add the build dependency for ScalarDB to your application.

<div id="tabset-1">
<div class="tab">
  <button class="tablinks" onclick="openTab(event, 'Gradle', 'tabset-1')" id="defaultOpen-1">Gradle</button>
  <button class="tablinks" onclick="openTab(event, 'Maven', 'tabset-1')">Maven</button>
</div>

<div id="Gradle" class="tabcontent" markdown="1">

To add the build dependency for ScalarDB by using Gradle, add the following to `build.gradle` in your application, replacing `<VERSION>` with the version of ScalarDB that you want to use:

```gradle
dependencies {
  implementation 'com.scalar-labs:scalardb:<VERSION>'
}
```
</div>
<div id="Maven" class="tabcontent" markdown="1">

To add the build dependency for ScalarDB by using Maven, add the following to `pom.xml` in your application, replacing `<VERSION>` with the version of ScalarDB that you want to use:

```xml
<dependency>
  <groupId>com.scalar-labs</groupId>
  <artifactId>scalardb</artifactId>
  <version><VERSION></version>
</dependency>
```
</div>
</div>
