# GMA Developer's Guide

## Building the Project

Fork and clone the repository if haven't done so already

```
git clone https://github.com/{username}/datahub-gma.git
```

Change into the repository's root directory

```
cd datahub-gma
```

Use [gradle wrapper](https://docs.gradle.org/current/userguide/gradle_wrapper.html) to build the project

```
./gradlew build
```

If you are using Mac with Apple chip, there are additional steps required.

Install MariaDB.

```bash
brew install mariadb
```

Uncomment the following three lines in `EmbeddedMariaInstance.java`:

```java
configurationBuilder.setBaseDir("/opt/homebrew");
configurationBuilder.setUnpackingFromClasspath(false);
configurationBuilder.setLibDir(System.getProperty("java.io.tmpdir") + "/MariaDB4j/no-libs");
```

Now you can build the project.

## IDE Support

The recommended IDE for DataHub development is [IntelliJ IDEA](https://www.jetbrains.com/idea/). You can run the
following command to generate or update the IntelliJ project file:

```
./gradlew idea
```

Open `datahub.ipr` in IntelliJ to start developing!

For consistency please import and auto format the code using
[LinkedIn IntelliJ Java style](../gradle/idea/LinkedIn%20Style.xml).

## Common Build Issues

### Getting `Unsupported class file major version 57`

You're probably using a Java version that's too new for Gradle. Gradle 6.9.4 supports up to JDK 16. Run the following
command to check your Java version

```
java --version
```

This project requires [Java 11](https://adoptium.net/) or later to build. If you have multiple JDK versions installed,
you can switch between them using the `JAVA_HOME` environment variable. See
[this document](https://docs.oracle.com/cd/E21454_01/html/821-2531/inst_jdk_javahome_t.html) for more details.
