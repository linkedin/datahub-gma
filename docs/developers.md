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

You're probably using a Java version that's too new for gradle. Run the following command to check your Java version

```
java --version
```

While it may be possible to build and run DataHub using newer versions of Java, we currently only support
[Java 1.8](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html) (aka Java 8). Plan for Java 11
migration is being discussed in [this issue](https://github.com/linkedin/datahub/issues/1699).

### Getting `cannot find symbol` error for `javax.annotation.Generated`

Similar to the previous issue, please use Java 1.8 to build the project.

You can install multiple version of Java on a single machine and switch between them using the `JAVA_HOME` environment
variable. See [this document](https://docs.oracle.com/cd/E21454_01/html/821-2531/inst_jdk_javahome_t.html) for more
details.
