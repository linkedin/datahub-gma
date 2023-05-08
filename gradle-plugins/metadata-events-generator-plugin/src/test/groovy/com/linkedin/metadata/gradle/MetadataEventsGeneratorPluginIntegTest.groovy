package com.linkedin.metadata.gradle


import org.apache.commons.io.FileUtils
import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

import java.nio.file.Path
import java.util.jar.JarFile


/**
 * Special instructions for invoking Gradle integration tests using {@link WrappedGradleRunnerBuilder}:
 *
 * <p>Gradle integration tests <b>must</b> be invoked from the CLI.
 *
 * <p>To invoke this test, run:
 * <br>{@code}$ ligradle :metadata-events-generator-plugin:integTest --tests=com.linkedin.metadata.gradle.MetadataEventsGeneratorPluginIntegTest{@code}
 *
 * <p>You can, however, debug from IntelliJ as long as the test is invoked via CLI.
 * <br>The inclusion of {@code}--no-daemon --debug-jvm{@code} arguments is critical!
 *
 * <p>To debug this test:
 * <ol>
 *   <li>Set a breakpoint(s) on your plugin class or task class</li>
 *   <li>Invoke test from CLI: {@code}$ ligradle :metadata-events-generator-plugin:integTest --tests=com.linkedin.metadata.gradle.MetadataEventsGeneratorPluginIntegTest --no-daemon --debug-jvm{@code}</li>
 *   <li>Attach from IntelliJ's {@pre}Remote{@pre} configuration type to localhost:5005</li>
 * </ol>
 *
 * Note that access to the {@link org.gradle.api.Project} API is very limited when using {@link WrappedGradleRunnerBuilder}.
 * <br>Overall build success/failure, individual task outcome and the stdout/stderr of the build are measurable.
 * <br>Write your own assertions for other attributes such as like expected task file outputs.
 */
class MetadataEventsGeneratorPluginIntegTest extends Specification {

  @Rule
  TemporaryFolder tempDir = new TemporaryFolder()

  def projectPath(String relative) {
    return tempDir.root.toPath().resolve(relative)
  }

  def projectFile(String relative) {
    return projectPath(relative).toFile()
  }

  def setup() {
    FileUtils.copyDirectory(
        new File(getClass().getResource("/metadata-models").getFile()), tempDir.root)

    def repositories = System.getProperty("repositories")
    def buildFile = projectPath('build.gradle')
    buildFile.write("""\
      apply from: '$repositories'
      """.stripMargin())

    def dependencies = System
        .getProperty("testProjectCompile")
        .split(",")
        .collect { x -> "'$x'" }
        .join(", ")

    def moduleBuildFile = projectPath('my-dummy-module/build.gradle')
    moduleBuildFile.write("""\
        plugins {
          id 'com.linkedin.metadata-events-generator'
        }
        
        dependencies {
          dataModel files($dependencies)
        }
        """.stripMargin())
  }

  def 'generate events'() {
    when:
    GradleRunner runner = GradleRunner
        .create()
        .withProjectDir(tempDir.root)
        .withArguments(':my-dummy-module:generateMetadataEventsSchema', '-is')
        .withPluginClasspath()
        .withDebug(Boolean.parseBoolean(System.getProperty("debug-jvm")))
    BuildResult result = runner.build()

    then:
    // all expressions below are implicit assertions!
    result.task(':my-dummy-module:generateMetadataEventsSchema').outcome == TaskOutcome.SUCCESS

    projectFile('build/my-dummy-module/generateMetadataEventsSchema/com/linkedin/mxe/foo/testAspect/MetadataChangeEvent.pdl').text == '''\
      namespace com.linkedin.mxe.foo.testAspect

      import com.linkedin.avro2pegasus.events.KafkaAuditHeader
      import com.linkedin.metadata.events.ChangeType
      import com.linkedin.testing.FooUrn
      import com.linkedin.metadata.test.aspects.TestAspect

      /**
       * MetadataChangeEvent for the FooUrn with TestAspect aspect.
       */
      @MetadataChangeEvent
      record MetadataChangeEvent {

        /**
         * Kafka audit header. See go/kafkaauditheader for more info.
         */
        auditHeader: optional KafkaAuditHeader

        /**
         * FooUrn as the key for the MetadataChangeEvent.
         */
        urn: FooUrn

        /**
         * Value of the proposed TestAspect change.
         */
        proposedValue: optional TestAspect

        /**
         * Change type.
         */
        changeType: optional union[null, ChangeType] = null
      }'''.stripIndent()

    projectFile('build/my-dummy-module/generateMetadataEventsSchema/com/linkedin/mxe/foo/testAspect/MetadataAuditEvent.pdl').text == '''\
      namespace com.linkedin.mxe.foo.testAspect

      import com.linkedin.avro2pegasus.events.KafkaAuditHeader
      import com.linkedin.metadata.events.ChangeType
      import com.linkedin.metadata.events.TrackingContext
      import com.linkedin.testing.FooUrn
      import com.linkedin.metadata.test.aspects.TestAspect

      /**
       * MetadataAuditEvent for the FooUrn with TestAspect aspect.
       */
      @MetadataAuditEvent
      record MetadataAuditEvent {

        /**
         * Kafka audit header for the MetadataAuditEvent.
         */
        auditHeader: optional KafkaAuditHeader

        /**
         * FooUrn as the key for the MetadataAuditEvent.
         */
        urn: FooUrn

        /**
         * Aspect of the TestAspect before the update.
         */
        oldValue: optional TestAspect

        /**
         * Aspect of the TestAspect after the update.
         */
        newValue: TestAspect

        /**
         * Change type.
         */
        changeType: optional union[null, ChangeType] = null
        
        /**
         * Tracking context to identify the lifecycle of the trackable item.
         */
        trackingContext: optional union[null, TrackingContext] = null
      }'''.stripIndent()

    projectFile('build/my-dummy-module/generateMetadataEventsSchema/com/linkedin/mxe/foo/testAspect/FailedMetadataChangeEvent.pdl').text == '''\
      namespace com.linkedin.mxe.foo.testAspect

      import com.linkedin.avro2pegasus.events.KafkaAuditHeader

      /**
       * FailedMetadataChangeEvent for the FooUrn with TestAspect aspect.
       */
      @FailedMetadataChangeEvent
      record FailedMetadataChangeEvent {

        /**
         * Kafka event for capturing a failure to process a MetadataChangeEvent.
         */
        auditHeader: optional KafkaAuditHeader

        /**
         * The event that failed to be processed.
         */
        metadataChangeEvent: MetadataChangeEvent

        /**
         * The error message or the stacktrace for the failure.
         */
        error: string
      }'''.stripIndent()

    projectFile('build/my-dummy-module/generateMetadataEventsSchema/com/linkedin/mxe/foo/testTyperefAspect/MetadataChangeEvent.pdl').text == '''\
      namespace com.linkedin.mxe.foo.testTyperefAspect

      import com.linkedin.avro2pegasus.events.KafkaAuditHeader
      import com.linkedin.metadata.events.ChangeType
      import com.linkedin.testing.FooUrn
      import com.linkedin.metadata.test.aspects.TestTyperefAspect

      /**
       * MetadataChangeEvent for the FooUrn with TestTyperefAspect aspect.
       */
      @MetadataChangeEvent
      record MetadataChangeEvent {

        /**
         * Kafka audit header. See go/kafkaauditheader for more info.
         */
        auditHeader: optional KafkaAuditHeader

        /**
         * FooUrn as the key for the MetadataChangeEvent.
         */
        urn: FooUrn

        /**
         * Value of the proposed TestTyperefAspect change.
         */
        proposedValue: optional TestTyperefAspect

        /**
         * Change type.
         */
        changeType: optional union[null, ChangeType] = null
      }'''.stripIndent()

    projectFile('build/my-dummy-module/generateMetadataEventsSchema/com/linkedin/mxe/foo/testTyperefAspect/MetadataAuditEvent.pdl').text == '''\
      namespace com.linkedin.mxe.foo.testTyperefAspect

      import com.linkedin.avro2pegasus.events.KafkaAuditHeader
      import com.linkedin.metadata.events.ChangeType
      import com.linkedin.metadata.events.TrackingContext
      import com.linkedin.testing.FooUrn
      import com.linkedin.metadata.test.aspects.TestTyperefAspect

      /**
       * MetadataAuditEvent for the FooUrn with TestTyperefAspect aspect.
       */
      @MetadataAuditEvent
      record MetadataAuditEvent {

        /**
         * Kafka audit header for the MetadataAuditEvent.
         */
        auditHeader: optional KafkaAuditHeader

        /**
         * FooUrn as the key for the MetadataAuditEvent.
         */
        urn: FooUrn

        /**
         * Aspect of the TestTyperefAspect before the update.
         */
        oldValue: optional TestTyperefAspect

        /**
         * Aspect of the TestTyperefAspect after the update.
         */
        newValue: TestTyperefAspect

        /**
         * Change type.
         */
        changeType: optional union[null, ChangeType] = null
        
        /**
         * Tracking context to identify the lifecycle of the trackable item.
         */
        trackingContext: optional union[null, TrackingContext] = null
      }'''.stripIndent()

    projectFile('build/my-dummy-module/generateMetadataEventsSchema/com/linkedin/mxe/foo/testTyperefAspect/FailedMetadataChangeEvent.pdl').text == '''\
      namespace com.linkedin.mxe.foo.testTyperefAspect

      import com.linkedin.avro2pegasus.events.KafkaAuditHeader

      /**
       * FailedMetadataChangeEvent for the FooUrn with TestTyperefAspect aspect.
       */
      @FailedMetadataChangeEvent
      record FailedMetadataChangeEvent {

        /**
         * Kafka event for capturing a failure to process a MetadataChangeEvent.
         */
        auditHeader: optional KafkaAuditHeader

        /**
         * The event that failed to be processed.
         */
        metadataChangeEvent: MetadataChangeEvent

        /**
         * The error message or the stacktrace for the failure.
         */
        error: string
      }'''.stripIndent()
  }

  def 'generate data template for events'() {
    when:
    GradleRunner runner = GradleRunner
        .create()
        .withProjectDir(tempDir.root)
        .withArguments(':my-dummy-module:generateDataTemplate', '-is')
        .withPluginClasspath()
        .withDebug(Boolean.parseBoolean(System.getProperty("debug-jvm")))
    BuildResult result = runner.build()

    then:
    // all expressions below are implicit assertions!
    result.task(':my-dummy-module:generateMetadataEventsSchema').outcome == TaskOutcome.SUCCESS
    result.task(':my-dummy-module:generateDataTemplate').outcome == TaskOutcome.SUCCESS
    result.task(':my-dummy-module:generateMetadataEventDataTemplate').outcome == TaskOutcome.SUCCESS

    Path testAspectDir = projectPath('my-dummy-module/src/mainGeneratedDataTemplate/java/com/linkedin/mxe/foo/testAspect/')
    testAspectDir.resolve('MetadataChangeEvent.java').toFile().exists()
    testAspectDir.resolve('MetadataAuditEvent.java').toFile().exists()
    testAspectDir.resolve('FailedMetadataChangeEvent.java').toFile().exists()

    Path testTyperefAspectDir = projectPath('my-dummy-module/src/mainGeneratedDataTemplate/java/com/linkedin/mxe/foo/testTyperefAspect/')
    testTyperefAspectDir.resolve('MetadataChangeEvent.java').toFile().exists()
    testTyperefAspectDir.resolve('MetadataAuditEvent.java').toFile().exists()
    testTyperefAspectDir.resolve('FailedMetadataChangeEvent.java').toFile().exists()
  }

  def 'data template jar has java class events'() {
    when:
    GradleRunner runner = GradleRunner
        .create()
        .withProjectDir(tempDir.root)
        .withArguments(':my-dummy-module:build', '-is')
        .withPluginClasspath()
        .withDebug(Boolean.parseBoolean(System.getProperty("debug-jvm")))
    BuildResult result = runner.build()

    then:
    result.task(":my-dummy-module:generateMetadataEventDataTemplate").outcome == TaskOutcome.SUCCESS
    result.task(":my-dummy-module:build").outcome == TaskOutcome.SUCCESS

    File jarFile = projectFile('my-dummy-module/build/libs/my-dummy-module-data-template.jar')
    JarFile jar = new JarFile(jarFile)
    jar.getEntry('com/linkedin/mxe/foo/testAspect/MetadataChangeEvent.class') != null
    jar.getEntry('com/linkedin/mxe/foo/testAspect/MetadataAuditEvent.class') != null
    jar.getEntry('com/linkedin/mxe/foo/testAspect/FailedMetadataChangeEvent.class') != null
    jar.getEntry('com/linkedin/metadata/test/aspects/TestAspect.class') != null

    jar.getEntry('com/linkedin/mxe/foo/testTyperefAspect/MetadataChangeEvent.class') != null
    jar.getEntry('com/linkedin/mxe/foo/testTyperefAspect/MetadataAuditEvent.class') != null
    jar.getEntry('com/linkedin/mxe/foo/testTyperefAspect/FailedMetadataChangeEvent.class') != null
    jar.getEntry('com/linkedin/metadata/test/aspects/TestTyperefAspect.class') != null
  }

  def 'data template jar has pdl events'() {
    when:
    GradleRunner runner = GradleRunner
        .create()
        .withProjectDir(tempDir.root)
        .withArguments(':my-dummy-module:build', '-is')
        .withPluginClasspath()
        .withDebug(Boolean.parseBoolean(System.getProperty("debug-jvm")))
    BuildResult result = runner.build()

    then:
    result.task(":my-dummy-module:build").outcome == TaskOutcome.SUCCESS
    File jarFile = projectFile('my-dummy-module/build/libs/my-dummy-module-data-template.jar')
    JarFile jar = new JarFile(jarFile)

    jar.getEntry('pegasus/com/linkedin/mxe/foo/testAspect/MetadataChangeEvent.pdl') != null
    jar.getEntry('pegasus/com/linkedin/mxe/foo/testAspect/MetadataAuditEvent.pdl') != null
    jar.getEntry('pegasus/com/linkedin/mxe/foo/testAspect/FailedMetadataChangeEvent.pdl') != null
    jar.getEntry('pegasus/com/linkedin/metadata/test/aspects/TestAspect.pdl') != null

    jar.getEntry('pegasus/com/linkedin/mxe/foo/testTyperefAspect/MetadataChangeEvent.pdl') != null
    jar.getEntry('pegasus/com/linkedin/mxe/foo/testTyperefAspect/MetadataAuditEvent.pdl') != null
    jar.getEntry('pegasus/com/linkedin/mxe/foo/testTyperefAspect/FailedMetadataChangeEvent.pdl') != null
    jar.getEntry('pegasus/com/linkedin/metadata/test/aspects/TestTyperefAspect.pdl') != null
  }
}
