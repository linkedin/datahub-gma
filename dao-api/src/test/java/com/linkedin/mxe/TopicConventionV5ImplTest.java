package com.linkedin.mxe;

import com.linkedin.pegasus2avro.mxe.pizza.pizzaInfo.FailedMetadataChangeEvent;
import com.linkedin.pegasus2avro.mxe.pizza.pizzaInfo.MetadataAuditEvent;
import com.linkedin.pegasus2avro.mxe.pizza.pizzaInfo.MetadataChangeEvent;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.Pizza;
import com.linkedin.testing.PizzaAspect;
import com.linkedin.testing.PizzaInfo;
import com.linkedin.testing.urn.FooUrn;
import com.linkedin.testing.urn.PizzaUrn;
import java.net.URISyntaxException;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TopicConventionV5ImplTest {
  @Test
  public void metadataChangeEventName() {
    assertEquals("MCE_Pizza_PizzaInfo",
        new TopicConventionV5Impl().getMetadataChangeEventTopicName(new PizzaUrn(0), new PizzaInfo()));
  }

  @Test
  public void metadataChangeEventNameCustomPattern() {
    assertEquals("Pizza-PizzaInfo.MCE",
        new TopicConventionV5Impl("%ENTITY%-%ASPECT%.%EVENT%").getMetadataChangeEventTopicName(new PizzaUrn(0),
            new PizzaInfo()));
  }

  @Test
  public void metadataChangeEventNameWithOverride() {
    final TopicConventionV5 topicConvention =
        new TopicConventionV5Impl().withOverride(MetadataChangeEvent.class, "customPizzaInfo")
            .withOverride("MCE_Pizza_AspectBar", "customBar");

    assertEquals("customPizzaInfo", topicConvention.getMetadataChangeEventTopicName(new PizzaUrn(0), new PizzaInfo()));
    assertEquals("customBar", topicConvention.getMetadataChangeEventTopicName(new PizzaUrn(0), new AspectBar()));
    assertEquals("MCE_Pizza_AspectFoo",
        topicConvention.getMetadataChangeEventTopicName(new PizzaUrn(0), new AspectFoo()));
  }

  @Test
  public void metadataChangeEventSchema() throws Exception {
    final Class<?> schema = new TopicConventionV5Impl().getMetadataChangeEventSchema(new PizzaUrn(0), new PizzaInfo());

    assertEquals(schema, MetadataChangeEvent.class);
  }

  @Test
  public void missingMetadataChangeEventSchema() {
    try {
      new TopicConventionV5Impl().getMetadataChangeEventSchema(new PizzaUrn(0), new AspectFoo());
      fail("Expected ClassNotFoundException");
    } catch (ClassNotFoundException e) {
      assertEquals(e.getMessage(), "com.linkedin.pegasus2avro.mxe.pizza.aspectFoo.MetadataChangeEvent");
    }
  }

  @Test
  public void metadataAuditEventName() {
    assertEquals("MAE_Pizza_PizzaInfo",
        new TopicConventionV5Impl().getMetadataAuditEventTopicName(new PizzaUrn(0), new PizzaInfo()));
  }

  @Test
  public void metadataAuditEventNameWithOverride() {
    final TopicConventionV5 topicConvention =
        new TopicConventionV5Impl().withOverride(MetadataAuditEvent.class, "customPizzaInfo")
            .withOverride("MAE_Pizza_AspectBar", "customBar");

    assertEquals("customPizzaInfo", topicConvention.getMetadataAuditEventTopicName(new PizzaUrn(0), new PizzaInfo()));
    assertEquals("customBar", topicConvention.getMetadataAuditEventTopicName(new PizzaUrn(0), new AspectBar()));
    assertEquals("MAE_Pizza_AspectFoo",
        topicConvention.getMetadataAuditEventTopicName(new PizzaUrn(0), new AspectFoo()));
  }

  @Test
  public void metadataAuditEventNameCustomPattern() {
    assertEquals("Pizza-PizzaInfo.MAE",
        new TopicConventionV5Impl("%ENTITY%-%ASPECT%.%EVENT%").getMetadataAuditEventTopicName(new PizzaUrn(0),
            new PizzaInfo()));
  }

  @Test
  public void metadataAuditEventSchema() throws Exception {
    final Class<?> schema = new TopicConventionV5Impl().getMetadataAuditEventSchema(new PizzaUrn(0), new PizzaInfo());

    assertEquals(schema, MetadataAuditEvent.class);
  }

  @Test
  public void missingMetadataAuditEventSchema() {
    try {
      new TopicConventionV5Impl().getMetadataAuditEventSchema(new PizzaUrn(0), new AspectFoo());
      fail("Expected ClassNotFoundException");
    } catch (ClassNotFoundException e) {
      assertEquals(e.getMessage(), "com.linkedin.pegasus2avro.mxe.pizza.aspectFoo.MetadataAuditEvent");
    }
  }

  @Test
  public void failedMetadataChangeEventName() {
    assertEquals("FMCE_Pizza_PizzaInfo",
        new TopicConventionV5Impl().getFailedMetadataChangeEventTopicName(new PizzaUrn(0), new PizzaInfo()));
  }

  @Test
  public void failedMetadataChangeEventNameWithOverride() {
    final TopicConventionV5 topicConvention =
        new TopicConventionV5Impl().withOverride(FailedMetadataChangeEvent.class, "customPizzaInfo")
            .withOverride("FMCE_Pizza_AspectBar", "customBar");

    assertEquals("customPizzaInfo",
        topicConvention.getFailedMetadataChangeEventTopicName(new PizzaUrn(0), new PizzaInfo()));
    assertEquals("customBar", topicConvention.getFailedMetadataChangeEventTopicName(new PizzaUrn(0), new AspectBar()));
    assertEquals("FMCE_Pizza_AspectFoo",
        topicConvention.getFailedMetadataChangeEventTopicName(new PizzaUrn(0), new AspectFoo()));
  }

  @Test
  public void failedMetadataChangeEventNameCustomPattern() {
    assertEquals("Pizza-PizzaInfo.FMCE",
        new TopicConventionV5Impl("%ENTITY%-%ASPECT%.%EVENT%").getFailedMetadataChangeEventTopicName(new PizzaUrn(0),
            new PizzaInfo()));
  }

  @Test
  public void failedMetadataChangeEventSchema() throws Exception {
    final Class<?> schema =
        new TopicConventionV5Impl().getFailedMetadataChangeEventSchema(new PizzaUrn(0), new PizzaInfo());

    assertEquals(schema, FailedMetadataChangeEvent.class);
  }

  @Test
  public void missingFailedMetadataChangeEventSchema() {
    try {
      new TopicConventionV5Impl().getFailedMetadataChangeEventSchema(new PizzaUrn(0), new AspectFoo());
      fail("Expected ClassNotFoundException");
    } catch (ClassNotFoundException e) {
      assertEquals(e.getMessage(), "com.linkedin.pegasus2avro.mxe.pizza.aspectFoo.FailedMetadataChangeEvent");
    }
  }
}