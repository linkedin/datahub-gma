package com.linkedin.testing;

import com.linkedin.avro2pegasus.events.UUID;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.BazUrn;
import com.linkedin.testing.urn.FooUrn;
import com.linkedin.testing.urn.BurgerUrn;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;


public class TestUtils {

  private TestUtils() {
    // util class
  }

  @Nonnull
  public static FooUrn makeFooUrn(int id) {
    try {
      return new FooUrn(id);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  public static FooUrn makeFooUrn(String urn) {
    try {
      return FooUrn.createFromString(urn);
    } catch (URISyntaxException e) {
      throw new RuntimeException(String.format("Invalid urn %s", urn));
    }
  }

  @Nonnull
  public static BurgerUrn makeBurgerUrn(String urn) {
    try {
      return BurgerUrn.createFromString(urn);
    } catch (URISyntaxException e) {
      throw new RuntimeException(String.format("Invalid urn %s", urn));
    }
  }

  @Nonnull
  public static BarUrn makeBarUrn(int id) {
    return new BarUrn(id);
  }

  @Nonnull
  public static BarUrn makeBarUrn(String urn) {
    try {
      return BarUrn.createFromString(urn);
    } catch (URISyntaxException e) {
      throw new RuntimeException(String.format("Invalid urn %s", urn));
    }
  }

  @Nonnull
  public static BazUrn makeBazUrn(int id) {
    try {
      return new BazUrn(id);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  public static Urn makeUrn(@Nonnull Object id) {
    try {
      return new Urn("urn:li:testing:" + id);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  public static Urn makeUrn(@Nonnull Object id, @Nonnull String entityType) {
    try {
      return new Urn("urn:li:" + entityType + ":" + id);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  public static EntityKey makeKey(long id) {
    return new EntityKey().setId(id);
  }

  @Nonnull
  public static ComplexResourceKey<EntityKey, EmptyRecord> makeResourceKey(@Nonnull Urn urn) {
    return new ComplexResourceKey<>(makeKey(urn.getIdAsLong()), new EmptyRecord());
  }

  @Nonnull
  public static EntityDocument makeDocument(@Nonnull Urn urn) {
    return new EntityDocument().setUrn(urn);
  }

  @Nonnull
  public static IngestionTrackingContext makeIngestionTrackingContext(byte[] uuid) {
    IngestionTrackingContext ingestionTrackingContext = new IngestionTrackingContext();
    ingestionTrackingContext.setTrackingId(new UUID(ByteString.copy(uuid)));
    return ingestionTrackingContext;
  }

  /**
   * Returns all test entity classes.
   */
  @Nonnull
  public static Set<Class<? extends RecordTemplate>> getAllTestEntities() {
    return Collections.unmodifiableSet(new HashSet<Class<? extends RecordTemplate>>() {
      {
        add(EntityBar.class);
        add(EntityBaz.class);
        add(EntityFoo.class);
      }
    });
  }
}
