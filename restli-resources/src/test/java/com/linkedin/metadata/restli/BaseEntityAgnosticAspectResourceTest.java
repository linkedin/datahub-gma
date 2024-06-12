package com.linkedin.metadata.restli;

import com.linkedin.common.AuditStamp;
import com.linkedin.metadata.backfill.BackfillMode;
import com.linkedin.metadata.dao.GenericLocalDAO;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.restli.server.ResourceContext;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.urn.FooUrn;
import java.net.URISyntaxException;
import java.time.Clock;
import java.util.Collections;
import java.util.Optional;

import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

public class BaseEntityAgnosticAspectResourceTest extends BaseEngineTest {
  private GenericLocalDAO _mockLocalDAO;

  private final BaseEntityAgnosticAspectResourceTest.TestResource _resource =
      new BaseEntityAgnosticAspectResourceTest.TestResource();

  private static final FooUrn ENTITY_URN;

  static {
    try {
      ENTITY_URN = FooUrn.createFromString("urn:li:foo:1");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  class TestResource extends BaseEntityAgnosticAspectResource {

    @Nonnull
    @Override
    protected GenericLocalDAO genericLocalDAO() {
      return _mockLocalDAO;
    }

    @Nonnull
    protected BaseRestliAuditor getAuditor() {
      return new DummyRestliAuditor(Clock.systemUTC());
    }

    public ResourceContext getContext() {
      return mock(ResourceContext.class);
    }
  }

  @BeforeMethod
  public void setup() {
    _mockLocalDAO = mock(GenericLocalDAO.class);
  }

  @Test
  public void testQuery() {
    AspectFoo foo = new AspectFoo().setValue("foo");
    when(_mockLocalDAO.queryLatest(ENTITY_URN, AspectFoo.class)).thenReturn(Optional.of(
        new GenericLocalDAO.MetadataWithExtraInfo(foo.toString(), null)));

    String result = runAndWait(_resource.queryLatest(ENTITY_URN.toString(), AspectFoo.class.getCanonicalName()));
    assertEquals(result, foo.toString());
  }

  @Test
  public void testIngest() {
    AspectFoo foo = new AspectFoo().setValue("foo");
    runAndWait(_resource.ingest(ENTITY_URN.toString(), foo.toString(), AspectFoo.class.getCanonicalName(), null, null));
    verify(_mockLocalDAO, times(1)).save(eq(ENTITY_URN),
        eq(AspectFoo.class), eq(foo.toString()), any(AuditStamp.class), any(), any());
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testBackfill() {
    runAndWait(_resource.backfill(ENTITY_URN.toString(), new String[] {AspectFoo.class.getCanonicalName()}));

    verify(_mockLocalDAO, times(1)).backfill(eq(BackfillMode.BACKFILL_ALL),
        eq(Collections.singletonMap(ENTITY_URN, Collections.singleton(AspectFoo.class))));

    verifyNoMoreInteractions(_mockLocalDAO);
  }
}
