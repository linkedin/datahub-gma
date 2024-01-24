package com.linkedin.metadata.restli.dao;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.urn.FooUrn;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.utils.ModelUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class DefaultLocalDaoRegistryImplTest {

  private BaseLocalDAO<EntityAspectUnion, FooUrn> _baseLocalDAO;

  @BeforeMethod
  void setup() {
    _baseLocalDAO = mock(BaseLocalDAO.class);
  }

  @Test
  public void initTestSuccess() {
    try {
      when(_baseLocalDAO.getUrnClass()).thenReturn(FooUrn.class);
      Map<String, BaseLocalDAO<? extends UnionTemplate, ? extends Urn>> map = ImmutableMap.of(
          getEntityTypeFromUrnClass(FooUrn.class), _baseLocalDAO
      );
      DefaultLocalDaoRegistryImpl.init(map);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void initTestException() {
    final Map<String, BaseLocalDAO<? extends UnionTemplate, ? extends Urn>> fooMap = ImmutableMap.of(
        getEntityTypeFromUrnClass(FooUrn.class), _baseLocalDAO
    );
    assertThrows(IllegalStateException.class, () -> DefaultLocalDaoRegistryImpl.init(fooMap));

    when(_baseLocalDAO.getUrnClass()).thenReturn(FooUrn.class);
    final Map<String, BaseLocalDAO<? extends UnionTemplate, ? extends Urn>> barMap = ImmutableMap.of("bar", _baseLocalDAO);
    assertThrows(IllegalArgumentException.class, () -> DefaultLocalDaoRegistryImpl.init(barMap));
  }
}
