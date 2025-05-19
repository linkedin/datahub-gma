package com.linkedin.metadata.dao.utils;

import com.google.common.io.Resources;
import com.linkedin.common.AuditStamp;
import com.linkedin.metadata.dao.EBeanDAOConfig;
import com.linkedin.metadata.dao.EbeanLocalAccess;
import com.linkedin.metadata.dao.EbeanLocalAccessTest;
import com.linkedin.metadata.dao.IEbeanLocalAccess;
import com.linkedin.metadata.dao.urnpath.EmptyPathExtractor;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.urn.BurgerUrn;
import com.linkedin.testing.urn.FooUrn;
import io.ebean.Ebean;
import io.ebean.EbeanServer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import static com.linkedin.common.AuditStamps.*;
import static com.linkedin.testing.TestUtils.*;
import static org.testng.AssertJUnit.*;


public class SchemaValidatorUtilTest {

  private static EbeanServer _server;
  private static long _now;
  private final EBeanDAOConfig _ebeanConfig = new EBeanDAOConfig();
  private SchemaValidatorUtil _validator;

  @Factory(dataProvider = "inputList")
  public SchemaValidatorUtilTest(boolean nonDollarVirtualColumnsEnabled) {
    _ebeanConfig.setNonDollarVirtualColumnsEnabled(nonDollarVirtualColumnsEnabled);
  }

  @DataProvider(name = "inputList")
  public static Object[][] inputList() {
    return new Object[][] {
        { true },
        { false }
    };
  }

  @BeforeClass
  public void init() {
    _server = EmbeddedMariaInstance.getServer(EbeanLocalAccessTest.class.getSimpleName());
    _now = System.currentTimeMillis();
  }

  @BeforeMethod
  public void setupTest() throws IOException {
    if (!_ebeanConfig.isNonDollarVirtualColumnsEnabled()) {
      _server.execute(Ebean.createSqlUpdate(
          Resources.toString(Resources.getResource("ebean-local-access-create-all.sql"), StandardCharsets.UTF_8)));
    } else {
      _server.execute(Ebean.createSqlUpdate(Resources.toString(
          Resources.getResource("ebean-local-access-create-all-with-non-dollar-virtual-column-names.sql"),
          StandardCharsets.UTF_8)));
    }
    _validator = new SchemaValidatorUtil(_server); // Instantiate validator
  }

  @Test
  public void testCheckColumnExists() {
    assertTrue(_validator.columnExists("metadata_entity_foo", "a_aspectfoo"));
    assertFalse(_validator.columnExists("metadata_entity_foo", "a_aspect_not_exist"));
    assertFalse(_validator.columnExists("metadata_entity_notexist", "a_aspectfoo"));

    if (!_ebeanConfig.isNonDollarVirtualColumnsEnabled()) {
      assertTrue(_validator.columnExists("metadata_entity_foo", "i_aspectfoo$value"));
    } else {
      assertTrue(_validator.columnExists("metadata_entity_foo", "i_aspectfoo0value"));
    }
  }

  @Test
  public void testIndexExists() {
    // Change index name to match what exists in your schema
    assertTrue(_validator.indexExists("metadata_entity_foo", "idx_aspect_bar"));
    assertFalse(_validator.indexExists("metadata_entity_foo", "non_existent_index"));
  }

}
