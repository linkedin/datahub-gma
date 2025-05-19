package com.linkedin.metadata.dao.utils;

import com.google.common.io.Resources;
import com.linkedin.metadata.dao.EBeanDAOConfig;
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

  private static EbeanServer server;
  private final EBeanDAOConfig ebeanConfig = new EBeanDAOConfig();
  private SchemaValidatorUtil validator;

  @Factory(dataProvider = "inputList")
  public SchemaValidatorUtilTest(boolean nonDollarVirtualColumnsEnabled) {
    ebeanConfig.setNonDollarVirtualColumnsEnabled(nonDollarVirtualColumnsEnabled);
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
    server = EmbeddedMariaInstance.getServer(SchemaValidatorUtilTest.class.getSimpleName());
  }

  @BeforeMethod
  public void setupTest() throws IOException {
    String sqlFile = !ebeanConfig.isNonDollarVirtualColumnsEnabled()
        ? "ebean-local-access-create-all.sql"
        : "ebean-local-access-create-all-with-non-dollar-virtual-column-names.sql";

    server.execute(Ebean.createSqlUpdate(
        Resources.toString(Resources.getResource(sqlFile), StandardCharsets.UTF_8)));

    validator = new SchemaValidatorUtil(server);
  }

  @Test
  public void testCheckColumnExists() {
    assertTrue(validator.columnExists("metadata_entity_foo", "a_aspectfoo"));
    assertFalse(validator.columnExists("metadata_entity_foo", "a_aspect_not_exist"));
    assertFalse(validator.columnExists("metadata_entity_notexist", "a_aspectfoo"));

    if (!ebeanConfig.isNonDollarVirtualColumnsEnabled()) {
      assertTrue(validator.columnExists("metadata_entity_foo", "i_aspectfoo$value"));
    } else {
      assertTrue(validator.columnExists("metadata_entity_foo", "i_aspectfoo0value"));
    }
  }

  @Test
  public void testCheckIndexExists() {
    assertFalse(validator.indexExists("metadata_entity_foo", "i_aspect_not_exist"));

    if (!ebeanConfig.isNonDollarVirtualColumnsEnabled()) {
      assertTrue(validator.indexExists("metadata_entity_foo", "i_aspectfoo$value"));
    } else {
      assertTrue(validator.indexExists("metadata_entity_foo", "i_aspectfoo0value"));
    }
  }

}
