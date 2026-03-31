package com.linkedin.metadata.dao.utils;

import com.google.common.io.Resources;
import com.linkedin.metadata.dao.EBeanDAOConfig;
import io.ebean.Ebean;
import io.ebean.EbeanServer;
import io.ebean.SqlQuery;
import io.ebean.SqlRow;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import static com.linkedin.common.AuditStamps.*;
import static com.linkedin.testing.TestUtils.*;
import static org.mockito.Mockito.*;
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
    // need to mock this since we will be stubbing in for the EXPRESSION column retrieval for that test since
    // MariaDB doesn't support functional indexes
    server = spy(EmbeddedMariaInstance.getServer(SchemaValidatorUtilTest.class.getSimpleName()));
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

  // These are all real examples of expressions used to create functional indexes
  // https://docs.google.com/document/d/1OSfx9DAXuPLlOaHWkn2o_WUlzNp1xZ1R_D63TNPZIG4/edit?tab=t.0#bookmark=id.y4f15dapxdh8
  @Test
  @SuppressWarnings("checkstyle:LineLength")
  public void testCleanIndexExpression() {
    assertEquals("(cast(json_unquote(json_extract(`a_azkabanjobinfo`,_utf8mb4'$.aspect.project.clusterInfo.hadoopCluster')) as char(255) charset utf8mb4))",
        SchemaValidatorUtil.cleanIndexExpression("cast(json_unquote(json_extract(`a_azkabanjobinfo`,_utf8mb4\\'$.aspect.project.clusterInfo.hadoopCluster\\')) as char(255) charset utf8mb4)"));

    assertEquals("(cast(json_unquote(json_extract(`a_urn`,_utf8mb4'$.\"\\\\\\\\/azkabanFlowUrn\"')) as char(255) charset utf8mb4))",
        SchemaValidatorUtil.cleanIndexExpression("cast(json_unquote(json_extract(`a_urn`,_utf8mb4\\'$.\"\\\\\\\\/azkabanFlowUrn\"\\')) as char(255) charset utf8mb4)"));

    assertEquals("(cast(replace(json_unquote(json_extract(`a_datapolicyinfo`,_utf8mb3'$.aspect.annotation.ontologyIris[*]')),_utf8mb4'\"',_utf8mb3'') as char(255) charset utf8mb4))",
        SchemaValidatorUtil.cleanIndexExpression("cast(replace(json_unquote(json_extract(`a_datapolicyinfo`,_utf8mb3\\'$.aspect.annotation.ontologyIris[*]\\')),_utf8mb4\\'\"\\',_utf8mb3\\'\\') as char(255) charset utf8mb4)"));

    assertEquals("(cast(json_unquote(json_extract(`a_urn`,_utf8mb3'$.\"\\\\\\\\/dataset\\\\\\\\/platform\\\\\\\\/platformName\"')) as char(255) charset utf8mb4))",
        SchemaValidatorUtil.cleanIndexExpression("cast(json_unquote(json_extract(`a_urn`,_utf8mb3\\'$.\"\\\\\\\\/dataset\\\\\\\\/platform\\\\\\\\/platformName\"\\')) as char(255) charset utf8mb4)"));

    // crazy AIM use case lol
    assertEquals("(cast(concat(json_unquote(json_extract(`a_model_instance_info`,_utf8mb3'$.aspect.multi_product_version.major')),_utf8mb4'.',json_unquote(json_extract(`a_model_instance_info`,_utf8mb3'$.aspect.multi_product_version.minor')),_utf8mb4'.',json_unquote(json_extract(`a_model_instance_info`,_utf8mb3'$.aspect.multi_product_version.patch'))) as char(255) charset utf8mb4))",
        SchemaValidatorUtil.cleanIndexExpression("cast(concat(json_unquote(json_extract(`a_model_instance_info`,_utf8mb3\\'$.aspect.multi_product_version.major\\')),_utf8mb4\\'.\\',json_unquote(json_extract(`a_model_instance_info`,_utf8mb3\\'$.aspect.multi_product_version.minor\\')),_utf8mb4\\'.\\',json_unquote(json_extract(`a_model_instance_info`,_utf8mb3\\'$.aspect.multi_product_version.patch\\'))) as char(255) charset utf8mb4)"));

    assertNull(SchemaValidatorUtil.cleanIndexExpression(null));
  }

  /**
   * These tests require mocking because MariaDB, our embedded test database, does not support functional indexes, which
   * the code under test is trying to access.
   */
  @Test
  public void testGetIndexExpression() {
    // NEED to set up all mocks for DB access BEFORE running ANY tests because it will be cached
    SqlQuery sqlQuery = mock(SqlQuery.class);
    List<SqlRow> indexTable = new ArrayList<>();

    when(sqlQuery.findList()).thenReturn(indexTable);
    when(server.createSqlQuery(anyString())).thenReturn(sqlQuery);

    // setup mock for the LEGACY index use case: no expression-based index, but the index still exists!
    SqlRow row1 = mock(SqlRow.class);
    indexTable.add(row1);
    when(row1.getString("EXPRESSION")).thenReturn(null);

    // setup mock for the EXPRESSION index use case
    SqlRow row2 = mock(SqlRow.class);
    indexTable.add(row2);
    when(row2.getString("INDEX_NAME")).thenReturn("e_aspectfoo0value");
    when(row2.getString("EXPRESSION")).thenReturn(
        "cast(json_extract(`a_aspectfoo`, '$.aspect.value') as char(1024) charset utf8mb4)");

    if (!ebeanConfig.isNonDollarVirtualColumnsEnabled()) {
      when(row1.getString("INDEX_NAME")).thenReturn("i_aspectfoo$value");
    } else {
      when(row1.getString("INDEX_NAME")).thenReturn("i_aspectfoo0value");
    }


    // NONEXISTENT test
    assertNull(validator.getIndexExpression("metadata_entity_burger", "idx_fake"));

    /// Verify!
    assertNotNull(validator.getIndexExpression("metadata_entity_burger", "e_aspectfoo0value"));
    assertEquals("(cast(json_extract(`a_aspectfoo`, '$.aspect.value') as char(1024) charset utf8mb4))",
        validator.getIndexExpression("metadata_entity_burger", "e_aspectfoo0value"));

    if (!ebeanConfig.isNonDollarVirtualColumnsEnabled()) {
      // Make sure that retrieving a "legacy" column-based index still returns true but returns null
      assertTrue(validator.indexExists("metadata_entity_foo", "i_aspectfoo$value"));
      assertNull(validator.getIndexExpression("metadata_entity_foo", "i_aspectfoo$value"));
    } else {
      // Make sure that retrieving a "legacy" column-based index still returns true but returns null
      assertTrue(validator.indexExists("metadata_entity_foo", "i_aspectfoo0value"));
      assertNull(validator.getIndexExpression("metadata_entity_foo", "i_aspectfoo0value"));
    }
  }

}
