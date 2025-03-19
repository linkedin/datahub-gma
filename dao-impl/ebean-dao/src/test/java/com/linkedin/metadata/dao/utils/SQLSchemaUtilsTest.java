package com.linkedin.metadata.dao.utils;

import com.linkedin.metadata.dao.GlobalAssetRegistry;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.BarAsset;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.FooUrn;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.utils.SQLSchemaUtils.*;
import static org.testng.Assert.*;


public class SQLSchemaUtilsTest {

  @DataProvider
  public static Object[][] sqlStatement() {

    // each row has three element - ["sql script", "number of high risk sql", "table used in high risk sql"]
    return new Object[][]{

        // Create new table is low-risk.
        {"CREATE TABLE IF NOT EXISTS metadata_entity_espressodatabase ("
            + "    urn VARCHAR(500) NOT NULL,\n"
            + "    lastmodifiedon TIMESTAMP NOT NULL,\n"
            + "    lastmodifiedby VARCHAR(255) NOT NULL,\n"
            + "    createdfor VARCHAR(255),\n"
            + "    CONSTRAINT pk_metadata_entity_espressodatabase PRIMARY KEY (urn)\n"
            + ");", 0, Collections.emptySet()},

        // Add new column is low-risk.
        {"ALTER TABLE metadata_entity_dataset ADD COLUMN a_datasetreferentialauditinfo JSON;", 0, Collections.emptySet()},

        // Add index is high-risk.
        {"ALTER TABLE metadata_relationship_downstreamof\n"
            + "ADD INDEX idx_deleted_ts (deleted_ts),\n"
            + "ADD INDEX idx_source_deleted_ts (source, deleted_ts);", 1,
            Collections.singleton("metadata_relationship_downstreamof")},

        // Create index is high-risk.
        {"CREATE INDEX idx_source_deleted_ts ON metadata_relationship_ownedby (source, deleted_ts);", 1,
            Collections.singleton("metadata_relationship_ownedby")},

        // Mix low-risk and high-risk in one script. Add new virtual column is low-risk but adding index is high risk.
        {"ALTER TABLE metadata_entity_datapolicy ADD i_datapolicyinfo$purposeUrn VARCHAR(128)"
            + " GENERATED ALWAYS AS (REPLACE(JSON_EXTRACT(a_datapolicyinfo, '$.aspect.purposeUrn'), '\"', ''));\n"
            + "CREATE INDEX i_datapolicyinfo$purposeUrn ON metadata_entity_datapolicy (urn, i_datapolicyinfo$purposeUrn);", 1,
            Collections.singleton("metadata_entity_datapolicy")},

        // Both creating new table and adding new columns is low risk.
        {"CREATE TABLE IF NOT EXISTS metadata_entity_dataset (\n"
            + "    urn VARCHAR(500) NOT NULL,\n"
            + "    lastmodifiedon TIMESTAMP NOT NULL,\n"
            + "    lastmodifiedby VARCHAR(255) NOT NULL,\n"
            + "    createdfor VARCHAR(255),\n"
            + "    CONSTRAINT pk_metadata_entity_dataset PRIMARY KEY (urn)\n"
            + ");\n"
            + "ALTER TABLE metadata_entity_dataset ADD COLUMN a_clusterdatasetsla JSON;\n"
            + "ALTER TABLE metadata_entity_dataset ADD COLUMN a_cmonjiratickets JSON;\n"
            + "ALTER TABLE metadata_entity_dataset ADD COLUMN a_complianceannotation JSON;\n"
            + "ALTER TABLE metadata_entity_dataset ADD COLUMN a_complianceinfo JSON;", 0, Collections.emptySet()},
    };
  }

  @Test
  public void testGetGeneratedColumnName() {
    String generatedColumnName =
        getGeneratedColumnName(FooUrn.ENTITY_TYPE, AspectFoo.class.getCanonicalName(), "/value", false);
    assertEquals(generatedColumnName, "i_aspectfoo$value");

    generatedColumnName =
        getGeneratedColumnName(FooUrn.ENTITY_TYPE, AspectFoo.class.getCanonicalName(), "/value", true);
    assertEquals(generatedColumnName, "i_aspectfoo0value");
  }

  @Test
  public void testGetAspectColumnName() {
    GlobalAssetRegistry.register(BarUrn.ENTITY_TYPE, BarAsset.class);
    assertEquals("a_aspect_bar",
        SQLSchemaUtils.getAspectColumnName(BarUrn.ENTITY_TYPE, "com.linkedin.testing.AspectBar"));
  }

  @Test(dataProvider = "sqlStatement")
  public void testDetectHighRiskSQL(String sql, int numOfHighRiskSQL, Set<String> tables) throws Exception {
    List<String> highRiskSQL = SQLSchemaUtils.detectPotentialHighRiskSQL(sql);
    assertEquals(highRiskSQL.size(), numOfHighRiskSQL);

    Set<String> highRiskSet = new HashSet<>();
    for (String statement : highRiskSQL) {
      highRiskSet.add(extractTableName(statement));
    }

    assertEquals(highRiskSet, tables);
  }
}