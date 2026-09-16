package com.linkedin.metadata.dao.localrelationship;

import com.linkedin.metadata.dao.EBeanDAOConfig;
import com.linkedin.metadata.dao.EbeanLocalDAO;
import com.linkedin.metadata.dao.EbeanLocalRelationshipQueryDAO;
import com.linkedin.metadata.dao.utils.RelationshipLookUpContext;
import com.linkedin.metadata.dao.utils.EBeanDAOUtils;
import com.linkedin.metadata.dao.utils.SchemaValidatorUtil;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.LocalRelationshipCriterionArray;
import com.linkedin.metadata.query.LocalRelationshipFilter;
import com.linkedin.metadata.query.LocalRelationshipValue;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.UrnField;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;


/**
 * Index hint coverage for the legacy (non-keyset) builder, {@code buildFindRelationshipSQL}.
 *
 * <p>No database: the builder is called directly with a mocked {@link SchemaValidatorUtil}, so these run
 * locally as well as in CI. The index gate is stubbed present, which is the state these cases are about.</p>
 */
public class LegacyBuilderIndexHintTest {

  private static final String REL_TABLE = "metadata_relationship_downstreamof";
  private static final String ENTITY_TABLE = "metadata_entity_dataset";
  private static final String URN = "urn:li:dataset:(urn:li:dataPlatform:hive,foo.bar,PROD)";
  private static final String DEST_HINT = "FORCE INDEX (idx_destination_deleted_ts)";
  private static final String SOURCE_HINT = "FORCE INDEX (idx_source_deleted_ts)";

  private EbeanLocalRelationshipQueryDAO dao() {
    SchemaValidatorUtil validator = mock(SchemaValidatorUtil.class);
    when(validator.indexExists(anyString(), anyString())).thenReturn(true);
    when(validator.columnExists(anyString(), anyString())).thenReturn(true);
    EbeanLocalRelationshipQueryDAO dao =
        new EbeanLocalRelationshipQueryDAO(null, new EBeanDAOConfig(), validator);
    dao.setSchemaConfig(EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY);
    return dao;
  }

  /** An entity filter pins the urn through a field named "urn", because it is rendered against dt/st. */
  private LocalRelationshipFilter entityUrnFilter() {
    return new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(
        EBeanDAOUtils.buildRelationshipFieldCriterion(
            LocalRelationshipValue.create(URN), Condition.EQUAL, new UrnField())));
  }

  /** A relationship filter names the relationship column instead. */
  private LocalRelationshipFilter relationshipUrnFilter(String urnFieldName) {
    return new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(
        EBeanDAOUtils.buildRelationshipFieldCriterion(
            LocalRelationshipValue.create(URN), Condition.EQUAL, new UrnField().setName(urnFieldName))))
        .setDirection(RelationshipDirection.OUTGOING);
  }

  private LocalRelationshipFilter emptyFilter() {
    return new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray())
        .setDirection(RelationshipDirection.OUTGOING);
  }

  private String build(LocalRelationshipFilter relationshipFilter, String sourceTableName,
      LocalRelationshipFilter sourceEntityFilter, String destTableName,
      LocalRelationshipFilter destinationEntityFilter) {
    return dao().buildFindRelationshipSQL(REL_TABLE, relationshipFilter, sourceTableName, sourceEntityFilter,
        destTableName, destinationEntityFilter, -1, -1, new RelationshipLookUpContext());
  }

  /**
   * The hint must sit between the table alias and the first join. Asserting the position matters as much as
   * asserting presence: appended after a join it would be syntactically invalid.
   */
  private void assertHintPrecedesJoins(String sql, String hint) {
    assertTrue("expected hint in: " + sql, sql.contains(hint));
    int hintAt = sql.indexOf(hint);
    int joinAt = sql.indexOf("INNER JOIN");
    assertTrue("hint must follow the table alias", hintAt > sql.indexOf(REL_TABLE + " rt"));
    if (joinAt >= 0) {
      assertTrue("hint must precede the first join, got: " + sql, hintAt < joinAt);
    }
  }

  /**
   * Previously unhinted. The hint block sat in the third arm of the destination if/else chain, whose first
   * arm fires whenever a destination join is appended, so joining dt made it unreachable.
   */
  @Test
  public void testDestinationJoinIsHinted() {
    String sql = build(emptyFilter(), null, null, ENTITY_TABLE, entityUrnFilter());
    assertHintPrecedesJoins(sql, DEST_HINT);
  }

  /**
   * Previously unhinted. The source arm consulted only the relationship filter, but a caller joining an
   * entity table pins the source through st.urn.
   */
  @Test
  public void testSourceJoinIsHintedFromEntityFilter() {
    String sql = build(emptyFilter(), ENTITY_TABLE, entityUrnFilter(), null, null);
    assertHintPrecedesJoins(sql, SOURCE_HINT);
  }

  /** Destination wins when both sides are pinned, matching the keyset builder. */
  @Test
  public void testDestinationTakesPrecedenceOverSource() {
    String sql = build(emptyFilter(), ENTITY_TABLE, entityUrnFilter(), ENTITY_TABLE, entityUrnFilter());
    assertHintPrecedesJoins(sql, DEST_HINT);
    assertFalse(sql.contains(SOURCE_HINT));
  }

  /** Pre-existing behaviour, must not regress: destination pinned through the relationship filter. */
  @Test
  public void testRelationshipFilterDestinationStillHinted() {
    String sql = build(relationshipUrnFilter("destination"), null, null, null, null);
    assertTrue(sql.contains(DEST_HINT));
  }

  /** Pre-existing behaviour, must not regress: source pinned through the relationship filter. */
  @Test
  public void testRelationshipFilterSourceStillHinted() {
    String sql = build(relationshipUrnFilter("source"), null, null, null, null);
    assertTrue(sql.contains(SOURCE_HINT));
  }

  /** A non-urn entity filter pins nothing, so neither index is forced. */
  @Test
  public void testNoHintWhenNothingIsPinned() {
    String sql = build(emptyFilter(), ENTITY_TABLE, emptyFilter(), ENTITY_TABLE, emptyFilter());
    assertFalse(sql.contains(DEST_HINT));
    assertFalse(sql.contains(SOURCE_HINT));
  }

  /** The gate still governs: a missing index means no hint even when the urn is pinned. */
  @Test
  public void testNoHintWhenIndexIsAbsent() {
    SchemaValidatorUtil validator = mock(SchemaValidatorUtil.class);
    when(validator.indexExists(anyString(), anyString())).thenReturn(false);
    when(validator.columnExists(anyString(), anyString())).thenReturn(true);
    EbeanLocalRelationshipQueryDAO dao =
        new EbeanLocalRelationshipQueryDAO(null, new EBeanDAOConfig(), validator);
    dao.setSchemaConfig(EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY);

    String sql = dao.buildFindRelationshipSQL(REL_TABLE, emptyFilter(), ENTITY_TABLE, entityUrnFilter(),
        null, null, -1, -1, new RelationshipLookUpContext());

    assertFalse(sql.contains(SOURCE_HINT));
    assertFalse(sql.contains(DEST_HINT));
  }
}
