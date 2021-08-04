package com.linkedin.metadata.dao.internal;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.BaseQueryDAO;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.testing.EntityBar;
import com.linkedin.testing.EntityFoo;
import com.linkedin.testing.RelationshipFoo;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.FooUrn;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.linkedin.metadata.dao.internal.BaseGraphWriterDAO.RemovalOption.*;
import static com.linkedin.testing.TestUtils.*;
import static org.testng.Assert.*;


abstract public class BaseGraphWriterDAOTestBase<W extends BaseGraphWriterDAO, Q extends BaseQueryDAO> {

  private W _dao;
  private Q _reader;

  abstract @Nonnull W newBaseGraphWriterDao();
  abstract @Nonnull Q newBaseQueryDao();

  @Nonnull
  abstract public Optional<Map<String, Object>> getNode(@Nonnull Urn urn);

  @Nonnull
  abstract public List<Map<String, Object>> getAllNodes(@Nonnull Urn urn);

  @Nonnull
  abstract public List<Map<String, Object>> getEdges(@Nonnull RecordTemplate relationship);

  @Nonnull
  abstract public List<Map<String, Object>> getEdgesFromSource(
          @Nonnull Urn sourceUrn,
          @Nonnull Class<? extends RecordTemplate> relationshipClass
  );

  @BeforeMethod
  public void init() {
    _dao = newBaseGraphWriterDao();
    _reader = newBaseQueryDao();
  }

  @Test
  public void testAddEntity() throws Exception {
    FooUrn urn = makeFooUrn(1);
    EntityFoo entity = new EntityFoo().setUrn(urn).setValue("foo");

    _dao.addEntity(entity);
    Optional<Map<String, Object>> node = getNode(urn);
    assertEntityFoo(node.get(), entity);
  }

  @Test
  public void testRemoveEntity() throws Exception {
    FooUrn urn = makeFooUrn(1);
    EntityFoo entity = new EntityFoo().setUrn(urn).setValue("foo");

    // addEntity tested in testAddEntity
    _dao.addEntity(entity);

    _dao.removeEntity(urn);
    Optional<Map<String, Object>> node = getNode(urn);
    assertFalse(node.isPresent());
  }

  @Test
  public void testPartialUpdateEntity() throws Exception {
    FooUrn urn = makeFooUrn(1);
    EntityFoo entity = new EntityFoo().setUrn(urn);

    _dao.addEntity(entity);
    Optional<Map<String, Object>> node = getNode(urn);
    assertEntityFoo(node.get(), entity);

    // add value for optional field
    EntityFoo entity2 = new EntityFoo().setUrn(urn).setValue("IamTheSameEntity");
    _dao.addEntity(entity2);
    node = getNode(urn);
    assertEquals(getAllNodes(urn).size(), 1);
    assertEntityFoo(node.get(), entity2);

    // change value for optional field
    EntityFoo entity3 = new EntityFoo().setUrn(urn).setValue("ChangeValue");
    _dao.addEntity(entity3);
    node = getNode(urn);
    assertEquals(getAllNodes(urn).size(), 1);
    assertEntityFoo(node.get(), entity3);
  }

  @Test
  public void testAddEntities() throws Exception {
    EntityFoo entity1 = new EntityFoo().setUrn(makeFooUrn(1)).setValue("foo");
    EntityFoo entity2 = new EntityFoo().setUrn(makeFooUrn(2)).setValue("bar");
    EntityFoo entity3 = new EntityFoo().setUrn(makeFooUrn(3)).setValue("baz");
    List<EntityFoo> entities = Arrays.asList(entity1, entity2, entity3);

    _dao.addEntities(entities);
    assertEntityFoo(getNode(entity1.getUrn()).get(), entity1);
    assertEntityFoo(getNode(entity2.getUrn()).get(), entity2);
    assertEntityFoo(getNode(entity3.getUrn()).get(), entity3);
  }

  @Test
  public void testRemoveEntities() throws Exception {
    EntityFoo entity1 = new EntityFoo().setUrn(makeFooUrn(1)).setValue("foo");
    EntityFoo entity2 = new EntityFoo().setUrn(makeFooUrn(2)).setValue("bar");
    EntityFoo entity3 = new EntityFoo().setUrn(makeFooUrn(3)).setValue("baz");
    List<EntityFoo> entities = Arrays.asList(entity1, entity2, entity3);

    // addEntities tested in testAddEntities
    _dao.addEntities(entities);

    _dao.removeEntities(Arrays.asList(entity1.getUrn(), entity3.getUrn()));
    assertFalse(getNode(entity1.getUrn()).isPresent());
    assertTrue(getNode(entity2.getUrn()).isPresent());
    assertFalse(getNode(entity3.getUrn()).isPresent());
  }

  @Test
  public void testAddRelationshipNodeNonExist() throws Exception {
    FooUrn urn1 = makeFooUrn(1);
    BarUrn urn2 = makeBarUrn(2);
    RelationshipFoo relationship = new RelationshipFoo().setSource(urn1).setDestination(urn2);

    _dao.addRelationship(relationship, REMOVE_NONE);

    assertRelationshipFoo(getEdges(relationship), 1);
    assertEntityFoo(getNode(urn1).get(), new EntityFoo().setUrn(urn1));
    assertEntityBar(getNode(urn2).get(), new EntityBar().setUrn(urn2));
  }

  @Test
  public void testPartialUpdateEntityCreatedByRelationship() throws Exception {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    RelationshipFoo relationship = new RelationshipFoo().setSource(urn1).setDestination(urn2);

    _dao.addRelationship(relationship, REMOVE_NONE);

    // Check if adding an entity with same urn and with label creates a new node
    _dao.addEntity(new EntityFoo().setUrn(urn1));
    assertEquals(getAllNodes(urn1).size(), 1);
  }

  @Test
  public void testAddRemoveRelationships() throws Exception {
    // Add entity1
    FooUrn urn1 = makeFooUrn(1);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("foo");
    _dao.addEntity(entity1);
    assertEntityFoo(getNode(urn1).get(), entity1);

    // Add entity2
    BarUrn urn2 = makeBarUrn(2);
    EntityBar entity2 = new EntityBar().setUrn(urn2).setValue("bar");
    _dao.addEntity(entity2);
    assertEntityBar(getNode(urn2).get(), entity2);

    // add relationship1 (urn1 -> urn2)
    RelationshipFoo relationship1 = new RelationshipFoo().setSource(urn1).setDestination(urn2);
    _dao.addRelationship(relationship1, REMOVE_NONE);
    assertRelationshipFoo(getEdges(relationship1), 1);

    // add relationship1 again
    _dao.addRelationship(relationship1);
    assertRelationshipFoo(getEdges(relationship1), 1);

    // add relationship2 (urn1 -> urn3)
    Urn urn3 = makeUrn(3);
    RelationshipFoo relationship2 = new RelationshipFoo().setSource(urn1).setDestination(urn3);
    _dao.addRelationship(relationship2);
    assertRelationshipFoo(getEdgesFromSource(urn1, RelationshipFoo.class), 2);

    // remove relationship1
    _dao.removeRelationship(relationship1);
    assertRelationshipFoo(getEdges(relationship1), 0);

    // remove relationship1 & relationship2
    _dao.removeRelationships(Arrays.asList(relationship1, relationship2));
    assertRelationshipFoo(getEdgesFromSource(urn1, RelationshipFoo.class), 0);
  }

  @Test
  public void testAddRelationshipRemoveAll() throws Exception {
    // Add entity1
    FooUrn urn1 = makeFooUrn(1);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("foo");
    _dao.addEntity(entity1);
    assertEntityFoo(getNode(urn1).get(), entity1);

    // Add entity2
    BarUrn urn2 = makeBarUrn(2);
    EntityBar entity2 = new EntityBar().setUrn(urn2).setValue("bar");
    _dao.addEntity(entity2);
    assertEntityBar(getNode(urn2).get(), entity2);

    // add relationship1 (urn1 -> urn2)
    RelationshipFoo relationship1 = new RelationshipFoo().setSource(urn1).setDestination(urn2);
    _dao.addRelationship(relationship1, REMOVE_NONE);
    assertRelationshipFoo(getEdges(relationship1), 1);

    // add relationship2 (urn1 -> urn3), removeAll from source
    Urn urn3 = makeUrn(3);
    RelationshipFoo relationship2 = new RelationshipFoo().setSource(urn1).setDestination(urn3);
    _dao.addRelationship(relationship2, REMOVE_ALL_EDGES_FROM_SOURCE);
    assertRelationshipFoo(getEdgesFromSource(urn1, RelationshipFoo.class), 1);

    // add relationship3 (urn4 -> urn3), removeAll from destination
    Urn urn4 = makeUrn(4);
    RelationshipFoo relationship3 = new RelationshipFoo().setSource(urn4).setDestination(urn3);
    _dao.addRelationship(relationship3, REMOVE_ALL_EDGES_TO_DESTINATION);
    assertRelationshipFoo(getEdgesFromSource(urn1, RelationshipFoo.class), 0);
    assertRelationshipFoo(getEdgesFromSource(urn4, RelationshipFoo.class), 1);

    // add relationship3 again without removal
    _dao.addRelationship(relationship3);
    assertRelationshipFoo(getEdgesFromSource(urn4, RelationshipFoo.class), 1);

    // add relationship3 again, removeAll from source & destination
    _dao.addRelationship(relationship3, REMOVE_ALL_EDGES_FROM_SOURCE_TO_DESTINATION);
    assertRelationshipFoo(getEdgesFromSource(urn1, RelationshipFoo.class), 0);
    assertRelationshipFoo(getEdgesFromSource(urn4, RelationshipFoo.class), 1);
  }

  @Test
  public void upsertNodeAddNewProperty() throws Exception {
    // given
    final FooUrn urn = makeFooUrn(1);
    final EntityFoo initialEntity = new EntityFoo().setUrn(urn);
    final EntityFoo updatedEntity = new EntityFoo().setUrn(urn).setValue("updated");

    // when
    _dao.addEntity(initialEntity);
    _dao.addEntity(updatedEntity);

    // then
    assertEntityFoo(getNode(urn).get(), updatedEntity);
  }

  @Test
  public void upsertEdgeAddNewProperty() throws Exception {
    // given
    final EntityFoo foo = new EntityFoo().setUrn(makeFooUrn(1));
    final EntityBar bar = new EntityBar().setUrn(makeBarUrn(2)).setValue("bar");
    _dao.addEntity(foo);
    _dao.addEntity(bar);

    final RelationshipFoo initialRelationship =
        new RelationshipFoo().setSource(foo.getUrn()).setDestination(bar.getUrn());
    final RelationshipFoo updatedRelationship =
        new RelationshipFoo().setSource(foo.getUrn()).setDestination(bar.getUrn()).setType("test");
    _dao.addRelationship(initialRelationship);

    // when
    _dao.addRelationship(updatedRelationship);

    // then
    assertEquals(_reader.findRelationships(EntityFoo.class,
        new Filter().setCriteria(new CriterionArray(new Criterion().setField("urn").setValue(foo.getUrn().toString()))),
        EntityBar.class,
        new Filter().setCriteria(new CriterionArray(new Criterion().setField("urn").setValue(bar.getUrn().toString()))),
        RelationshipFoo.class, new Filter().setCriteria(new CriterionArray()), 0, 10),
        Collections.singletonList(updatedRelationship));
  }

  @Test
  public void upsertNodeChangeProperty() throws Exception {
    // given
    final FooUrn urn = makeFooUrn(1);
    final EntityFoo initialEntity = new EntityFoo().setUrn(urn).setValue("before");
    final EntityFoo updatedEntity = new EntityFoo().setUrn(urn).setValue("after");
    _dao.addEntity(initialEntity);

    // when
    _dao.addEntity(updatedEntity);

    // then
    assertEntityFoo(getNode(urn).get(), updatedEntity);
  }

  @Test
  public void upsertEdgeChangeProperty() throws Exception {
    // given
    final EntityFoo foo = new EntityFoo().setUrn(makeFooUrn(1));
    final EntityBar bar = new EntityBar().setUrn(makeBarUrn(2)).setValue("bar");
    _dao.addEntity(foo);
    _dao.addEntity(bar);

    final RelationshipFoo initialRelationship =
        new RelationshipFoo().setSource(foo.getUrn()).setDestination(bar.getUrn()).setType("before");
    final RelationshipFoo updatedRelationship =
        new RelationshipFoo().setSource(foo.getUrn()).setDestination(bar.getUrn()).setType("after");
    _dao.addRelationship(initialRelationship);

    // when
    _dao.addRelationship(updatedRelationship);

    // then
    assertEquals(_reader.findRelationships(EntityFoo.class,
        new Filter().setCriteria(new CriterionArray(new Criterion().setField("urn").setValue(foo.getUrn().toString()))),
        EntityBar.class,
        new Filter().setCriteria(new CriterionArray(new Criterion().setField("urn").setValue(bar.getUrn().toString()))),
        RelationshipFoo.class, new Filter().setCriteria(new CriterionArray()), 0, 10),
        Collections.singletonList(updatedRelationship));
  }

  @Test
  public void upsertNodeRemovedProperty() throws Exception {
    // given
    final FooUrn urn = makeFooUrn(1);
    final EntityFoo initialEntity = new EntityFoo().setUrn(urn).setValue("before");
    final EntityFoo updatedEntity = new EntityFoo().setUrn(urn);
    _dao.addEntity(initialEntity);

    // when
    _dao.addEntity(updatedEntity);

    // then
    // Upsert won't ever delete properties.
    assertEntityFoo(getNode(urn).get(), initialEntity);
  }

  @Test
  public void upsertEdgeRemoveProperty() throws Exception {
    // given
    final EntityFoo foo = new EntityFoo().setUrn(makeFooUrn(1));
    final EntityBar bar = new EntityBar().setUrn(makeBarUrn(2)).setValue("bar");
    _dao.addEntity(foo);
    _dao.addEntity(bar);

    final RelationshipFoo initialRelationship =
        new RelationshipFoo().setSource(foo.getUrn()).setDestination(bar.getUrn()).setType("before");
    final RelationshipFoo updatedRelationship =
        new RelationshipFoo().setSource(foo.getUrn()).setDestination(bar.getUrn());
    _dao.addRelationship(initialRelationship);

    // when
    _dao.addRelationship(updatedRelationship);

    // then
    assertEquals(_reader.findRelationships(EntityFoo.class,
        new Filter().setCriteria(new CriterionArray(new Criterion().setField("urn").setValue(foo.getUrn().toString()))),
        EntityBar.class,
        new Filter().setCriteria(new CriterionArray(new Criterion().setField("urn").setValue(bar.getUrn().toString()))),
        RelationshipFoo.class, new Filter().setCriteria(new CriterionArray()), 0, 10),
        // Upsert won't ever delete properties.
        Collections.singletonList(initialRelationship));
  }

  private void assertEntityFoo(@Nonnull Map<String, Object> node, @Nonnull EntityFoo entity) {
    assertEquals(node.get("urn"), entity.getUrn().toString());
    assertEquals(node.get("value"), entity.getValue());
  }

  private void assertEntityBar(@Nonnull Map<String, Object> node, @Nonnull EntityBar entity) {
    assertEquals(node.get("urn"), entity.getUrn().toString());
    assertEquals(node.get("value"), entity.getValue());
  }

  private void assertRelationshipFoo(@Nonnull List<Map<String, Object>> edges, int count) {
    assertEquals(edges.size(), count);
    edges.forEach(edge -> assertTrue(edge.isEmpty()));
  }
}
