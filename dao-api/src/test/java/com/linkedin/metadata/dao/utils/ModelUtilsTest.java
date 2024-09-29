package com.linkedin.metadata.dao.utils;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.CommonTestAspect;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.validator.NullFieldException;
import com.linkedin.testing.AspectAttributes;
import com.linkedin.testing.AspectFooEvolved;
import com.linkedin.testing.AspectUnionWithSoftDeletedAspect;
import com.linkedin.testing.BarAsset;
import com.linkedin.testing.BarUrnArray;
import com.linkedin.testing.DatasetInfo;
import com.linkedin.testing.DeltaUnionAlias;
import com.linkedin.testing.EntityAspectUnionAliasArray;
import com.linkedin.testing.EntityAsset;
import com.linkedin.testing.EntityDeltaAlias;
import com.linkedin.testing.EntityFoo;
import com.linkedin.testing.EntityFooInvalid;
import com.linkedin.testing.EntityFooOptionalUrn;
import com.linkedin.testing.EntitySnapshotAlias;
import com.linkedin.testing.EntitySnapshotAliasOptionalFields;
import com.linkedin.testing.EntityUnion;
import com.linkedin.testing.EntityUnionAlias;
import com.linkedin.testing.EntityValue;
import com.linkedin.testing.InternalEntityAspectUnion;
import com.linkedin.testing.InternalEntityAspectUnionArray;
import com.linkedin.testing.InternalEntitySnapshot;
import com.linkedin.testing.PizzaInfo;
import com.linkedin.testing.PizzaOrder;
import com.linkedin.testing.SnapshotUnionAlias;
import com.linkedin.testing.SnapshotUnionAliasWithEntitySnapshotAliasOptionalFields;
import com.linkedin.testing.TyperefPizzaAspect;
import com.linkedin.testing.localrelationship.AspectFooBar;
import com.linkedin.testing.namingedgecase.InternalEntitySnapshotNamingEdgeCase;
import com.linkedin.testing.urn.PizzaUrn;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.validator.InvalidSchemaException;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.DeltaUnion;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.EntityAspectUnionAlias;
import com.linkedin.testing.EntityAspectUnionArray;
import com.linkedin.testing.EntityBar;
import com.linkedin.testing.EntityDelta;
import com.linkedin.testing.EntityDocument;
import com.linkedin.testing.EntityDocumentInvalid;
import com.linkedin.testing.EntityDocumentOptionalUrn;
import com.linkedin.testing.EntitySnapshot;
import com.linkedin.testing.EntitySnapshotInvalid;
import com.linkedin.testing.EntitySnapshotOptionalFields;
import com.linkedin.testing.InvalidAspectUnion;
import com.linkedin.testing.RelationshipFoo;
import com.linkedin.testing.RelationshipFooOptionalFields;
import com.linkedin.testing.RelationshipUnion;
import com.linkedin.testing.RelationshipUnionAlias;
import com.linkedin.testing.SnapshotUnion;
import com.linkedin.testing.SnapshotUnionWithEntitySnapshotOptionalFields;
import com.linkedin.testing.urn.FooUrn;
import com.linkedin.testing.namingedgecase.EntityValueNamingEdgeCase;
import com.linkedin.testing.namingedgecase.InternalEntityAspectUnionNamingEdgeCase;
import com.linkedin.testing.namingedgecase.InternalEntityAspectUnionNamingEdgeCaseArray;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static com.linkedin.testing.TestUtils.*;
import static org.testng.Assert.*;


public class ModelUtilsTest {

  class ChildUrn extends Urn {

    public ChildUrn(String rawUrn) throws URISyntaxException {
      super(rawUrn);
    }
  }

  @Test
  public void testGetAspectName() {
    String aspectName = ModelUtils.getAspectName(AspectFoo.class);
    assertEquals(aspectName, "com.linkedin.testing.AspectFoo");
  }

  @Test
  public void testGetAspectClass() {
    Class aspectClass = ModelUtils.getAspectClass("com.linkedin.testing.AspectFoo");
    assertEquals(aspectClass, AspectFoo.class);
  }

  @Test(expectedExceptions = ClassCastException.class)
  public void testGetInvalidAspectClass() {
    ModelUtils.getAspectClass(EntityAspectUnion.class.getCanonicalName());
  }

  @Test
  public void testGetValidAspectTypes() {
    Set<Class<? extends RecordTemplate>> validTypes = ModelUtils.getValidAspectTypes(EntityAspectUnion.class);

    assertEquals(validTypes,
        ImmutableSet.of(AspectFoo.class, AspectBar.class, AspectFooBar.class, AspectAttributes.class));
  }

  @Test
  public void testSoftDeletedAspect() {
    assertThrows(InvalidSchemaException.class,
        () -> ModelUtils.getValidAspectTypes(AspectUnionWithSoftDeletedAspect.class));
  }

  @Test
  public void testGetValidAspectTypesWithTyperef() {
    Set<Class<? extends RecordTemplate>> validTypes = ModelUtils.getValidAspectTypes(TyperefPizzaAspect.class);

    assertEquals(validTypes, ImmutableSet.of(PizzaInfo.class, PizzaOrder.class));
  }

  @Test
  public void testGetValidSnapshotClassFromName() {
    Class<? extends RecordTemplate> actualClass =
        ModelUtils.getMetadataSnapshotClassFromName(EntitySnapshot.class.getCanonicalName());
    assertEquals(actualClass, EntitySnapshot.class);
  }

  @Test(expectedExceptions = InvalidSchemaException.class)
  public void testGetInvalidSnapshotClassFromName() {
    ModelUtils.getMetadataSnapshotClassFromName(AspectFoo.class.getCanonicalName());
  }

  @Test
  public void testGetUrnFromSnapshot() {
    Urn expected = makeUrn(1);
    EntitySnapshot snapshot = new EntitySnapshot().setUrn(expected);

    Urn urn = ModelUtils.getUrnFromSnapshot(snapshot);
    assertEquals(urn, expected);
  }

  @Test
  public void testGetNullUrnFromSnapshot() {
    EntitySnapshotOptionalFields snapshot = new EntitySnapshotOptionalFields().setUrn(null, SetMode.IGNORE_NULL);
    assertThrows(NullFieldException.class, () -> ModelUtils.getUrnFromSnapshot(snapshot));
  }

  @Test
  public void testGetUrnFromAsset() {
    Urn expected = makeUrn(1);
    EntityAsset asset = new EntityAsset().setUrn(expected);

    Urn urn = ModelUtils.getUrnFromAsset(asset);
    assertEquals(urn, expected);
  }

  @Test
  public void testGetNullUrnFromAsset() {
    EntityAsset asset = new EntityAsset().setUrn(null, SetMode.IGNORE_NULL);
    assertThrows(NullFieldException.class, () -> ModelUtils.getUrnFromAsset(asset));
  }

  @Test
  public void testGetUrnFromSnapshotUnion() {
    Urn expected = makeUrn(1);
    EntitySnapshot snapshot = new EntitySnapshot().setUrn(expected);
    SnapshotUnion snapshotUnion = new SnapshotUnion();
    snapshotUnion.setEntitySnapshot(snapshot);

    Urn urn = ModelUtils.getUrnFromSnapshotUnion(snapshotUnion);
    assertEquals(urn, expected);
  }

  @Test
  public void testGetUrnFromSnapshotUnionAlias() {
    Urn expected = makeUrn(1);
    EntitySnapshotAlias snapshot = new EntitySnapshotAlias().setUrn(expected);
    SnapshotUnionAlias snapshotUnion = new SnapshotUnionAlias();
    snapshotUnion.setEntity(snapshot);

    Urn urn = ModelUtils.getUrnFromSnapshotUnion(snapshotUnion);
    assertEquals(urn, expected);
  }

  @Test
  public void testGetUrnFromDelta() {
    Urn expected = makeUrn(1);
    EntityDelta delta = new EntityDelta().setUrn(expected);

    Urn urn = ModelUtils.getUrnFromDelta(delta);
    assertEquals(urn, expected);
  }

  @Test
  public void testGetUrnFromDeltaUnion() {
    Urn expected = makeUrn(1);
    EntityDelta delta = new EntityDelta().setUrn(expected);
    DeltaUnion deltaUnion = new DeltaUnion();
    deltaUnion.setEntityDelta(delta);

    Urn urn = ModelUtils.getUrnFromDeltaUnion(deltaUnion);
    assertEquals(urn, expected);
  }

  @Test
  public void testGetUrnFromDeltaUnionAlias() {
    Urn expected = makeUrn(1);
    EntityDeltaAlias delta = new EntityDeltaAlias().setUrn(expected);
    DeltaUnionAlias deltaUnion = new DeltaUnionAlias();
    deltaUnion.setEntity(delta);

    Urn urn = ModelUtils.getUrnFromDeltaUnion(deltaUnion);
    assertEquals(urn, expected);
  }

  @Test
  public void testGetUrnFromDocument() {
    Urn expected = makeUrn(1);
    EntityDocument document = new EntityDocument().setUrn(expected);

    Urn urn = ModelUtils.getUrnFromDocument(document);
    assertEquals(urn, expected);
  }

  @Test
  public void testGetNullUrnFromDocument() {
    EntityDocumentOptionalUrn document = new EntityDocumentOptionalUrn().setUrn(null, SetMode.IGNORE_NULL);
    assertThrows(NullFieldException.class, () -> ModelUtils.getUrnFromDocument(document));
  }

  @Test
  public void testGetUrnFromEntity() {
    FooUrn expected = makeFooUrn(1);
    EntityFoo entity = new EntityFoo().setUrn(expected);

    Urn urn = ModelUtils.getUrnFromEntity(entity);
    assertEquals(urn, expected);
  }

  @Test
  public void testGetNullUrnFromEntity() {
    EntityFooOptionalUrn entity = new EntityFooOptionalUrn().setUrn(null, SetMode.IGNORE_NULL);
    assertThrows(NullFieldException.class, () -> ModelUtils.getUrnFromEntity(entity));
  }

  @Test
  public void testGetUrnFromRelationship() {
    FooUrn expectedSource = makeFooUrn(1);
    BarUrn expectedDestination = makeBarUrn(1);
    RelationshipFoo relationship = new RelationshipFoo().setSource(expectedSource).setDestination(expectedDestination);

    Urn sourceUrn = ModelUtils.getSourceUrnFromRelationship(relationship);
    Urn destinationUrn = ModelUtils.getDestinationUrnFromRelationship(relationship);
    assertEquals(sourceUrn, expectedSource);
    assertEquals(destinationUrn, expectedDestination);
  }

  @Test
  public void testGetNullUrnFromRelationship() {
    RelationshipFooOptionalFields relationship =
        new RelationshipFooOptionalFields().setSource(null, SetMode.IGNORE_NULL)
            .setDestination(null, SetMode.IGNORE_NULL);
    assertThrows(NullFieldException.class, () -> ModelUtils.getSourceUrnFromRelationship(relationship));
    assertThrows(NullFieldException.class, () -> ModelUtils.getDestinationUrnFromRelationship(relationship));
  }

  @Test
  public void testGetAspectsFromSnapshot() {
    EntitySnapshot snapshot = new EntitySnapshot();
    snapshot.setAspects(new EntityAspectUnionArray());
    snapshot.getAspects().add(new EntityAspectUnion());
    AspectFoo foo = new AspectFoo();
    snapshot.getAspects().get(0).setAspectFoo(foo);

    List<? extends RecordTemplate> aspects = ModelUtils.getAspectsFromSnapshot(snapshot);

    assertEquals(aspects.size(), 1);
    assertEquals(aspects.get(0), foo);
  }

  @Test
  public void testGetNullAspectsFromSnapshot() {
    EntitySnapshotOptionalFields snapshot = new EntitySnapshotOptionalFields();
    snapshot.setAspects(null, SetMode.IGNORE_NULL);
    assertThrows(NullFieldException.class, () -> ModelUtils.getAspectsFromSnapshot(snapshot));
  }

  @Test
  public void testGetAspectsFromSnapshotAlias() {
    EntitySnapshotAlias snapshot = new EntitySnapshotAlias();
    snapshot.setAspects(new EntityAspectUnionAliasArray());
    AspectFoo foo = new AspectFoo();
    EntityAspectUnionAlias aspect1 = new EntityAspectUnionAlias();
    aspect1.setFoo(foo);
    snapshot.getAspects().add(aspect1);
    AspectBar bar = new AspectBar();
    EntityAspectUnionAlias aspect2 = new EntityAspectUnionAlias();
    aspect2.setBar(bar);
    snapshot.getAspects().add(aspect2);

    List<? extends RecordTemplate> aspects = ModelUtils.getAspectsFromSnapshot(snapshot);

    assertEquals(aspects.size(), 2);
    assertEquals(aspects.get(0), foo);
    assertEquals(aspects.get(1), bar);
  }

  @Test
  public void testGetNullAspectsFromSnapshotAlias() {
    EntitySnapshotAliasOptionalFields snapshot = new EntitySnapshotAliasOptionalFields();
    snapshot.setAspects(null, SetMode.IGNORE_NULL);
    assertThrows(NullFieldException.class, () -> ModelUtils.getAspectsFromSnapshot(snapshot));
  }

  @Test
  public void testGetAspectsFromAsset() {
    EntityAsset asset = new EntityAsset();
    AspectFoo expectedAspectFoo = new AspectFoo().setValue("foo");
    AspectBar expectedAspectBar = new AspectBar().setValue("bar");
    asset.setFoo(expectedAspectFoo);
    asset.setBar(expectedAspectBar);

    List<? extends RecordTemplate> aspects = ModelUtils.getAspectsFromAsset(asset);

    assertEquals(aspects.size(), 2);
    assertEquals(aspects.get(0), expectedAspectFoo);
    assertEquals(aspects.get(1), expectedAspectBar);


    BarAsset barAsset = new BarAsset();
    aspects = ModelUtils.getAspectsFromAsset(barAsset);
    assertTrue(aspects.isEmpty());
  }

  @Test
  public void testGetAspectTypesFromAssetType() {
    List<Class<? extends RecordTemplate>> assetTypes = ModelUtils.getAspectTypesFromAssetType(BarAsset.class);
    assertEquals(assetTypes.size(), 1);
    assertEquals(assetTypes.get(0), AspectBar.class);

    assetTypes = ModelUtils.getAspectTypesFromAssetType(EntityAsset.class);
    assertEquals(assetTypes.size(), 5);
    assertEquals(assetTypes.get(0), AspectFoo.class);
    assertEquals(assetTypes.get(1), AspectBar.class);
    assertEquals(assetTypes.get(2), AspectFooEvolved.class);
    assertEquals(assetTypes.get(3), AspectFooBar.class);
    assertEquals(assetTypes.get(4), AspectAttributes.class);
  }

  @Test
  public void testGetAspectFromSnapshot() {
    EntitySnapshot snapshot = new EntitySnapshot();
    snapshot.setAspects(new EntityAspectUnionArray());
    snapshot.getAspects().add(new EntityAspectUnion());
    AspectFoo foo = new AspectFoo();
    snapshot.getAspects().get(0).setAspectFoo(foo);

    Optional<AspectFoo> aspectFoo = ModelUtils.getAspectFromSnapshot(snapshot, AspectFoo.class);
    assertTrue(aspectFoo.isPresent());
    assertEquals(aspectFoo.get(), foo);

    Optional<AspectBar> aspectBar = ModelUtils.getAspectFromSnapshot(snapshot, AspectBar.class);
    assertFalse(aspectBar.isPresent());
  }

  @Test
  public void testGetNullAspectFromSnapshot() {
    EntitySnapshotOptionalFields snapshot = new EntitySnapshotOptionalFields();
    snapshot.setAspects(null, SetMode.IGNORE_NULL);
    assertThrows(NullFieldException.class, () -> ModelUtils.getAspectFromSnapshot(snapshot, AspectFoo.class));
  }

  @Test
  public void testGetAspectsFromSnapshotUnion() {
    EntitySnapshot snapshot = new EntitySnapshot();
    snapshot.setAspects(new EntityAspectUnionArray());
    snapshot.getAspects().add(new EntityAspectUnion());
    AspectFoo foo = new AspectFoo();
    snapshot.getAspects().get(0).setAspectFoo(foo);
    SnapshotUnion snapshotUnion = new SnapshotUnion();
    snapshotUnion.setEntitySnapshot(snapshot);

    List<? extends RecordTemplate> aspects = ModelUtils.getAspectsFromSnapshotUnion(snapshotUnion);

    assertEquals(aspects.size(), 1);
    assertEquals(aspects.get(0), foo);
  }

  @Test
  public void testGetNullAspectsFromSnapshotUnion() {
    EntitySnapshotOptionalFields snapshot = new EntitySnapshotOptionalFields();
    snapshot.setAspects(null, SetMode.IGNORE_NULL);
    SnapshotUnionWithEntitySnapshotOptionalFields snapshotUnion = new SnapshotUnionWithEntitySnapshotOptionalFields();
    snapshotUnion.setEntitySnapshotOptionalFields(snapshot);
    assertThrows(NullFieldException.class, () -> ModelUtils.getAspectsFromSnapshotUnion(snapshotUnion));
  }

  @Test
  public void testGetAspectsFromSnapshotUnionAlias() {
    EntitySnapshotAlias snapshot = new EntitySnapshotAlias();
    snapshot.setAspects(new EntityAspectUnionAliasArray());
    snapshot.getAspects().add(new EntityAspectUnionAlias());
    AspectFoo foo = new AspectFoo();
    snapshot.getAspects().get(0).setFoo(foo);
    SnapshotUnionAlias snapshotUnion = new SnapshotUnionAlias();
    snapshotUnion.setEntity(snapshot);

    List<? extends RecordTemplate> aspects = ModelUtils.getAspectsFromSnapshotUnion(snapshotUnion);

    assertEquals(aspects.size(), 1);
    assertEquals(aspects.get(0), foo);
  }

  public void testGetNullAspectsFromSnapshotUnionAlias() {
    EntitySnapshotAliasOptionalFields snapshot = new EntitySnapshotAliasOptionalFields();
    snapshot.setAspects(null, SetMode.IGNORE_NULL);
    SnapshotUnionAliasWithEntitySnapshotAliasOptionalFields snapshotUnion =
        new SnapshotUnionAliasWithEntitySnapshotAliasOptionalFields();
    snapshotUnion.setEntity(snapshot);

    assertThrows(NullFieldException.class, () -> ModelUtils.getAspectsFromSnapshotUnion(snapshotUnion));
  }

  @Test
  public void testNewSnapshot() {
    Urn urn = makeUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    EntityAspectUnion aspectUnion = new EntityAspectUnion();
    aspectUnion.setAspectFoo(foo);

    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn, Lists.newArrayList(aspectUnion));

    assertEquals(snapshot.getUrn(), urn);
    assertEquals(snapshot.getAspects().size(), 1);
    assertEquals(snapshot.getAspects().get(0).getAspectFoo(), foo);
  }

  @Test
  public void testNewSnapshotInvalidSchema() {
    Urn urn = makeUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    EntityAspectUnion aspectUnion = new EntityAspectUnion();
    aspectUnion.setAspectFoo(foo);

    assertThrows(InvalidSchemaException.class,
        () -> ModelUtils.newSnapshot(EntitySnapshotInvalid.class, urn, Lists.newArrayList(aspectUnion)));
  }

  @Test
  public void testNewAspect() {
    AspectFoo foo = new AspectFoo().setValue("foo");

    EntityAspectUnion aspectUnion = ModelUtils.newAspectUnion(EntityAspectUnion.class, foo);

    assertEquals(aspectUnion.getAspectFoo(), foo);
  }

  @Test
  public void testNewAspectAlias() {
    AspectFoo foo = new AspectFoo().setValue("foo");

    EntityAspectUnionAlias aspectUnion = ModelUtils.newAspectUnion(EntityAspectUnionAlias.class, foo);

    assertEquals(aspectUnion.getFoo(), foo);
  }

  @Test
  public void testAspectClassForSnapshot() {
    assertEquals(ModelUtils.aspectClassForSnapshot(EntitySnapshot.class), EntityAspectUnion.class);
  }

  @Test
  public void testAspectClassForSnapshotInvalidSchema() {
    assertThrows(InvalidSchemaException.class, () -> ModelUtils.aspectClassForSnapshot(EntitySnapshotInvalid.class));
  }

  @Test
  public void testUrnClassForEntity() {
    assertEquals(ModelUtils.urnClassForEntity(EntityBar.class), BarUrn.class);
  }

  @Test
  public void testUrnClassForEntityInvalidSchema() {
    assertThrows(InvalidSchemaException.class, () -> ModelUtils.urnClassForEntity(EntityFooInvalid.class));
  }

  @Test
  public void testUrnClassForSnapshot() {
    assertEquals(ModelUtils.urnClassForSnapshot(EntitySnapshot.class), Urn.class);
  }

  @Test
  public void testUrnClassForSnapshotInvalidSchema() {
    assertThrows(InvalidSchemaException.class, () -> ModelUtils.urnClassForSnapshot(EntitySnapshotInvalid.class));
  }

  @Test
  public void testUrnClassForDelta() {
    assertEquals(ModelUtils.urnClassForDelta(EntityDelta.class), Urn.class);
  }

  @Test
  public void testUrnClassForDocument() {
    assertEquals(ModelUtils.urnClassForDocument(EntityDocument.class), Urn.class);
  }

  @Test
  public void testUrnClassForDocumentInvalidSchema() {
    assertThrows(InvalidSchemaException.class, () -> ModelUtils.urnClassForDocument(EntityDocumentInvalid.class));
  }

  @Test(expectedExceptions = InvalidSchemaException.class)
  public void testValidateIncorrectAspectForSnapshot() {
    ModelUtils.validateSnapshotAspect(EntitySnapshot.class, InvalidAspectUnion.class);
  }

  @Test
  public void testValidateCorrectAspectForSnapshot() {
    ModelUtils.validateSnapshotAspect(EntitySnapshot.class, EntityAspectUnion.class);
  }

  @Test
  public void testValidateCorrectUrnForSnapshot() {
    ModelUtils.validateSnapshotUrn(EntitySnapshot.class, Urn.class);
    ModelUtils.validateSnapshotUrn(EntitySnapshot.class, ChildUrn.class);
  }

  @Test
  public void testNewRelationshipUnion() {
    RelationshipFoo foo = new RelationshipFoo().setDestination(makeFooUrn(1)).setSource(makeFooUrn(2));

    RelationshipUnion relationshipUnion = ModelUtils.newRelationshipUnion(RelationshipUnion.class, foo);

    assertEquals(relationshipUnion.getRelationshipFoo(), foo);
  }

  @Test
  public void testNewRelationshipUnionAlias() {
    RelationshipFoo foo = new RelationshipFoo().setDestination(makeFooUrn(1)).setSource(makeFooUrn(2));

    RelationshipUnionAlias relationshipUnion = ModelUtils.newRelationshipUnion(RelationshipUnionAlias.class, foo);

    assertEquals(relationshipUnion.getFoo(), foo);
  }

  @Test
  public void testGetMAETopicName() throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");

    assertEquals(ModelUtils.getAspectSpecificMAETopicName(urn, foo), "METADATA_AUDIT_EVENT_FOO_ASPECTFOO");

    PizzaUrn pizza = new PizzaUrn(1);
    AspectBar bar = new AspectBar().setValue("bar");
    assertEquals(ModelUtils.getAspectSpecificMAETopicName(pizza, bar), "METADATA_AUDIT_EVENT_PIZZA_ASPECTBAR");
  }

  @Test
  public void testIsCommonAspect() {
    boolean result = ModelUtils.isCommonAspect(AspectFoo.class);
    assertFalse(result);

    result = ModelUtils.isCommonAspect(CommonTestAspect.class);
    assertTrue(result);
  }

  @Test
  public void testNewEntityUnion() {
    EntityFoo entityFoo = new EntityFoo().setUrn(makeFooUrn(1));
    EntityUnion entityUnion = ModelUtils.newEntityUnion(EntityUnion.class, entityFoo);

    assertEquals(entityUnion.getEntityFoo(), entityFoo);
  }

  @Test
  public void testNewEntityUnionAlias() {
    EntityFoo entityFoo = new EntityFoo().setUrn(makeFooUrn(1));
    EntityUnionAlias entityUnion = ModelUtils.newEntityUnion(EntityUnionAlias.class, entityFoo);

    assertEquals(entityUnion.getFoo(), entityFoo);
  }

  @Test
  public void testGetAspectClassNames() {
    List<String> classNames = ModelUtils.getAspectClassNames(EntityUnion.class);
    assertEquals(classNames.get(0), "com.linkedin.testing.EntityFoo");
    assertEquals(classNames.get(1), "com.linkedin.testing.EntityBar");
  }

  @Test
  public void testGetUnionClassFromSnapshot() {
    Class<UnionTemplate> unionTemplate = ModelUtils.getUnionClassFromSnapshot(EntitySnapshot.class);
    assertEquals(unionTemplate.getCanonicalName(), "com.linkedin.testing.EntityAspectUnion");
  }

  @Test
  public void testGetUrnFromString() {
    FooUrn urn = ModelUtils.getUrnFromString("urn:li:foo:1", FooUrn.class);
    assertEquals(urn, makeFooUrn(1));
  }

  @Test
  public void testGetUrnFromStringException() {
    assertThrows(IllegalArgumentException.class, () -> ModelUtils.getUrnFromString("urn:li:foo", FooUrn.class));
  }

  @Test
  public void testNewAsset() {
    AspectFoo expectedFoo = new AspectFoo().setValue("foo");
    AspectBar expectedBar = new AspectBar().setValue("bar");
    Urn expectedUrn = makeUrn(1);
    EntityAspectUnion aspectUnion1 = new EntityAspectUnion();
    aspectUnion1.setAspectFoo(expectedFoo);

    EntityAspectUnion aspectUnion2 = new EntityAspectUnion();
    aspectUnion2.setAspectBar(expectedBar);

    EntityAsset asset =
        ModelUtils.newAsset(EntityAsset.class, expectedUrn, Lists.newArrayList(aspectUnion1, aspectUnion2));

    assertEquals(asset.getUrn(), expectedUrn);
    assertTrue(asset.hasFoo());
    assertTrue(asset.hasBar());
    assertEquals(asset.getFoo(), expectedFoo);
    assertEquals(asset.getBar(), expectedBar);
    assertFalse(asset.hasAspectAttributes());
  }

  @Test
  public void testConvertSnapshotToAsset() {
    Urn expectedUrn = makeUrn(1);
    EntitySnapshot snapshot = new EntitySnapshot().setUrn(expectedUrn);
    EntityAspectUnion aspectUnion1 = new EntityAspectUnion();
    EntityAspectUnion aspectUnion2 = new EntityAspectUnion();
    EntityAspectUnionArray aspectUnionArray = new EntityAspectUnionArray();
    AspectFoo expectedFoo = new AspectFoo().setValue("foo");
    AspectBar expectedBar = new AspectBar().setValue("bar");
    aspectUnion1.setAspectFoo(expectedFoo);
    aspectUnion2.setAspectBar(expectedBar);
    aspectUnionArray.add(aspectUnion1);
    aspectUnionArray.add(aspectUnion2);
    snapshot.setAspects(aspectUnionArray);

    EntityAsset entityAsset = ModelUtils.convertSnapshotToAsset(EntityAsset.class, snapshot);

    assertEquals(entityAsset.getUrn(), expectedUrn);
    assertTrue(entityAsset.hasFoo());
    assertTrue(entityAsset.hasBar());
    assertEquals(entityAsset.getFoo(), expectedFoo);
    assertEquals(entityAsset.getBar(), expectedBar);
    assertFalse(entityAsset.hasAspectAttributes());
  }

  @Test
  public void testConvertInternalSnapshotToAsset() {
    Urn expectedUrn = makeUrn(1);
    InternalEntitySnapshot internalEntitySnapshot = new InternalEntitySnapshot().setUrn(expectedUrn);
    InternalEntityAspectUnion internalAspectUnion1 = new InternalEntityAspectUnion();
    InternalEntityAspectUnion internalAspectUnion2 = new InternalEntityAspectUnion();
    InternalEntityAspectUnionArray internalEntityAspectUnionArray = new InternalEntityAspectUnionArray();
    AspectFoo expectedFoo = new AspectFoo().setValue("foo");
    AspectFooBar expectedFooBar = new AspectFooBar().setBars(new BarUrnArray(new BarUrn(2)));
    internalAspectUnion1.setAspectFoo(expectedFoo);
    internalAspectUnion2.setAspectFooBar(expectedFooBar);
    internalEntityAspectUnionArray.add(internalAspectUnion1);
    internalEntityAspectUnionArray.add(internalAspectUnion2);
    internalEntitySnapshot.setAspects(internalEntityAspectUnionArray);

    EntityAsset entityAsset = ModelUtils.convertSnapshotToAsset(EntityAsset.class, internalEntitySnapshot);

    assertEquals(entityAsset.getUrn(), expectedUrn);
    assertEquals(entityAsset.getFoo(), expectedFoo);
    assertEquals(entityAsset.getAspectFooBar(), expectedFooBar);
    assertFalse(entityAsset.hasBar());
    assertFalse(entityAsset.hasAspectAttributes());
  }

  @Test
  public void testConvertAssetToInternalSnapshot() {
    Urn expectedUrn = makeUrn(1);
    EntityAsset asset = new EntityAsset();
    AspectFoo expectedFoo = new AspectFoo().setValue("foo");
    AspectBar expectedBar = new AspectBar().setValue("bar");
    AspectFooBar expectedFooBar = new AspectFooBar().setBars(new BarUrnArray(new BarUrn(2)));
    asset.setUrn(expectedUrn);
    asset.setFoo(expectedFoo);
    asset.setBar(expectedBar);
    asset.setAspectFooBar(expectedFooBar);

    InternalEntitySnapshot internalEntitySnapshot =
        ModelUtils.convertAssetToInternalSnapshot(InternalEntitySnapshot.class, asset);

    assertEquals(internalEntitySnapshot.getUrn(), expectedUrn);
    assertEquals(internalEntitySnapshot.getAspects().size(), 3);
    assertEquals(internalEntitySnapshot.getAspects().get(0).getAspectFoo(), expectedFoo);
    assertEquals(internalEntitySnapshot.getAspects().get(1).getAspectBar(), expectedBar);
    assertEquals(internalEntitySnapshot.getAspects().get(2).getAspectFooBar(), expectedFooBar);
  }

  @Test
  public void testConvertSnapshotToInternalSnapshot() {
    Urn expectedUrn = makeUrn(1);
    EntitySnapshot snapshot = new EntitySnapshot().setUrn(expectedUrn);
    EntityAspectUnion aspectUnion1 = new EntityAspectUnion();
    EntityAspectUnion aspectUnion2 = new EntityAspectUnion();
    EntityAspectUnionArray aspectUnionArray = new EntityAspectUnionArray();
    AspectFoo expectedFoo = new AspectFoo().setValue("foo");
    AspectBar expectedBar = new AspectBar().setValue("bar");
    aspectUnion1.setAspectFoo(expectedFoo);
    aspectUnion2.setAspectBar(expectedBar);
    aspectUnionArray.add(aspectUnion1);
    aspectUnionArray.add(aspectUnion2);
    snapshot.setAspects(aspectUnionArray);

    InternalEntitySnapshot internalEntitySnapshot =
        ModelUtils.convertSnapshots(InternalEntitySnapshot.class, snapshot);

    assertEquals(internalEntitySnapshot.getUrn(), expectedUrn);
    assertEquals(internalEntitySnapshot.getAspects().size(), 2);
    assertEquals(internalEntitySnapshot.getAspects().get(0).getAspectFoo(), expectedFoo);
    assertEquals(internalEntitySnapshot.getAspects().get(1).getAspectBar(), expectedBar);
  }

  @Test
  public void testConvertInternalAspectUnionToAspectUnion() {
    InternalEntityAspectUnion internalEntityAspectUnion1 = new InternalEntityAspectUnion();
    InternalEntityAspectUnion internalEntityAspectUnion2 = new InternalEntityAspectUnion();
    List<InternalEntityAspectUnion> internalEntityAspectUnions = new ArrayList<>();
    AspectFoo expectedFoo = new AspectFoo().setValue("foo");
    AspectBar expectedBar = new AspectBar().setValue("bar");
    internalEntityAspectUnion1.setAspectFoo(expectedFoo);
    internalEntityAspectUnion2.setAspectBar(expectedBar);
    internalEntityAspectUnions.add(internalEntityAspectUnion1);
    internalEntityAspectUnions.add(internalEntityAspectUnion2);


    List<EntityAspectUnion> entityAspectUnions =
        ModelUtils.convertInternalAspectUnionToAspectUnion(EntityAspectUnion.class, internalEntityAspectUnions);

    assertEquals(entityAspectUnions.size(), 2);
    assertEquals(entityAspectUnions.get(0).getAspectFoo(), expectedFoo);
    assertEquals(entityAspectUnions.get(1).getAspectBar(), expectedBar);
  }

  @Test
  public void testDecorateValue() {
    Urn expectedUrn = makeUrn(1);
    InternalEntitySnapshot internalEntitySnapshot = new InternalEntitySnapshot().setUrn(expectedUrn);
    InternalEntityAspectUnion internalAspectUnion1 = new InternalEntityAspectUnion();
    InternalEntityAspectUnion internalAspectUnion2 = new InternalEntityAspectUnion();
    InternalEntityAspectUnion internalAspectUnion3 = new InternalEntityAspectUnion();
    InternalEntityAspectUnionArray internalEntityAspectUnionArray = new InternalEntityAspectUnionArray();
    AspectFoo expectedFoo = new AspectFoo().setValue("foo");
    AspectBar expectedBar = new AspectBar().setValue("bar");
    AspectFooBar expectedFooBar = new AspectFooBar().setBars(new BarUrnArray(new BarUrn(2)));
    internalAspectUnion1.setAspectFoo(expectedFoo);
    internalAspectUnion2.setAspectBar(expectedBar);
    internalAspectUnion3.setAspectFooBar(expectedFooBar);
    internalEntityAspectUnionArray.add(internalAspectUnion1);
    internalEntityAspectUnionArray.add(internalAspectUnion2);
    internalEntityAspectUnionArray.add(internalAspectUnion3);
    internalEntitySnapshot.setAspects(internalEntityAspectUnionArray);

    EntityValue entityValue = new EntityValue();
    AspectFoo entityFoo = new AspectFoo().setValue("fooEntity");
    entityValue.setFoo(entityFoo);


    EntityValue decoratedValue = ModelUtils.decorateValue(internalEntitySnapshot, entityValue);

    assertTrue(decoratedValue.hasFoo());
    assertTrue(decoratedValue.hasBar());
    assertFalse(decoratedValue.hasAttributes());
    assertEquals(decoratedValue.getFoo(), entityFoo);
    assertEquals(decoratedValue.getBar(), expectedBar);
  }

  /**
   * Same as the above but with an aspect that has the substring "set" -- ie. DatasetInfo -- in it.
   * This came as the result of a bug where the code was using .replace() and not .replaceFirst(), which caused
   * strings like "setDatasetInfo" to become "XXXDataXXXInfo" instead of "XXXDatasetInfo".
   */
  @Test
  public void testDecorateValueWithStringReplacementEdgeCase() {
    Urn expectedUrn = makeUrn(1);
    InternalEntitySnapshotNamingEdgeCase internalEntitySnapshot =
        new InternalEntitySnapshotNamingEdgeCase().setUrn(expectedUrn);
    InternalEntityAspectUnionNamingEdgeCase internalAspectUnion4 = new InternalEntityAspectUnionNamingEdgeCase();
    DatasetInfo expectedDatasetInfo = new DatasetInfo().setValue("datasetInfo");
    internalAspectUnion4.setDatasetInfo(expectedDatasetInfo);
    InternalEntityAspectUnionNamingEdgeCaseArray internalEntityAspectUnionArray =
        new InternalEntityAspectUnionNamingEdgeCaseArray();
    internalEntityAspectUnionArray.add(internalAspectUnion4);
    internalEntitySnapshot.setAspects(internalEntityAspectUnionArray);

    EntityValueNamingEdgeCase entityValue = new EntityValueNamingEdgeCase();
    DatasetInfo datasetInfo = new DatasetInfo().setValue("datasetInfo");
    entityValue.setDatasetInfo(datasetInfo);

    EntityValueNamingEdgeCase decoratedValue = ModelUtils.decorateValue(internalEntitySnapshot, entityValue);

    assertTrue(decoratedValue.hasDatasetInfo());
    assertEquals(decoratedValue.getDatasetInfo(), expectedDatasetInfo);
  }

  @Test
  public void testGetEntityType() {
    Urn expectedUrn = makeUrn(1);

    String entityType = ModelUtils.getEntityType(expectedUrn);

    assertEquals(entityType, expectedUrn.getEntityType());
    assertNull(ModelUtils.getEntityType(null));
  }

  @Test
  public void testGetUrnTypeFromAssetType() {
    String assetType = ModelUtils.getUrnTypeFromAsset(BarAsset.class);
    assertEquals(BarUrn.ENTITY_TYPE, assetType);
  }

  @Test
  public void testGetAspectAlias() {
    assertEquals(ModelUtils.getAspectAlias(BarAsset.class, AspectBar.class.getCanonicalName()), "aspect_bar");
    assertNull(ModelUtils.getAspectAlias(BarAsset.class, AspectFoo.class.getCanonicalName()), "aspect_foo");
    try {
      ModelUtils.getAspectAlias(AspectBar.class, AspectFoo.class.getCanonicalName());
      fail("should fail on non-Asset template");
    } catch (Exception e) {

    }
  }
}
