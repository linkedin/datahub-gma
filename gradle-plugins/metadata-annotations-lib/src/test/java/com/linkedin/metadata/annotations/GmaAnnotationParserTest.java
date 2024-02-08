package com.linkedin.metadata.annotations;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.testing.AnnotatedAspectBar;
import com.linkedin.testing.AnnotatedAspectFoo;
import com.linkedin.testing.BarAspect;
import com.linkedin.testing.CommonAspect;
import com.linkedin.testing.SearchAnnotatedAspectBar;

import java.util.Optional;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;


public class GmaAnnotationParserTest {
  @Test
  public void parseBar() {
    // has both @gma.aspect.entity.urn and @gma.aspect.column.name annotations
    final Optional<GmaAnnotation> gma =
        new GmaAnnotationParser().parse((RecordDataSchema) DataTemplateUtil.getSchema(AnnotatedAspectBar.class));
    assertThat(gma).contains(new GmaAnnotation().setAspect(
        new AspectAnnotation().setEntity(new AspectEntityAnnotation().setUrn("com.linkedin.testing.BarUrn"))
            .setColumn(new ColumnNameAnnotation().setName("barurn"))));
  }

  @Test
  public void parseBarAspect() {
    // has both @gma.aspect.entity.urn and @gma.aspect.column.name annotations
    final Optional<GmaAnnotation> gma =
            new GmaAnnotationParser().parse(DataTemplateUtil.getTyperefInfo(BarAspect.class).getSchema());
    assertThat(gma).contains(new GmaAnnotation().setAspect(
            new AspectAnnotation().setEntity(new AspectEntityAnnotation().setUrn("com.linkedin.testing.BarUrn"))));
  }

  @Test
  public void parseFoo() {
    final Optional<GmaAnnotation> gma =
        new GmaAnnotationParser().parse((RecordDataSchema) DataTemplateUtil.getSchema(AnnotatedAspectFoo.class));
    assertThat(gma).contains(new GmaAnnotation().setAspect(
        new AspectAnnotation().setEntity(new AspectEntityAnnotation().setUrn("com.linkedin.testing.FooUrn"))));
  }

  @Test
  public void parseCommonAspectAllowed() {
    final Optional<GmaAnnotation> gma = new GmaAnnotationParser(new AlwaysAllowList()).parse(
        (RecordDataSchema) DataTemplateUtil.getSchema(CommonAspect.class));
    assertThat(gma).contains(new GmaAnnotation().setAspect(new AspectAnnotation().setEntities(
        new AspectEntityAnnotationArray(new AspectEntityAnnotation().setUrn("com.linkedin.testing.FooUrn"),
            new AspectEntityAnnotation().setUrn("com.linkedin.testing.BarUrn")))));
  }

  @Test
  public void parseCommonAspectDisallowedUrn() {
    assertThatThrownBy(() -> new GmaAnnotationParser(new GmaEntitiesAnnotationAllowListImpl(
        ImmutableSetMultimap.<String, String>builder().putAll("com.linkedin.testing.CommonAspect",
            "com.linkedin.testing.FooUrn").build())).parse(
        (RecordDataSchema) DataTemplateUtil.getSchema(CommonAspect.class))).isInstanceOf(
        GmaEntitiesAnnotationAllowList.AnnotationNotAllowedException.class)
        .hasMessage("URN com.linkedin.testing.BarUrn is not allowed for entity com.linkedin.testing.CommonAspect");
  }

  @Test
  public void parseCommonAspectDisallowed() {
    assertThatThrownBy(() -> new GmaAnnotationParser().parse(
        (RecordDataSchema) DataTemplateUtil.getSchema(CommonAspect.class))).isInstanceOf(
        GmaEntitiesAnnotationAllowList.AnnotationNotAllowedException.class)
        .hasMessage("@gma.aspect.entities not allowed on com.linkedin.testing.CommonAspect");
  }

  @Test
  public void parseCommonAspectAllowedWithNullAllowList() {
    final Optional<GmaAnnotation> gma = new GmaAnnotationParser(null).parse(
        (RecordDataSchema) DataTemplateUtil.getSchema(CommonAspect.class));
    assertThat(gma).contains(new GmaAnnotation().setAspect(new AspectAnnotation().setEntities(
        new AspectEntityAnnotationArray(new AspectEntityAnnotation().setUrn("com.linkedin.testing.FooUrn"),
            new AspectEntityAnnotation().setUrn("com.linkedin.testing.BarUrn")))));
  }

  @Test
  public void parseAspectWithOnlySearchIndexAnnotations() {
    final Optional<GmaAnnotation> gma =
            new GmaAnnotationParser().parse((RecordDataSchema) DataTemplateUtil.getSchema(SearchAnnotatedAspectBar.class));
     assertThat(gma).contains(new GmaAnnotation().setSearch(
         new SearchAnnotation().setIndex(
             new IndexAnnotationArrayMap(ImmutableMap.of(
                 "stringField", new IndexAnnotationArray(
                     new IndexAnnotation().setUrn("com.linkedin.testing.BarkUrn"),
                     new IndexAnnotation().setUrn("com.linkedin.testing.FooUrn")),
                 "longField", new IndexAnnotationArray(
                     new IndexAnnotation().setUrn("com.linkedin.testing.BarkUrn")
                 ))))));
  }

  // TODO: if add support for disallowing certain search annotations, add tests for them
}
