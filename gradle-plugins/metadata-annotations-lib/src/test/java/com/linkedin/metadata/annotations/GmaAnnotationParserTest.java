package com.linkedin.metadata.annotations;

import com.google.common.collect.ImmutableSetMultimap;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.testing.AnnotatedAspectBar;
import com.linkedin.testing.AnnotatedAspectFoo;
import com.linkedin.testing.CommonAspect;
import java.util.Optional;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;


public class GmaAnnotationParserTest {
  @Test
  public void parseBar() {
    final Optional<GmaAnnotation> gma =
        new GmaAnnotationParser().parse((RecordDataSchema) DataTemplateUtil.getSchema(AnnotatedAspectBar.class));
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
}
