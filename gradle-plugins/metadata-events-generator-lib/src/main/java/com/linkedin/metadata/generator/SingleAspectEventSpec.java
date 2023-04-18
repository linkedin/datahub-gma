package com.linkedin.metadata.generator;

import com.google.common.collect.Lists;
import lombok.Getter;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

/**
 * An {@link EventSpec} to create events for submissions of single urn, single aspect.
 */
@Getter
public class SingleAspectEventSpec extends EventSpec {

  // fullValueType of the model, such as: com.linkedin.identity.CorpUserInfo.
  private final String fullValueType;

  // valueType of the model, such as: CorpUserInfo.
  private final String shortValueType;

  public SingleAspectEventSpec(String fullValueType, String urnType, String baseNamespace) {
    super(urnType, baseNamespace);
    this.fullValueType = fullValueType;
    this.shortValueType = SchemaGeneratorUtil.stripNamespace(fullValueType);
  }

  @Override
  public String getNamespace() {
    return String.format("%s.%s", super.getNamespace(), SchemaGeneratorUtil.deCapitalize(this.shortValueType));
  }

  @Override
  public Collection<File> renderEventSchemas(File baseDirectory) throws IOException {
    File subdirectory = new File (new File(baseDirectory, SchemaGeneratorUtil.deCapitalize(getEntityName())),
            SchemaGeneratorUtil.deCapitalize(this.getShortValueType()));
    return Lists.newArrayList(
            renderFile(new File(subdirectory, SchemaGeneratorConstants.MetadataEventType.CHANGE.getDefaultFileName()),
                    SchemaGeneratorConstants.MetadataEventType.CHANGE.getTemplateName()),
            renderFile(new File(subdirectory, SchemaGeneratorConstants.MetadataEventType.AUDIT.getDefaultFileName()),
                    SchemaGeneratorConstants.MetadataEventType.AUDIT.getTemplateName()),
            renderFile(new File(subdirectory, SchemaGeneratorConstants.MetadataEventType.FAILED_CHANGE.getDefaultFileName()),
                    SchemaGeneratorConstants.MetadataEventType.FAILED_CHANGE.getTemplateName())
    );
  }
}
