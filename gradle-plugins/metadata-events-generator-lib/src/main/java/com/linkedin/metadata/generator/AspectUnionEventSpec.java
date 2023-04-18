package com.linkedin.metadata.generator;

import com.google.common.collect.Lists;
import lombok.Getter;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

/**
 * An {@link EventSpec} to handle aspect unions, generating MCEs that can contain an array of aspects related to the
 * same URN.
 */
@Getter
public class AspectUnionEventSpec extends EventSpec {

  private final String typerefName;
  private final Collection<String> valueTypes;
  private final String shortTyperefName;

  public AspectUnionEventSpec(String urnType, String typerefName, Collection<String> valueTypes, String baseNamespace) {
    super(urnType, baseNamespace);
    this.typerefName = typerefName;
    this.valueTypes = valueTypes;
    this.shortTyperefName = SchemaGeneratorUtil.stripNamespace(this.typerefName);
  }

  @Override
  Collection<File> renderEventSchemas(File baseDirectory) throws IOException {
    File subdirectory = new File(baseDirectory, SchemaGeneratorUtil.deCapitalize(getEntityName()));
    return Lists.newArrayList(
            renderFile(new File(subdirectory, "MCE_" + getShortTyperefName() + SchemaGeneratorConstants.PDL_SUFFIX),
                    "AspectUnionEvent.rythm"),
            renderFile(new File(subdirectory, "FailedMCE_" + getShortTyperefName() + SchemaGeneratorConstants.PDL_SUFFIX),
                    "FailedAspectUnionEvent.rythm")
    );
  }
}
