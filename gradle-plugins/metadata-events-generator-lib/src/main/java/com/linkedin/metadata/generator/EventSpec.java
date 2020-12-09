package com.linkedin.metadata.generator;

import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;


/**
 * Getter & setter class for schema event metadata.
 */
@Data
@RequiredArgsConstructor
@Builder(toBuilder = true)
public final class EventSpec {
  // fullValueType of the model, such as: com.linkedin.identity.CorpUserInfo.
  private final String fullValueType;

  // namespace of the model, such as: com.linkedin.identity.
  private final String namespace;

  // specType of the model, such as: MetadataChangeEvent.
  private final SchemaGeneratorConstants.MetadataEventType eventType;

  // entity that this aspect refers to, such as: com.linkedin.common.CorpuserUrn.
  private final String urn;

  // valueType of the model, such as: CorpUserInfo.
  private final String valueType;
}