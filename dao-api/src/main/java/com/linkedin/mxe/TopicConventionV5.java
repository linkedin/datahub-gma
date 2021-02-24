package com.linkedin.mxe;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;


/**
 * The convention for naming event topics, meant to be used in conjunction with a {@link
 * com.linkedin.metadata.dao.producer.BaseMetadataEventProducer}.
 *
 * <p>Different companies may have different naming conventions or styles for their kafka topics. Namely, companies
 * should pick _ or . as a delimiter, but not both, as they collide in metric names.
 *
 * <p>This convention applies to MXE v5, not to MXE v4 or below. In MXE v5, every entity/aspect pair can have its
 * own topic, where as in v4 (and below) topics were monolithic. There is a topic convention for v4 as well, but it is
 * defined in the DataHub repo due to tight coupling with MXE v4.
 */
public interface TopicConventionV5 {
  /**
   * Returns the name of the metadata change event topic.
   *
   * @param urn the urn of the entity being updated
   * @param aspect the aspect name being updated
   */
  @Nonnull
  String getMetadataChangeEventTopicName(@Nonnull Urn urn, @Nonnull RecordTemplate aspect);

  /**
   * Returns the class that defines the MCE schema for a given topic.
   *
   * @param urn the urn of the entity being updated
   * @param aspect the aspect name being updated
   */
  Class<?> getMetadataChangeEventSchema(@Nonnull Urn urn, @Nonnull RecordTemplate aspect) throws ClassNotFoundException;

  /**
   * Returns the name of the metadata audit event topic.
   *
   * @param urn the urn of the entity being updated
   * @param aspect the aspect name being updated
   */
  @Nonnull
  String getMetadataAuditEventTopicName(@Nonnull Urn urn, @Nonnull RecordTemplate aspect);

  /**
   * Returns the class that defines the MAE schema for a given topic.
   *
   * @param urn the urn of the entity being updated
   * @param aspect the aspect name being updated
   */
  Class<?> getMetadataAuditEventSchema(@Nonnull Urn urn, @Nonnull RecordTemplate aspect) throws ClassNotFoundException;

  /**
   * Returns the name of the failed metadata change event topic.
   *
   * @param urn the urn of the entity being updated
   * @param aspect the aspect name being updated
   */
  @Nonnull
  String getFailedMetadataChangeEventTopicName(@Nonnull Urn urn, @Nonnull RecordTemplate aspect);

  /**
   * Returns the class that defines the FMCE schema for a given topic.
   *
   * @param urn the urn of the entity being updated
   * @param aspect the aspect name being updated
   */
  Class<?> getFailedMetadataChangeEventSchema(@Nonnull Urn urn, @Nonnull RecordTemplate aspect)
      throws ClassNotFoundException;
}
