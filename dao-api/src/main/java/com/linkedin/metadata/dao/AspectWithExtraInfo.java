package com.linkedin.metadata.dao;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.query.ExtraInfo;
import javax.annotation.Nonnull;
import lombok.NonNull;
import lombok.Value;


/**
 * A value class that holds aspect along with other information.
 */
@Value
public class AspectWithExtraInfo<ASPECT extends RecordTemplate> {

  @Nonnull
  ASPECT aspect;

  @NonNull
  ExtraInfo extraInfo;
}