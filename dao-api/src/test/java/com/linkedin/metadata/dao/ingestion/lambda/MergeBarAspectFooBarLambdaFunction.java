package com.linkedin.metadata.dao.ingestion.lambda;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.ingestion.BaseLambdaFunction;
import com.linkedin.testing.BarUrnArray;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.localrelationship.AspectFooBar;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class MergeBarAspectFooBarLambdaFunction extends BaseLambdaFunction<AspectFooBar> {
  public MergeBarAspectFooBarLambdaFunction(@Nonnull Class<AspectFooBar> aspectFooBarClass) {
    super(aspectFooBarClass);
  }

  @Nonnull
  @Override
  public <URN extends Urn> AspectFooBar apply(@Nonnull URN urn, @Nullable Optional<AspectFooBar> oldAspect,
      @Nonnull AspectFooBar newAspect) {
    final AspectFooBar newAspectFooBar = new AspectFooBar();
    final LinkedHashSet<BarUrn> bars = new LinkedHashSet<>();

    if (oldAspect != null && oldAspect.isPresent()) {
      bars.addAll(oldAspect.get().getBars().stream().collect(Collectors.toList()));
    }
    bars.addAll(newAspect.getBars().stream().collect(Collectors.toList()));
    newAspectFooBar.setBars(new BarUrnArray(bars));

    return newAspectFooBar;
  }
}
