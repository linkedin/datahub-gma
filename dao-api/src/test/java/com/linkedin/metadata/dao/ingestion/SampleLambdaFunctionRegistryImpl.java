package com.linkedin.metadata.dao.ingestion;

import com.google.common.collect.ImmutableMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.ingestion.lambda.MergeBarAspectFooBarLambdaFunction;
import com.linkedin.testing.localrelationship.AspectFooBar;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class SampleLambdaFunctionRegistryImpl implements LambdaFunctionRegistry {
  private final ImmutableMap _lambdaFunctions = new ImmutableMap.Builder().put(AspectFooBar.class,
      Collections.unmodifiableList(new LinkedList<BaseLambdaFunction>() {
        {
          add(new MergeBarAspectFooBarLambdaFunction(AspectFooBar.class));
        }
      })).build();

  @Nullable
  @Override
  public <ASPECT extends RecordTemplate> List<BaseLambdaFunction> getLambdaFunctions(@Nonnull ASPECT aspect) {
    return (List<BaseLambdaFunction>) _lambdaFunctions.get(aspect.getClass());
  }

  @Override
  public <ASPECT extends RecordTemplate> boolean isRegistered(@Nonnull Class<ASPECT> aspectClass) {
    return _lambdaFunctions.containsKey(aspectClass);
  }
}
