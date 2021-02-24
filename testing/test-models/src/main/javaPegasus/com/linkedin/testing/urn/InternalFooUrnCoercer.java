package com.linkedin.testing.urn;

import com.linkedin.data.template.Custom;


public class InternalFooUrnCoercer extends BaseUrnCoercer<InternalFooUrn> {
  private static final boolean REGISTER_COERCER =
      Custom.registerCoercer(new InternalFooUrnCoercer(), InternalFooUrn.class);
}
