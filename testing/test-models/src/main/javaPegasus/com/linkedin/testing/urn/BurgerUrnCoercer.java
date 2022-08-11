package com.linkedin.testing.urn;

import com.linkedin.data.template.Custom;


public class BurgerUrnCoercer extends BaseUrnCoercer<BurgerUrn> {
  private static final boolean REGISTER_COERCER = Custom.registerCoercer(new BurgerUrnCoercer(), BurgerUrn.class);
}

