package com.linkedin.metadata.restli.lix;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import javax.annotation.Nonnull;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class DummyResourceLixTest {
  @Test
  public void testDefaultResource() throws Exception {
    testResourceSwitch(new LegacyResourceImpl(), false);
    testResourceSwitch(new RampedResourceImpl(), true);
  }

  private void testResourceSwitch(@Nonnull ResourceLix instance, boolean expected) throws Exception {
    Class<?> resourceLixClass = instance.getClass();
    for (Method method : resourceLixClass.getMethods()) {
      if (method.getName().startsWith("test") && !Modifier.isPrivate(method.getModifiers())) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        Object[] params = new Object[parameterTypes.length];
        for (int i = 0; i < params.length; i++) {
          params[i] = null;
        }
        assertEquals(method.invoke(instance, params), expected);
      }
    }
  }
}