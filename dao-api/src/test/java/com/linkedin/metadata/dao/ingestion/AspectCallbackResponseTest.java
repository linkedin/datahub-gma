package com.linkedin.metadata.dao.ingestion;

import com.linkedin.data.template.RecordTemplate;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;


public class AspectCallbackResponseTest {

  @Test
  public void testConstructorAndGetter() {
    // Create a mock instance of RecordTemplate
    RecordTemplate mockAspect = mock(RecordTemplate.class);

    // Create an instance of AspectCallbackResponse with the mock aspect
    AspectCallbackResponse<RecordTemplate> response = new AspectCallbackResponse<>(mockAspect);

    // Verify that the getter returns the correct value
    assertEquals(mockAspect, response.getUpdatedAspect());
  }
}
