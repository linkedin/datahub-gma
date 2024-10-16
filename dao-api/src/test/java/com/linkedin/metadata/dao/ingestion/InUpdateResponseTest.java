package com.linkedin.metadata.dao.ingestion;

import com.linkedin.data.template.RecordTemplate;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;


public class InUpdateResponseTest {

  @Test
  public void testConstructorAndGetter() {
    // Create a mock instance of RecordTemplate
    RecordTemplate mockAspect = mock(RecordTemplate.class);

    // Create an instance of InUpdateResponse with the mock aspect
    InUpdateResponse<RecordTemplate> response = new InUpdateResponse<>(mockAspect);

    // Verify that the getter returns the correct value
    assertEquals(mockAspect, response.getUpdatedAspect());
  }
}
