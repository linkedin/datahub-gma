package com.linkedin.metadata.dao.ingestion.preupdate;

import com.linkedin.data.template.RecordTemplate;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;


public class PreUpdateResponseTest {

  @Test
  public void testConstructorAndGetter() {
    // Create a mock instance of RecordTemplate
    RecordTemplate mockAspect = mock(RecordTemplate.class);

    // Create an instance of PreUpdateResponse with the mock aspect
    PreUpdateResponse<RecordTemplate> response = new PreUpdateResponse<>(mockAspect);

    // Verify that the getter returns the correct value
    assertEquals(mockAspect, response.getUpdatedAspect());
  }
}
