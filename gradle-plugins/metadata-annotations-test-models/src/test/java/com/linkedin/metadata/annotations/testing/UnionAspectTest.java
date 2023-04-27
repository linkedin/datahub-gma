package com.linkedin.metadata.annotations.testing;

import com.linkedin.metadata.events.ChangeType;
import com.linkedin.testing.AnnotatedAspectBar;
import com.linkedin.testing.AnotherAspectBar;
import com.linkedin.testing.mxe.bar.FailedMCEBarAspect;
import com.linkedin.testing.mxe.bar.MCEBarAspect;
import com.linkedin.testing.mxe.bar.ProposedAnnotatedAspectBar;
import com.linkedin.testing.mxe.bar.ProposedAnotherAspectBar;
import com.linkedin.testing.urn.BarUrn;
import org.junit.jupiter.api.Test;

public class UnionAspectTest {

  // This test is meant to show that we can successfully build UnionAspect events. It serves as a testing ground
  // to ensure event building is reasonable.
  @Test
  public void test() throws Exception {
    MCEBarAspect unionAspect = new MCEBarAspect().setUrn(new BarUrn(123));

    AnnotatedAspectBar bar = new AnnotatedAspectBar().setBoolField(true);
    ProposedAnnotatedAspectBar proposedBar = new ProposedAnnotatedAspectBar().setProposed(bar);

    AnotherAspectBar anotherBar = new AnotherAspectBar().setStringField("foo");
    ProposedAnotherAspectBar proposedAnotherBar =
            new ProposedAnotherAspectBar().setProposed(anotherBar).setChangeType(ChangeType.DELETE);

    unionAspect.setProposedValues(new MCEBarAspect.ProposedValuesArray(
            MCEBarAspect.ProposedValues.create(proposedBar),
            MCEBarAspect.ProposedValues.create(proposedAnotherBar)));

    FailedMCEBarAspect failedAspect = new FailedMCEBarAspect();
    failedAspect.setMetadataChangeEvent(unionAspect);
    failedAspect.setError("test");
  }

}
