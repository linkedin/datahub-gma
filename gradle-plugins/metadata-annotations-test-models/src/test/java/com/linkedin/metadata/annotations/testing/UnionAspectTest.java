package com.linkedin.metadata.annotations.testing;

import com.linkedin.metadata.events.ChangeType;
import com.linkedin.testing.AnnotatedAspectBar;
import com.linkedin.testing.AnotherAspectBar;
import com.linkedin.testing.mxe.bar.FailedMCE_BarAspect;
import com.linkedin.testing.mxe.bar.MCE_BarAspect;
import com.linkedin.testing.mxe.bar.ProposedAnnotatedAspectBar;
import com.linkedin.testing.mxe.bar.ProposedAnotherAspectBar;
import com.linkedin.testing.urn.BarUrn;
import org.junit.jupiter.api.Test;

public class UnionAspectTest {

  // This test is meant to show that we can successfully build UnionAspect events. It serves as a testing ground
  // to ensure event building is reasonable.
  @Test
  public void test() throws Exception {
    MCE_BarAspect unionAspect = new MCE_BarAspect().setUrn(new BarUrn(123));

    AnnotatedAspectBar bar = new AnnotatedAspectBar().setBoolField(true);
    ProposedAnnotatedAspectBar proposedBar = new ProposedAnnotatedAspectBar().setProposed(bar);

    AnotherAspectBar anotherBar = new AnotherAspectBar().setStringField("foo");
    ProposedAnotherAspectBar proposedAnotherBar =
            new ProposedAnotherAspectBar().setProposed(anotherBar).setChangeType(ChangeType.DELETE);

    unionAspect.setProposedValues(new MCE_BarAspect.ProposedValuesArray(
            MCE_BarAspect.ProposedValues.create(proposedBar),
            MCE_BarAspect.ProposedValues.create(proposedAnotherBar)));

    FailedMCE_BarAspect failedAspect = new FailedMCE_BarAspect();
    failedAspect.setMetadataChangeEvent(unionAspect);
    failedAspect.setError("test");
  }

}
