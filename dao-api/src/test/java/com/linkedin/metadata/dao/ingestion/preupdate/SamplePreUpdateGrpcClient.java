package com.linkedin.metadata.dao.ingestion.preupdate;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.testing.AspectFoo;


public class SamplePreUpdateGrpcClient implements PreUpdateClient {
  @Override
  public PreUpdateResponse preUpdate(Urn urn, RecordTemplate recordTemplate) {
    // Convert URN to Protobuf message
    Message urnMessage = convertUrnToMessage(urn);

    // Convert RecordTemplate to Protobuf message
    Message aspectMessage = convertAspectToMessage(recordTemplate);

    // For testing, change the aspect value to "bar"
    Message updatedAspectMessage = Any.pack(StringValue.of("bar"));

    // Convert the updated aspect message back to RecordTemplate
    RecordTemplate updatedAspect = convertAspectToRecordTemplate(updatedAspectMessage);
    // Return a new PreUpdateResponse with the updated aspect
    return new PreUpdateResponse<>(updatedAspect);
  }

  @Override
  public Message convertUrnToMessage(Urn urn) {
    // Directly wrap the URN string into a Protobuf message for testing
    return Any.pack(StringValue.of(urn.toString()));
  }

  @Override
  public Message convertAspectToMessage(RecordTemplate aspect) {
    String aspectString = aspect.toString();  // Extracting the aspect as a string
    return Any.pack(StringValue.of(aspectString));
  }

  @Override
  public RecordTemplate convertAspectToRecordTemplate(Message messageAspect) {
    // For testing, convert TestMessageProtos.AspectMessage back to AspectFoo
    // Create a new RecordTemplate (AspectFoo in this case) and set the value field
    return new AspectFoo().setValue("bar");
  }
}
