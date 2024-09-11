package com.linkedin.metadata.dao.ingestion;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.testing.AspectFoo;


public class SamplePreUpdateRoutingClient implements RestliCompliantPreUpdateRoutingClient {
  @Override
  public Message routingLambda(Message urn, Message aspect) {
    // For testing, change the aspect value to "bar"
    return Any.pack(StringValue.of("bar"));
  }

  @Override
  public Message convertUrnToMessage(Urn urn) {
    // Directly wrap the URN string into a Protobuf message for testing
    return Any.pack(StringValue.of(urn.toString()));
  }

  @Override
  public Message convertAspectToMessage(RecordTemplate pegasusAspect) {
    // For testing, convert AspectFoo to a TestMessageProtos.AspectMessage
    // Assuming the aspect has a `value` field and its string representation can be used for now
    String aspectString = pegasusAspect.toString();  // Extracting the aspect as a string (e.g., {value=foo})

    // Wrap the aspect string into a simple Protobuf message for testing
    return Any.pack(StringValue.of(aspectString));
  }

  @Override
  public RecordTemplate convertAspectToRecordTemplate(Message messageAspect) {
    // For testing, convert TestMessageProtos.AspectMessage back to AspectFoo
    // Create a new RecordTemplate (AspectFoo in this case) and set the value field
    return new AspectFoo().setValue("bar");
  }
}
