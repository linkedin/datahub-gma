package com.linkedin.metadata.dao.ingestion.preupdate;

import com.google.protobuf.Message;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;


/**
 * A restli client to route update request to the appropriate to custom APIs.
 * <p>This interface extends {@link PreUpdateRoutingClient} and provides additional methods for converting
 *  * URNs and aspects between different representations (e.g., from Pegasus to Protobuf).</p>
 *  *
 */

public interface RestliCompliantPreUpdateRoutingClient<ASPECT extends Message> extends PreUpdateRoutingClient {

  /**
   * Converts a URN to a Protobuf message.
   *
   * @param pegasusUrn the URN to be converted
   * @return the converted Protobuf message
   */
  Message convertUrnToMessage(Urn pegasusUrn);

  /**
   * Converts a {@link RecordTemplate} aspect to a Protobuf message aspect.
   *
   * @param pegasusAspect the aspect to be converted
   * @return the converted Protobuf message aspect
   */
  ASPECT convertAspectToMessage(RecordTemplate pegasusAspect);

  /**
   * Converts a Protobuf message aspect to a {@link RecordTemplate} aspect.
   *
   * @param messageAspect the Protobuf message aspect to be converted
   * @return the converted {@link RecordTemplate} aspect
   */
  RecordTemplate convertAspectToRecordTemplate(ASPECT messageAspect);
}