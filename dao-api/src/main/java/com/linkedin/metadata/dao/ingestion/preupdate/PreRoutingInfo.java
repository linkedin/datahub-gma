package com.linkedin.metadata.dao.ingestion.preupdate;

import com.google.protobuf.Message;
import lombok.Data;


@Data
public class PreRoutingInfo {

  public PreUpdateClient<? extends Message> preUpdateClient;

  public enum RoutingAction {
    PROCEED, SKIP
  }
}
