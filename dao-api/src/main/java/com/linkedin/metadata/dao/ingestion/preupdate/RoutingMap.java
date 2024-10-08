package com.linkedin.metadata.dao.ingestion.preupdate;

import com.google.protobuf.Message;
import lombok.Data;


@Data
public class RoutingMap {

  public enum RoutingAction {
    SKIP,
    PROCEED
  }
  public PreUpdateService<? extends Message> preUpdateService;
}
