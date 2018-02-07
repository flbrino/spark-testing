package com.github.ccaspanello.spark.etl.api;

/**
 * HopMeta
 * <p>
 * Created by ccaspanello on 1/29/18.
 */
public class HopMeta {

  /**
   * Incoming Step Reference
   */
  private final String incoming;

  /**
   * Outgoing Step Reference
   */
  private final String outgoing;

  public HopMeta( StepMeta incoming, StepMeta outgoing ) {
    this.incoming = incoming.getName();
    this.outgoing = outgoing.getName();
  }

  public HopMeta( String incoming, String outgoing ) {
    this.incoming = incoming;
    this.outgoing = outgoing;
  }

  //<editor-fold desc="Getters & Setters">
  public String getIncoming() {
    return incoming;
  }

  public String getOutgoing() {
    return outgoing;
  }
  //</editor-fold>
}
