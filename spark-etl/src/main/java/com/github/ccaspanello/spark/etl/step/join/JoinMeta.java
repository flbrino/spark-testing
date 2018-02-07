package com.github.ccaspanello.spark.etl.step.join;

import com.github.ccaspanello.spark.etl.step.BaseStepMeta;

/**
 * Join Step Meta-data
 *
 * Created by ccaspanello on 1/29/18.
 */
public class JoinMeta extends BaseStepMeta {

  public String joinColumn;

  public JoinMeta( String name ) {
    super( name );
  }

  //<editor-fold desc="Getters & Setters">
  public String getJoinColumn() {
    return joinColumn;
  }

  public void setJoinColumn(String joinColumn) {
    this.joinColumn = joinColumn;
  }
  //</editor-fold>
}
