package com.github.ccaspanello.spark.etl.step.addsequence;

import com.github.ccaspanello.spark.etl.step.BaseStepMeta;

/**
 * AddSequence Step Meta-data
 *
 * Created by ccaspanello on 1/29/18.
 */
public class AddSequenceMeta extends BaseStepMeta {

  private String columnName;

  public AddSequenceMeta( String name ) {
    super( name );
  }

  //<editor-fold desc="Getters & Setters">
  public String getColumnName() {
    return columnName;
  }

  public void setColumnName( String columnName ) {
    this.columnName = columnName;
  }
  //</editor-fold>
}
