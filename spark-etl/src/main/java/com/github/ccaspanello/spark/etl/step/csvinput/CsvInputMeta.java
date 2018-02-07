package com.github.ccaspanello.spark.etl.step.csvinput;

import com.github.ccaspanello.spark.etl.step.BaseStepMeta;

/**
 * CSV Input Step Meta-data
 *
 * Created by ccaspanello on 1/29/18.
 */
public class CsvInputMeta extends BaseStepMeta {

  private boolean header;
  private String filename;

  public CsvInputMeta(String name) {
    super(name);
  }

  //<editor-fold desc="Getters & Setters">
  public String getFilename() {
    return filename;
  }

  public void setFilename(String filename) {
    this.filename = filename;
  }

  public boolean isHeader() {
    return header;
  }

  public void setHeader(boolean header) {
    this.header = header;
  }
  //</editor-fold>
}