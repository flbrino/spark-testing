package com.github.ccaspanello.spark.etl.step.csvoutput;

import com.github.ccaspanello.spark.etl.step.BaseStepMeta;

/**
 * CSV Output Step Meta-data
 *
 * Created by ccaspanello on 1/29/18.
 */
public class CsvOutputMeta extends BaseStepMeta {

  private boolean header;
  private String filename;

  public CsvOutputMeta(String name) {
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
