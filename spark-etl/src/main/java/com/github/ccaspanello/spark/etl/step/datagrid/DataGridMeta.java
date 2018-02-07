package com.github.ccaspanello.spark.etl.step.datagrid;

import com.github.ccaspanello.spark.etl.step.BaseStepMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * DataGrid Step Meta-data
 *
 * Created by ccaspanello on 1/29/18.
 */
public class DataGridMeta extends BaseStepMeta {

  private List<Column> columns;
  private List<List<String>> data;

  public DataGridMeta(String name) {
    super(name);
    this.columns = new ArrayList<>();
    this.data = new ArrayList<>();
  }

  //<editor-fold desc="Getters & Setters">
  public List<Column> getColumns() {
    return columns;
  }

  public void setColumns(List<Column> columns) {
    this.columns = columns;
  }

  public List<List<String>> getData() {
    return data;
  }

  public void setData(List<List<String>> data) {
    this.data = data;
  }
  //</editor-fold>
}
