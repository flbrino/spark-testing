package com.github.ccaspanello.spark.etl.step.datagrid;

import org.apache.spark.sql.types.DataType;

import java.io.Serializable;

/**
 * Column Definition
 *
 * Created by ccaspanello on 1/29/18.
 */
public class Column implements Serializable {

  private String name;
  private DataType type;
  private boolean nullable;

  public Column(String name, DataType type){
    this.name = name;
    this.type = type;
  }

  //<editor-fold desc="Getters & Setters">
  public String getName() {
    return name;
  }

  public DataType getType() {
    return type;
  }

  public boolean isNullable() {
    return nullable;
  }

  public void setNullable(boolean nullable) {
    this.nullable = nullable;
  }
  //</editor-fold>
}
