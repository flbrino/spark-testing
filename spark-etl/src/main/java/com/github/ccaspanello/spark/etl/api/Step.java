package com.github.ccaspanello.spark.etl.api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Set;

/**
 * Step Interface
 * <p>
 * Interface for classes that hold step logic.
 * <p>
 * Created by ccaspanello on 1/29/2018.
 */
public interface Step extends Serializable {

  /**
   * Executes Step Logic
   */
  void execute();

  //<editor-fold desc="Getters & Setters">
  void setSparkSession( SparkSession spark );

  SparkSession getSparkSession();

  StepMeta getStepMeta();

  void setIncoming( Set<Step> incoming );

  void setOutgoing( Set<Step> outgoing );

  Dataset<Row> getData();
  //</editor-fold>
}
