package com.github.ccaspanello.spark.etl;

import org.apache.spark.SparkContext;

/**
 * Created by ccaspanello on 2/4/18.
 */
public class AppContext {

  private final SparkContext sparkContext;
  private final StepRegistry stepRegistry;

  public AppContext( SparkContext sparkContext, StepRegistry stepRegistry ) {
    this.sparkContext = sparkContext;
    this.stepRegistry = stepRegistry;
  }

  public SparkContext getSparkContext() {
    return sparkContext;
  }

  public StepRegistry getStepRegistry() {
    return stepRegistry;
  }
}
