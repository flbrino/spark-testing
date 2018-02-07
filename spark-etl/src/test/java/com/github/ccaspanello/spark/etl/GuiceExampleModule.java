package com.github.ccaspanello.spark.etl;

import com.google.inject.Binder;
import com.google.inject.Module;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/**
 * Test Guice Module
 * <p>
 * Injects dependencies into class using the @Inject annotation
 * <p>
 * Created by ccaspanello on 12/29/18.
 */
public class GuiceExampleModule implements Module {

  @Override
  public void configure( Binder binder ) {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set( "spark.eventLog.enabled", "true" );
    sparkConf.set( "spark.driver.host", "localhost" );
    sparkConf.set( "spark.driver.allowMultipleContexts", "true" );
    sparkConf.set( "spark.logLineage", "true" );
    SparkContext sparkContext = new SparkContext( "local[*]", "SparkTest", sparkConf );

    StepRegistry stepRegistry = new StepRegistry();
    stepRegistry.init();

    AppContext appContext = new AppContext(sparkContext, stepRegistry);

    binder.bind( AppContext.class ).toInstance( appContext );
  }

}

